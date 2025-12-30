import sys
import os
from datetime import datetime
import pandas as pd
from FinMind.data import DataLoader 
from common import utils,db
import requests
from typing import Union, Iterable

sys.stdout.reconfigure(encoding='utf-8')

### FinMind api設定
apiUrl = "https://api.finmindtrade.com/api/v4/data"
api = DataLoader()
token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNS0xMi0yMiAyMDoxOTo1OSIsInVzZXJfaWQiOiJueWN1bGFiNjE1IiwiaXAiOiIyMjMuMTQzLjE5NC45In0.cpV6AuW_6FhXhZXvvnyKhFojvH7gBML9ipthkfNwIUo"
api.login_by_token(api_token=token)

storageDir = "../data/FinMind"
os.makedirs(storageDir, exist_ok=True)

storageDir_twStockInfo =  f"{storageDir}/TW/StockInfo"
os.makedirs(storageDir_twStockInfo, exist_ok=True)

def getDataLoader() -> DataLoader:
    return api

# 撈取台股清單
def twStockInfo(includeCateHistory:bool=False) -> pd.DataFrame:
    df = None
    output_file = f"{storageDir_twStockInfo}/stock_info.csv"
    if os.path.exists(output_file):
        df = pd.read_csv(output_file)
        print(f"☑️ Data exist: {output_file}")
    else:        
        df = api.taiwan_stock_info() # 台股總覽
        df['date'] = pd.to_datetime(df['date'], errors='coerce') 
        df = df[df['date'].notna()] 
        if not includeCateHistory:
            # 確保 date 欄位是 datetime 格式
            df['date'] = pd.to_datetime(df['date'])

            # 依 stock_id 分組，選取每組中 date 最大的那筆資料
            latest_df = df.sort_values('date').groupby('stock_id', as_index=False).tail(1)

            # 依照 stock_id 排序（可選）
            df = latest_df.sort_values(by='stock_id')
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
    return df

# 撈取上市清單
def twStockInfoTwse(includeCateHistory:bool=False) -> pd.DataFrame:
    df_twse_filtered = None
    output_file = f"{storageDir_twStockInfo}/stock_info-twse.csv"
    if os.path.exists(output_file):
        df = pd.read_csv(output_file)
        print(f"☑️ Data exist: {output_file}")
        return df
    else:        
        df = twStockInfo(includeCateHistory)
        
        # 篩選 type 為 'twse' (上市)
        df_twse = df[df['type'] == 'twse']

        # 排除 industry_category 欄位含有指定關鍵字的資料
        exclude_keywords = ['ETF', 'Index', '受益證券', 'ETN', '大盤', '存託憑證', '創新板股票', '創新版股票']
        pattern = '|'.join(exclude_keywords)  # 建立 regex 模式
        df_twse_filtered = df_twse[~df_twse['industry_category'].str.contains(pattern, na=False)]

        df_twse_filtered.to_csv(output_file, index=False, encoding='utf-8-sig')
    return df_twse_filtered

# 排除興櫃的台股清單
def twStockInfoNoEmerging(includeCateHistory:bool=False) -> pd.DataFrame:
    df_twse_filtered = None
    output_file = f"{storageDir_twStockInfo}/stock_info-no_emerging.csv"
    if os.path.exists(output_file):
        df_twse_filtered = pd.read_csv(output_file)
        print(f"☑️ Data exist: {output_file}")
    else:          
        df = twStockInfo(includeCateHistory)
        
        # 排除 type 為 'emerging' (興櫃)
        df_twse = df[df['type'] != 'emerging']

        # 排除 industry_category 欄位含有指定關鍵字的資料
        exclude_keywords = ['ETF', 'Index', '受益證券', 'ETN', '大盤', '存託憑證', '創新板股票', '創新版股票']
        pattern = '|'.join(exclude_keywords)  # 建立 regex 模式
        df_twse_filtered = df_twse[~df_twse['industry_category'].str.contains(pattern, na=False)]

        df_twse_filtered.to_csv(output_file, index=False, encoding='utf-8-sig')
    return df_twse_filtered

storageDir_twMarketValue =  f"{storageDir}/TW/MarketValue"
os.makedirs(storageDir_twMarketValue, exist_ok=True)

# 撈取各股票市值資料（逐年存檔）
def runTwMarketValue(stockList: list, sDt: datetime, eDt: datetime) -> bool:
    result = True
    try:
        utils.ptMsg("📢 即將撈取[市值歷史]資料（逐年存檔），股票清單長度：", len(stockList))

        outputDir = storageDir_twMarketValue

        for stock_id in stockList:
            cur_year = sDt.year
            end_year = eDt.year

            while cur_year <= end_year:
                year_start = datetime(cur_year, 1, 1)
                year_end = datetime(cur_year, 12, 31)
                if year_end > eDt:
                    year_end = eDt
                if year_start < sDt:
                    year_start = sDt

                outputFile = f"{outputDir}/{cur_year}/TWMV-{stock_id}.csv"

                if os.path.exists(outputFile):
                    utils.ptMsg("☑️ 檔案已存在：", outputFile)
                else:
                    os.makedirs(os.path.dirname(outputFile), exist_ok=True)
                    try:
                        utils.ptMsg(f"➡️ 撈取 {stock_id} 年度：{cur_year}（{year_start.date()} ~ {year_end.date()}）")
                        dfMV = api.taiwan_stock_market_value(
                            stock_id=stock_id,
                            start_date=year_start.strftime("%Y-%m-%d"),
                            end_date=year_end.strftime("%Y-%m-%d")
                        )
                        dfMV.to_csv(outputFile, index=False, encoding='utf-8-sig')
                        utils.ptMsg("✅ 檔案存取成功：", outputFile)
                    except Exception as e:
                        utils.ptMsg(f"❌ {stock_id} 年度 {cur_year} 抓取失敗，錯誤訊息：{e}")
                        # 不 raise，繼續跑其他年度

                cur_year += 1

        utils.ptMsg("📢 [市值歷史]資料撈取結束。")

    except Exception as e:
        utils.ptMsg(f"發生錯誤：{e}")
        return False

    return result

storageDir_twDailyPriceAdj =  f"{storageDir}/TW/DailyPriceAdj"
os.makedirs(storageDir_twStockInfo, exist_ok=True)

# 撈取股票每日調整後價格（逐年存檔）
def runTwStockDailyPriceAdj(stockList: list, sDt: datetime, eDt: datetime, outputDir: str=None) -> bool:
    result = True
    try:
        utils.ptMsg("📢 即將撈取[歷史修正股價]資料（逐年存檔），股票清單長度：", len(stockList))
        if outputDir is None:
            outputDir = storageDir_twDailyPriceAdj

        for stock_id in stockList:
            cur_year = sDt.year
            end_year = eDt.year

            while cur_year <= end_year:
                year_start = datetime(cur_year, 1, 1)
                year_end = datetime(cur_year, 12, 31)
                # 確保不超過指定的 eDt
                if year_end > eDt:
                    year_end = eDt
                if year_start < sDt:
                    year_start = sDt

                outputFile = f'{outputDir}/{cur_year}/TWDPadj-{stock_id}.csv'

                if os.path.exists(outputFile):
                    utils.ptMsg("☑️ 檔案已存在：", outputFile)
                else:
                    os.makedirs(os.path.dirname(outputFile), exist_ok=True)
                    try:
                        utils.ptMsg(f"➡️ 撈取 {stock_id} 年度：{cur_year}（{year_start.date()} ~ {year_end.date()}）")
                        dfSDA = api.taiwan_stock_daily_adj(
                            stock_id=stock_id,
                            start_date=year_start.strftime("%Y-%m-%d"),
                            end_date=year_end.strftime("%Y-%m-%d")
                        )
                        dfSDA.to_csv(outputFile, index=False, encoding='utf-8-sig')
                        utils.ptMsg("✅ 檔案存取成功：", outputFile)
                    except Exception as e:
                        utils.ptMsg(f"❌ {stock_id} 年度 {cur_year} 抓取失敗，錯誤訊息：{e}")
                        # 不要 raise，繼續抓下一年
                cur_year += 1

    except Exception as e:
        utils.ptMsg(f"發生重大錯誤：{e}")
        return False

    return result

# 取得加權指數的調整後價格          
def getWeightIdxDailyPriceAdj(sDt: datetime, eDt: datetime) -> bool:    
    # outputDir = f'{storageDir_twDailyPriceAdj}/weightIdx'
    stockList = ['TAIEX']
    return runTwStockDailyPriceAdj(stockList, sDt, eDt)

# 取得台股的所有交易日期
def getTwStockTradingDates() -> pd.DataFrame:
    fileDir = "../data/FinMind/TW/StockInfo/twStockTradingDate.csv"
    url = "https://api.finmindtrade.com/api/v4/data"
    parameter = {"dataset": "TaiwanStockTradingDate"}

    # ===== 檔案存在，先讀舊資料 =====
    if os.path.exists(fileDir):
        df_local = pd.read_csv(fileDir)
        try:
            last_local_date = pd.to_datetime(df_local["date"].max()).date()
        except Exception:
            last_local_date = None
    else:
        df_local = None
        last_local_date = None

    # ===== 檢查是否需要更新 =====
    resp = requests.get(url, params=parameter)
    data = pd.DataFrame(resp.json()["data"])
    last_online_date = pd.to_datetime(data["date"].max()).date()

    need_update = (
        last_local_date is None or
        last_local_date < last_online_date
    )

    if need_update:
        # 更新資料並覆蓋
        os.makedirs(os.path.dirname(fileDir), exist_ok=True)
        data.to_csv(fileDir, index=False, encoding="utf-8-sig")
        print(f"✅ 已更新交易日曆至 {last_online_date}")
        return data
    else:
        print(f"📁 使用本地快取：已是最新至 {last_local_date}")
        return df_local

# 取得台股日資料
def get_tw_stock_daily_price(
    stock_id: Union[str, list[str]],
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    print(f"--- run finMind.get_tw_stock_daily_price--[{stock_id}]")
    target_table = "fm_taiwan_stock_daily"

    # === 0) stock_id 正規化 ===
    if isinstance(stock_id, str):
        stock_ids = [stock_id]
    else:
        stock_ids = list(stock_id)

    req_s = pd.Timestamp(start_date).normalize()
    req_e = pd.Timestamp(end_date).normalize()
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")

    def dstr(t: pd.Timestamp) -> str:
        return t.strftime("%Y-%m-%d")

    all_dfs: list[pd.DataFrame] = []

    # === 1) 逐檔處理 ===
    for sid in stock_ids:

        # --- 查 span ---
        span_row = db.query_to_df(
            """
            SELECT start_date, end_date
            FROM stock_span
            WHERE target_table = ? AND stock_id = ?
            """,
            (target_table, sid),
        )

        mem_s = pd.Timestamp(span_row.loc[0, "start_date"]).normalize() if not span_row.empty else None
        mem_e = pd.Timestamp(span_row.loc[0, "end_date"]).normalize() if not span_row.empty else None

        fetch_ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []

        if mem_s is None:
            fetch_ranges = [(req_s, req_e)]
            new_s, new_e = req_s, req_e
        else:
            new_s = min(mem_s, req_s)
            new_e = max(mem_e, req_e)

            if req_s >= mem_s and req_e <= mem_e:
                fetch_ranges = []
            elif req_s > mem_e:
                fetch_ranges = [(mem_e + pd.Timedelta(days=1), req_e)]
            elif req_e < mem_s:
                fetch_ranges = [(req_s, mem_s - pd.Timedelta(days=1))]
            else:
                if req_s < mem_s:
                    fetch_ranges.append((req_s, mem_s - pd.Timedelta(days=1)))
                if req_e > mem_e:
                    fetch_ranges.append((mem_e + pd.Timedelta(days=1), req_e))

        # --- 補資料 ---
        upsert_sql = f"""
        INSERT INTO {target_table}
        (date, stock_id, Trading_Volume, Trading_money, open, max, min, close, spread, Trading_turnover)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(date, stock_id) DO UPDATE SET
          Trading_Volume   = excluded.Trading_Volume,
          Trading_money    = excluded.Trading_money,
          open             = excluded.open,
          max              = excluded.max,
          min              = excluded.min,
          close            = excluded.close,
          spread           = excluded.spread,
          Trading_turnover = excluded.Trading_turnover
        """

        for fs, fe in fetch_ranges:
            if fs <= fe:
                df_api = api.taiwan_stock_daily(
                    stock_id=sid,
                    start_date=dstr(fs),
                    end_date=dstr(fe),
                )
                if df_api is not None and not df_api.empty:
                    df_api = df_api.copy()
                    df_api["date"] = pd.to_datetime(df_api["date"]).dt.strftime("%Y-%m-%d")
                    df_api["stock_id"] = df_api["stock_id"].astype(str)

                    params = list(df_api[
                        ["date", "stock_id", "Trading_Volume", "Trading_money",
                         "open", "max", "min", "close", "spread", "Trading_turnover"]
                    ].itertuples(index=False, name=None))

                    db.execute_sql(upsert_sql, params)

        # --- 更新 span ---
        db.execute_sql(
            """
            INSERT INTO stock_span (target_table, stock_id, start_date, end_date, updated_at)
            VALUES (?, ?, ?, ?, strftime('%s','now'))
            ON CONFLICT(target_table, stock_id) DO UPDATE SET
              start_date = excluded.start_date,
              end_date   = excluded.end_date,
              updated_at = strftime('%s','now')
            """,
            (target_table, sid, dstr(new_s), dstr(new_e)),
        )

        # --- DB 回傳該檔 ---
        df_sid = db.query_to_df(
            f"""
            SELECT date, stock_id, Trading_Volume, Trading_money,
                   open, max, min, close, spread, Trading_turnover
            FROM {target_table}
            WHERE stock_id = ?
              AND date >= ?
              AND date <= ?
            ORDER BY date
            """,
            (sid, dstr(req_s), dstr(req_e)),
        )

        all_dfs.append(df_sid)

    # === 2) 合併回傳 ===
    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)

# python -m module.finMind
if __name__ == "__main__":
    stock_id = ['0050']
    sDt = datetime(2025, 12, 1)
    eDt = datetime.now()
    df = get_tw_stock_daily_price(stock_id, sDt, eDt)
    
    print(df.head(), df.tail())
    
