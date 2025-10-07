import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import requests
import calendar
from db import query_to_df, execute_sql, get_connection, query_single
import json

twseUrl = "https://www.twse.com.tw/rwd/zh"
data_center = "../data/TwStockExchange"
common_params = "response=json"

# ======== 共用工具 ========
def _date_to_str(date: datetime = None, formate: str = None) -> str:
    """將 datetime 轉為 yyyymmdd 字串，若未指定則取今日"""
    if date is None:
        date = datetime.today()
    if formate is None:
        formate = "%Y%m%d"
    return date.strftime(formate)

def _save_to_csv(df: pd.DataFrame, apiEndpoint: str, filename: str):
    # 1. 檢查 data_center 是否存在
    if not os.path.exists(data_center):
        raise FileNotFoundError(f"❌ data_center 不存在：{data_center}")
    
    # 2. 確保目標資料夾存在
    dir_path = os.path.join(data_center, apiEndpoint)
    os.makedirs(dir_path, exist_ok=True)  # 如果不存在就建立
    
    path = os.path.join(dir_path, f"{filename}.csv")
    
    # 3. 儲存 CSV
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"✅ 已儲存：{path}")

def _read_from_csv(apiEndpoint: str, filename: str) -> pd.DataFrame:
    # 1. 檢查 data 是否存在
    dir_path = os.path.join(data_center, apiEndpoint)    
    path = os.path.join(dir_path, f"{filename}.csv")

    if not os.path.exists(path):
        print(f"❌ CSV檔案 不存在：{path}")
        return None
    
    df = pd.read_csv(path)
    return df

def _cleanup_old_files(dir_path: str, stock_no: str, date_str: str, keep: str):
    """刪除同月份中除了 keep 的其他檔案"""
    for f in os.listdir(dir_path):
        if f.startswith(f"{stock_no}_{date_str}") and f.endswith(".csv") and f != keep:
            try:
                os.remove(os.path.join(dir_path, f))
                print(f"🧹 已刪除舊檔：{f}")
            except Exception as e:
                print(f"⚠️ 無法刪除 {f}: {e}")

def _to_roc_date(dt: datetime) -> str:
    """將 datetime 轉成民國年月日格式（yyy.mm.dd）"""
    roc_year = dt.year - 1911
    return f"{roc_year:03d}.{dt.month:02d}.{dt.day:02d}"

def _roc_to_datetime(roc_date: str) -> datetime:
    """
    將民國年日期字串轉成 datetime
    例如 "114.04.11" → datetime(2025, 4, 11)
    """
    try:
        parts = roc_date.split(".")
        if len(parts) != 3:
            raise ValueError("格式錯誤，應為 yyy.mm.dd")
        year = int(parts[0]) + 1911  # 民國年轉西元年
        month = int(parts[1])
        day = int(parts[2])
        return datetime(year, month, day)
    except Exception as e:
        print(f"[Error] 轉換失敗: {roc_date} ({e})")
        return None
    
def _is_fully_in_range(sDt: datetime, eDt: datetime, minDt: datetime, maxDt: datetime) -> bool:
    """
    判斷區間 sDt~eDt 是否完全包含在 minDt~maxDt 內
    回傳布林值
    """
    return minDt <= sDt <= maxDt and minDt <= eDt <= maxDt

def _is_no_overlap(sDt: datetime, eDt: datetime, minDt: datetime, maxDt: datetime) -> bool:
    """
    判斷區間 sDt~eDt 是否與 minDt~maxDt 完全不重疊
    回傳布林值
    """
    return eDt < minDt or sDt > maxDt


# ======== 1. 個股成交日資訊 (含快取、合併、清理機制) ========
# 日期,成交股數,成交金額,開盤價,最高價,最低價,收盤價,漲跌價差,成交筆數
def get_stock_day(stock_no: str, date: datetime | None = None) -> pd.DataFrame:
    today = datetime.today()
    date = (date or today).replace(day=1)
    date_str = date.strftime("%Y%m")
    apiEndpoint = "afterTrading/STOCK_DAY"
    dir_path = os.path.join(data_center, apiEndpoint)
    os.makedirs(dir_path, exist_ok=True)

    # 嘗試完整檔
    df = _read_from_csv(apiEndpoint, f"{stock_no}_{date_str}")
    if df is not None:
        print(f"📂 檔案已存在：{stock_no}_{date_str}.csv")
        return df

    # 呼叫 API
    apiUrl = f"{twseUrl}/{apiEndpoint}?date={date.strftime('%Y%m%d')}&stockNo={stock_no}&{common_params}"
    res = requests.get(apiUrl).json()
    df = pd.DataFrame(res.get("data", []), columns=res.get("fields", []))

    if df.empty:
        print("⚠️ API 回傳空資料")
        return df

    # 處理民國年日期（例如 '114/10/03' → '20241003'）
    raw = df.iloc[-1, 0].replace("/", "")
    if len(raw) == 7:  # 民國年格式
        raw = str(int(raw[:3]) + 1911) + raw[3:]
    last_date = datetime.strptime(raw, "%Y%m%d")

    # 判斷是否為當月（尚未結束的月）
    is_current_month = date.year == today.year and date.month == today.month
    if not is_current_month:
        # 過去月份一定存成完整月檔
        filename = f"{stock_no}_{date_str}"
    else:
        # 本月尚未結束，可能不完整
        days_in_month = calendar.monthrange(date.year, date.month)[1]
        if last_date.day == days_in_month:
            filename = f"{stock_no}_{date_str}"
        else:
            filename = f"{stock_no}_{date.strftime('%Y%m%d')}_{last_date.strftime('%Y%m%d')}"

    _save_to_csv(df, apiEndpoint, filename)
    _cleanup_old_files(dir_path, stock_no, date_str, keep=f"{filename}.csv")

    return df

# ======== 2. 個股收盤價 ========
# 日期,收盤價
def get_stock_day_avg(stock_no: str, date: datetime | None = None) -> pd.DataFrame:
    today = datetime.today()
    date = (date or today).replace(day=1)
    date_str = date.strftime("%Y%m")
    apiEndpoint = "afterTrading/STOCK_DAY_AVG"
    dir_path = os.path.join(data_center, apiEndpoint)
    os.makedirs(dir_path, exist_ok=True)

    # 嘗試完整檔
    df = _read_from_csv(apiEndpoint, f"{stock_no}_{date_str}")
    if df is not None:
        print(f"📂 檔案已存在：{stock_no}_{date_str}.csv")
        return df

    # 呼叫 API
    apiUrl = f"{twseUrl}/{apiEndpoint}?date={date.strftime('%Y%m%d')}&stockNo={stock_no}&{common_params}"
    res = requests.get(apiUrl).json()
    df = pd.DataFrame(res.get("data", []), columns=res.get("fields", []))

    if df.empty:
        print("⚠️ API 回傳空資料")
        return df

    # 找最後一個合法日期（排除非日期列，例如月平均收盤價）
    date_col = df.columns[0]  # 假設第一欄是日期
    for d in reversed(df[date_col]):
        raw = str(d).replace("/", "")
        if raw.isdigit() and (len(raw) == 7 or len(raw) == 8):
            # 處理民國年格式
            if len(raw) == 7:
                raw = str(int(raw[:3]) + 1911) + raw[3:]
            last_date = datetime.strptime(raw, "%Y%m%d")
            break
    else:
        raise ValueError("找不到有效日期欄位")

    # 判斷是否為當月（尚未結束的月）
    is_current_month = date.year == today.year and date.month == today.month
    if not is_current_month:
        # 過去月份直接存完整月檔
        filename = f"{stock_no}_{date_str}"
    else:
        # 本月尚未結束
        days_in_month = calendar.monthrange(date.year, date.month)[1]
        if last_date.day == days_in_month:
            filename = f"{stock_no}_{date_str}"
        else:
            filename = f"{stock_no}_{date.strftime('%Y%m%d')}_{last_date.strftime('%Y%m%d')}"

    # 儲存 CSV 並清理舊檔
    _save_to_csv(df, apiEndpoint, filename)
    _cleanup_old_files(dir_path, stock_no, date_str, keep=f"{filename}.csv")

    return df

# ======== 3. 三大法人 ========
# 單位名稱,買進金額,賣出金額,買賣差額
def get_institutional_investors(date: datetime | None = None) -> pd.DataFrame:
    date_str = _date_to_str(date)
    apiEndpoint = "fund/BFI82U"
    apiParams = f"type=day&dayDate={date_str}&weekDate={date_str}&monthDate={date_str}&{common_params}"
    apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

    res = requests.get(apiUrl)
    data = res.json()

    df = pd.DataFrame(data.get("data", []), columns=data.get("fields", []))
    _save_to_csv(df, apiEndpoint, f"{date_str}")
    return df

# ======== 3.2 三大法人 區間版 ========
# 單位名稱,買進金額,賣出金額,買賣差額
def get_institutional_investors_range(startDate: datetime, endDate: datetime) -> pd.DataFrame:
    apiEndpoint = "fund/BFI82U"
    all_data = []
    current = startDate
    while current <= endDate:
        date_str = _date_to_str(current)
        apiParams = f"type=day&dayDate={date_str}&weekDate={date_str}&monthDate={date_str}&{common_params}"
        apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

        res = requests.get(apiUrl)
        data = res.json()

        # 如果 API 回傳不是 OK 或沒有資料，跳過
        if data.get("stat") != "OK" or "data" not in data:
            print(f"⚠️ {date_str} 沒有資料，已跳過")
            current += timedelta(days=1)
            continue

        df_day = pd.DataFrame(data["data"], columns=data["fields"])
        df_day.insert(0, "日期", date_str)  # 加上日期欄位
        all_data.append(df_day)

        current += timedelta(days=1)

    # 合併所有日期的資料
    if not all_data:
        raise ValueError("❌ 區間內沒有任何有效資料")

    df_all = pd.concat(all_data, ignore_index=True)

    # 存成一個總檔案
    filename = f"{_date_to_str(startDate)}_{_date_to_str(endDate)}"
    _save_to_csv(df_all, apiEndpoint, filename)

    return df_all

# ======== 4. 融資融券餘額 ========
# 日期,項目,買進,賣出,現金(券)償還,前日餘額,今日餘額
def get_margin_trading(date: datetime | None = None) -> pd.DataFrame:
    date_str = _date_to_str(date)
    apiEndpoint = "marginTrading/MI_MARGN"
    apiParams = f"date={date_str}&selectType=MS&{common_params}"
    apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

    res = requests.get(apiUrl)
    data = res.json()

    # tables[0] 才有資料
    tables = data.get("tables", [])
    if not tables or "fields" not in tables[0] or "data" not in tables[0]:
        raise ValueError(f"❌ API 回傳格式異常: {data}")

    fields = tables[0]["fields"]
    rows = tables[0]["data"]

    df = pd.DataFrame(rows, columns=fields)

    _save_to_csv(df, apiEndpoint, f"{date_str}")
    return df

# ======== 4.2 融資融券餘額 區間版 ========
# 日期,項目,買進,賣出,現金(券)償還,前日餘額,今日餘額
def get_margin_trading_range(sDt: datetime, eDt: datetime) -> pd.DataFrame:
    apiEndpoint = "marginTrading/MI_MARGN"
    all_data = []

    current = sDt
    while current <= eDt:
        date_str = _date_to_str(current)
        apiParams = f"date={date_str}&selectType=MS&{common_params}"
        apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

        res = requests.get(apiUrl)
        try:
            data = res.json()
        except ValueError:
            print("⚠️ API 回傳不是 JSON！")
            print(res.text)
            current += timedelta(days=1)
            continue

        # 如果回傳只有 "stat" 且不是 "OK"，表示沒資料
        if data.get("stat") != "OK":
            print(f"⚠️ {date_str} 沒有資料，已跳過")
            current += timedelta(days=1)
            continue

        tables = data.get("tables", [])
        if tables and "fields" in tables[0] and "data" in tables[0]:
            fields = tables[0]["fields"]
            rows = tables[0]["data"]

            df_day = pd.DataFrame(rows, columns=fields)
            df_day.insert(0, "日期", date_str)  # 加上日期欄位
            all_data.append(df_day)
        else:
            print(f"⚠️ {date_str} API 格式異常，已跳過")

        current += timedelta(days=1)

    # 合併所有日期的資料
    if not all_data:
        raise ValueError("❌ 區間內沒有任何有效資料")

    df_all = pd.concat(all_data, ignore_index=True)

    # 存成一個總檔案
    filename = f"{_date_to_str(sDt)}_{_date_to_str(eDt)}"
    _save_to_csv(df_all, apiEndpoint, filename)

    return df_all

# ======== 5. 注意股公告 ========
# 編號,證券代號,證券名稱,累計次數,注意交易資訊,日期,收盤價,本益比
def get_notice(sDt: datetime | None = None, eDt: datetime | None = None):
    if eDt > datetime.today():
        eDt = datetime.today()

    start_str = _date_to_str(sDt)
    end_str = _date_to_str(eDt)

    ### 先確認庫資料的上下界
    sql = "SELECT min(日期), MAX(日期) FROM twse_announcement_notice"
    df_check = query_to_df(sql)
    minDt = _roc_to_datetime(df_check["min(日期)"].iloc[0])
    maxDt = _roc_to_datetime(df_check["MAX(日期)"].iloc[0])
    print(minDt, maxDt)

    if (_is_fully_in_range(sDt, eDt, minDt, maxDt)):
        print("*** 直接從庫提取資料")
        sql = "SELECT * FROM twse_announcement_notice"
        sql += f" WHERE 日期 between '{_to_roc_date(sDt)}' AND '{_to_roc_date(eDt)}'"
        print(sql)
        df = query_to_df(sql)
        return df
    
    print("*** 從API提取資料")
    apiEndpoint = "announcement/notice"
    apiParams = f"querytype=1&stockNo=&selectType=&sortKind=STKNO&{common_params}"
    apiParams += f"&startDate={start_str}&endDate={end_str}"
    apiUrl = f"{twseUrl}/{apiEndpoint}?{apiParams}"

    res = requests.get(apiUrl)
    raw_data = res.json()
    data = raw_data["data"]

    ### 存到db裡
    if (_is_no_overlap(sDt, eDt, minDt, maxDt)):
        values = []
        for r in data:
            try:
                pe = str(r[7]).strip()
                pe_value = float(pe) if pe.replace('.', '', 1).isdigit() else None
                if pe_value is None:
                    continue
                values.append((
                    r[1].strip(),
                    r[2].strip(),
                    int(r[3]),
                    r[4].strip(),
                    r[5].strip(),
                    float(r[6]),
                    pe_value
                ))
            except:
                continue

        sql = """
        INSERT OR REPLACE INTO twse_announcement_notice
        (證券代號, 證券名稱, 累計次數, 注意交易資訊, 日期, 收盤價, 本益比)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        execute_sql(sql, values)

    df = pd.DataFrame(raw_data.get("data", []), columns=raw_data.get("fields", []))
    _save_to_csv(df, apiEndpoint, f"{start_str}_{end_str}")
    return df


# ======== 範例測試 ========
if __name__ == "__main__":
    year = 2023
    sDt = datetime(2020, 1, 1)
    eDt = datetime(2020, 12, 31)
    testData = get_notice(sDt, eDt)
    print(testData.head())

    # test = datetime.today()
    # test = test - relativedelta(months=1)

    # # 測試下載各項資料
    # get_stock_day("2330", test)
    # get_stock_day_avg("0050", test)
    # get_institutional_investors(test)
    # get_margin_trading(test)
    # get_notice(datetime(2025, 10, 1), datetime(2025, 10, 4))
