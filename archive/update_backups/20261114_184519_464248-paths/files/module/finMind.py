import sys
import os
from datetime import datetime
import pandas as pd
from FinMind.data import DataLoader 
from common import utils,db
import requests
from typing import Union
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

sys.stdout.reconfigure(encoding='utf-8')

### FinMind api設定
apiUrl = "https://api.finmindtrade.com/api/v4/data"
api = DataLoader()
token = os.getenv("FINMIND_TOKEN", "").strip()
if not token:
    raise RuntimeError(
        f"找不到 FINMIND_TOKEN，請確認已在 {PROJECT_ROOT / '.env'} 設定 FINMIND_TOKEN"
    )
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

storageDir_twDailyPriceAdj =  f"{storageDir}/TW/DailyPriceAdj"
os.makedirs(storageDir_twDailyPriceAdj, exist_ok=True)

# =========================================================
# 台股交易日
# =========================================================
def get_tw_stock_trading_dates() -> pd.DataFrame:
    """
    取得台股交易日，寫入 SQLite 後回傳 DataFrame。
    """

    print("--- run finMind.get_tw_stock_trading_dates ---")

    target_table = "fm_taiwan_stock_trading_date"

    parameter = {
        "dataset": "TaiwanStockTradingDate",
    }

    headers = {
        "Authorization": f"Bearer {token}"
    }

    resp = requests.get(apiUrl, headers=headers, params=parameter)
    resp.raise_for_status()

    json_data = resp.json()

    if "data" not in json_data:
        raise ValueError(f"FinMind 回傳格式異常：{json_data}")

    df = pd.DataFrame(json_data["data"])

    if df.empty:
        print("⚠️ FinMind TaiwanStockTradingDate 回傳空資料，改讀取本地資料表")
        return db.sql_to_df(
            f"""
            SELECT
                date
            FROM {target_table}
            ORDER BY date
            """
        )

    # =====================================================
    # 資料清洗
    # =====================================================
    df = utils._normalize_date_col(df, "date")

    df = df[["date"]].copy()
    df = df.dropna(subset=["date"])
    df = df.drop_duplicates(subset=["date"])
    df = df.sort_values("date").reset_index(drop=True)

    print(df.head())
    print(df.tail())

    # =====================================================
    # Upsert 入庫
    # =====================================================
    upsert_sql = f"""
    INSERT INTO {target_table}
    (
        date
    )
    VALUES (?)
    ON CONFLICT(date) DO NOTHING
    """

    params = list(
        df[["date"]].itertuples(index=False, name=None)
    )

    if params:
        db.execute_sql(upsert_sql, params)
        print(f"✅ 已寫入/略過既有交易日資料：{len(params)} 筆")
    else:
        print("⚠️ 無資料可寫入")

    # =====================================================
    # 從 DB 讀回並回傳
    # =====================================================
    result_df = db.sql_to_df(
        f"""
        SELECT
            date
        FROM {target_table}
        ORDER BY date
        """
    )

    if result_df is not None and not result_df.empty:
        print(
            f"📦 {target_table}："
            f"{result_df['date'].min()} ~ {result_df['date'].max()}，"
            f"共 {len(result_df)} 筆"
        )

    return result_df


# 取得台股日資料
def get_tw_stock_daily_price(
    stock_id: Union[str, list[str]],
    start_date: datetime,
    end_date: datetime,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    取得台股日成交資料，並寫入 fm_taiwan_stock_daily。

    支援三種模式：
    1. 單檔：
        get_tw_stock_daily_price("2330", start_date, end_date)

    2. 多檔：
        get_tw_stock_daily_price(["2330", "2317"], start_date, end_date)

    3. 全市場：
        get_tw_stock_daily_price("all", start_date, end_date)

    快取邏輯：
    - date_span idx_key="all" 代表該日期區間全市場資料已抓過。
    - 單檔查詢時會優先檢查 all span。
    - 若 all span 已涵蓋 requested range，直接查 DB，不打 API。
    - 若 all span 只涵蓋部分，會扣掉已涵蓋區間，只抓剩餘區間。
    """

    print(f"--- run finMind.get_tw_stock_daily_price--[{stock_id}]")

    target_table = "fm_taiwan_stock_daily"

    stock_ids = utils._ensure_list(stock_id)
    stock_ids = [str(sid).strip() for sid in stock_ids]

    req_s = pd.Timestamp(start_date).normalize()
    req_e = pd.Timestamp(end_date).normalize()

    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")

    today = utils._taipei_today()

    # API 抓取最多抓到今天，不抓未來
    fetch_req_e = min(req_e, today)

    # date_span 快取最多只記到昨天
    span_req_e = utils._cap_end_date_to_yesterday(req_e)

    if req_s > fetch_req_e:
        utils.ptMsg(
            f"⚠️ 請求區間只包含未來日期，不進行 API 抓取："
            f"req={utils._dstr(req_s)}~{utils._dstr(req_e)}，"
            f"fetch_req_e={utils._dstr(fetch_req_e)}"
        )
        return pd.DataFrame()

    upsert_sql = f"""
    INSERT INTO {target_table}
    (
        date,
        stock_id,
        "Trading_Volume",
        "Trading_money",
        open,
        max,
        min,
        close,
        spread,
        "Trading_turnover"
    )
    VALUES (?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(date, stock_id) DO UPDATE SET
        "Trading_Volume" = EXCLUDED."Trading_Volume",
        "Trading_money" = EXCLUDED."Trading_money",
        open = EXCLUDED.open,
        max = EXCLUDED.max,
        min = EXCLUDED.min,
        close = EXCLUDED.close,
        spread = EXCLUDED.spread,
        "Trading_turnover" = EXCLUDED."Trading_turnover"
    """

    def _clean_stock_daily_df(df_api: pd.DataFrame) -> pd.DataFrame:
        """
        清洗 FinMind taiwan_stock_daily 回傳資料。
        """
        if df_api is None or df_api.empty:
            return pd.DataFrame()

        df_api = df_api.copy()

        required_cols = [
            "date",
            "stock_id",
            "Trading_Volume",
            "Trading_money",
            "open",
            "max",
            "min",
            "close",
            "spread",
            "Trading_turnover",
        ]

        for col in required_cols:
            if col not in df_api.columns:
                df_api[col] = None

        df_api["date"] = pd.to_datetime(df_api["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df_api["stock_id"] = df_api["stock_id"].astype(str).str.strip()

        df_api = df_api.dropna(subset=["date", "stock_id"])
        df_api = df_api[df_api["stock_id"] != ""]

        df_api = df_api.drop_duplicates(subset=["date", "stock_id"], keep="last")

        return df_api[required_cols].copy()

    def _write_stock_daily_df(df_api: pd.DataFrame, log_prefix: str = "") -> int:
        """
        將清洗後資料寫入 DB。
        """
        if df_api is None or df_api.empty:
            return 0

        params = list(
            df_api[
                [
                    "date",
                    "stock_id",
                    "Trading_Volume",
                    "Trading_money",
                    "open",
                    "max",
                    "min",
                    "close",
                    "spread",
                    "Trading_turnover",
                ]
            ].itertuples(index=False, name=None)
        )

        if not params:
            return 0

        ok = db.execute_sql(upsert_sql, params)

        if not ok:
            utils.ptMsg(
                f"❌ stock_daily 寫入失敗：{log_prefix}，"
                f"預計寫入筆數：{len(params)}"
            )
            return 0

        if log_prefix:
            utils.ptMsg(
                f"✅ stock_daily 已寫入/更新：{log_prefix}，"
                f"筆數：{len(params)}"
            )

        return len(params)

    def _query_stock_daily_from_db(
        sid: str | None,
        q_s: pd.Timestamp,
        q_e: pd.Timestamp,
    ) -> pd.DataFrame:
        """
        從 DB 查 stock daily。
        sid=None 代表查全市場。
        """
        if sid is None:
            return db.query_to_df(
                f"""
                SELECT
                    date,
                    stock_id,
                    "Trading_Volume",
                    "Trading_money",
                    open,
                    max,
                    min,
                    close,
                    spread,
                    "Trading_turnover"
                FROM {target_table}
                WHERE date >= ?
                  AND date <= ?
                ORDER BY date, stock_id
                """,
                (utils._dstr(q_s), utils._dstr(q_e)),
            )

        return db.query_to_df(
            f"""
            SELECT
                date,
                stock_id,
                "Trading_Volume",
                "Trading_money",
                open,
                max,
                min,
                close,
                spread,
                "Trading_turnover"
            FROM {target_table}
            WHERE stock_id = ?
              AND date >= ?
              AND date <= ?
            ORDER BY date
            """,
            (sid, utils._dstr(q_s), utils._dstr(q_e)),
        )

    is_all_mode = len(stock_ids) == 1 and stock_ids[0].lower() == "all"

    if any(sid.lower() == "all" for sid in stock_ids) and not is_all_mode:
        raise ValueError(
            'stock_id="all" 不可和其他股票代號混用。'
            '請單獨呼叫 get_tw_stock_daily_price("all", ...)。'
        )

    # 先取得 all 快取區間
    all_s, all_e = db._get_date_span_record(target_table, "all")

    # ============================================================
    # all mode：逐日抓全市場
    # ============================================================
    if is_all_mode:
        utils.ptMsg(
            f"[stock_daily][ALL] "
            f"req={utils._dstr(req_s)}~{utils._dstr(req_e)}, "
            f"fetch_req={utils._dstr(req_s)}~{utils._dstr(fetch_req_e)}, "
            f"span_req_e={utils._dstr(span_req_e)}, "
            f"all_cache={utils._dstr(all_s) if all_s is not None else 'None'}~"
            f"{utils._dstr(all_e) if all_e is not None else 'None'}"
        )

        fetch_ranges = (
            [(req_s, fetch_req_e)]
            if force_refresh
            else db._subtract_covered_ranges(
                req_s=req_s,
                req_e=fetch_req_e,
                covered_spans=[(all_s, all_e)],
            )
        )
        
        utils.ptMsg(
            f"[stock_daily][ALL] fetch="
            f"{[(utils._dstr(s), utils._dstr(e)) for s, e in fetch_ranges]}"
        )

        total_written = 0

        for fs, fe in fetch_ranges:
            if fs > fe:
                continue

            # 逐日抓全市場
            all_dates = pd.date_range(fs, fe, freq="D")

            range_success = True
            range_written = 0

            for d in all_dates:
                d_str = utils._dstr(d)

                try:
                    df_api = api.taiwan_stock_daily(
                        start_date=d_str,
                    )
                except Exception as e:
                    range_success = False
                    utils.ptMsg(f"❌ FinMind stock_daily 全市場抓取失敗：{d_str}，{e}")
                    continue

                # 週末或休市日可能是空資料，這裡不視為失敗
                if df_api is None or df_api.empty:
                    utils.ptMsg(f"⚠️ FinMind stock_daily 全市場回傳空資料：{d_str}，可能為休市日")
                    continue

                df_api = _clean_stock_daily_df(df_api)

                if df_api.empty:
                    utils.ptMsg(f"⚠️ stock_daily 全市場清洗後無有效資料：{d_str}")
                    continue

                written = _write_stock_daily_df(df_api, log_prefix=f"ALL {d_str}")
                range_written += written
                total_written += written

            # 只有該 fetch range 沒有 API exception，
            # 且本區間真的有有效資料寫入 DB，才更新 all span。
            # 空資料日，例如週末或休市日，不算失敗；
            # 但如果整個區間 range_written == 0，就不更新 date_span。
            if range_success and range_written > 0:
                safe_fe = utils._cap_end_date_to_yesterday(fe)

                if fs <= safe_fe:
                    db._upsert_date_span_merge(
                        target_table=target_table,
                        idx_key="all",
                        new_s=fs,
                        new_e=safe_fe,
                    )

                    db._delete_stock_spans_fully_covered_by_all(target_table)

                    utils.ptMsg(
                        f"📌 date_span 已更新 ALL："
                        f"{utils._dstr(fs)}~{utils._dstr(safe_fe)}，"
                        f"本區間寫入/更新筆數：{range_written}"
                    )
                else:
                    utils.ptMsg(
                        f"📌 本區間有資料寫入，但日期超過 date_span 可快取範圍，"
                        f"不更新 ALL date_span：{utils._dstr(fs)}~{utils._dstr(fe)}，"
                        f"本區間寫入/更新筆數：{range_written}"
                    )

            elif range_success and range_written == 0:
                utils.ptMsg(
                    f"⚠️ 本區間 API 無錯誤，但沒有任何有效資料寫入，"
                    f"不更新 ALL date_span：{utils._dstr(fs)}~{utils._dstr(fe)}"
                )

            else:
                utils.ptMsg(
                    f"⚠️ 因本區間有 API 抓取失敗，"
                    f"未更新 ALL date_span：{utils._dstr(fs)}~{utils._dstr(fe)}"
                )
                

        utils.ptMsg(
            f"📌 stock_daily 全市場模式完成："
            f"{utils._dstr(req_s)}~{utils._dstr(fetch_req_e)}，"
            f"總寫入/更新筆數：{total_written}"
        )
        return _query_stock_daily_from_db(
            sid=None,
            q_s=req_s,
            q_e=fetch_req_e,
        )

    # ============================================================
    # 單檔 / 多檔模式
    # ============================================================
    all_dfs: list[pd.DataFrame] = []

    for sid in stock_ids:
        sid = str(sid).strip()
        sid_written_total = 0

        # 個股自己的 cache span
        sid_s, sid_e = db._get_date_span_record(target_table, sid)

        # covered spans = all cache + sid cache
        covered_spans = [
            (all_s, all_e),
            (sid_s, sid_e),
        ]

        fetch_ranges = (
            [(req_s, fetch_req_e)]
            if force_refresh
            else db._subtract_covered_ranges(
                req_s=req_s,
                req_e=fetch_req_e,
                covered_spans=covered_spans,
            )
        )

        utils.ptMsg(
            f"[stock_daily][{sid}] "
            f"req={utils._dstr(req_s)}~{utils._dstr(req_e)}, "
            f"all_cache={utils._dstr(all_s) if all_s is not None else 'None'}~"
            f"{utils._dstr(all_e) if all_e is not None else 'None'}, "
            f"stock_cache={utils._dstr(sid_s) if sid_s is not None else 'None'}~"
            f"{utils._dstr(sid_e) if sid_e is not None else 'None'}, "
            f"fetch={[(utils._dstr(s), utils._dstr(e)) for s, e in fetch_ranges]}"
        )

        for fs, fe in fetch_ranges:
            if fs > fe:
                continue

            try:
                df_api = api.taiwan_stock_daily(
                    stock_id=sid,
                    start_date=utils._dstr(fs),
                    end_date=utils._dstr(fe),
                )
            except Exception as e:
                utils.ptMsg(
                    f"❌ FinMind stock_daily 抓取失敗："
                    f"{sid} {utils._dstr(fs)}~{utils._dstr(fe)}，{e}"
                )
                continue

            if df_api is None or df_api.empty:
                utils.ptMsg(
                    f"⚠️ FinMind stock_daily 回傳空資料："
                    f"{sid} {utils._dstr(fs)}~{utils._dstr(fe)}，不更新 span"
                )
                continue

            df_api = _clean_stock_daily_df(df_api)

            # 單檔模式保險濾掉其他股票
            df_api = df_api[df_api["stock_id"] == sid].copy()

            if df_api.empty:
                utils.ptMsg(
                    f"⚠️ stock_daily 清洗後無有效資料："
                    f"{sid} {utils._dstr(fs)}~{utils._dstr(fe)}，不更新 span"
                )
                continue

            written = _write_stock_daily_df(df_api, log_prefix=sid)
            sid_written_total += written

            safe_fe = utils._cap_end_date_to_yesterday(fe)

            if written > 0 and fs <= safe_fe:
                db._upsert_date_span_merge(
                    target_table=target_table,
                    idx_key=sid,
                    new_s=fs,
                    new_e=safe_fe,
                )

                utils.ptMsg(
                    f"📌 date_span 已更新 stock_daily [{sid}]："
                    f"{utils._dstr(fs)}~{utils._dstr(safe_fe)}"
                )

            elif written > 0:
                utils.ptMsg(
                    f"📌 stock_daily [{sid}] 本區間有資料寫入，"
                    f"但日期超過 date_span 可快取範圍，不更新 date_span："
                    f"{utils._dstr(fs)}~{utils._dstr(fe)}"
                )

        # 注意：
        # 如果 fetch_ranges 為空，代表 all 或 sid cache 已完整涵蓋。
        # 這種情況不需要同步 sid span。
        #
        # 如果有 fetch_ranges，但本次沒有任何有效資料寫入，也不應同步 sid span。
        if fetch_ranges and sid_written_total > 0:
            safe_sync_e = utils._cap_end_date_to_yesterday(fetch_req_e)

            if req_s <= safe_sync_e:
                db._upsert_date_span_merge(
                    target_table=target_table,
                    idx_key=sid,
                    new_s=req_s,
                    new_e=safe_sync_e,
                )

                db._delete_stock_spans_fully_covered_by_all(target_table)

                utils.ptMsg(
                    f"📌 date_span 已更新 stock_daily [{sid}]："
                    f"{utils._dstr(req_s)}~{utils._dstr(safe_sync_e)}"
                )
            else:
                utils.ptMsg(
                    f"📌 stock_daily [{sid}] 本次有資料寫入，"
                    f"但資料日期超過 date_span 可快取範圍，不更新 date_span"
                )

        elif fetch_ranges and sid_written_total == 0:
            utils.ptMsg(
                f"⚠️ stock_daily [{sid}] 本次有 fetch range，"
                f"但沒有任何有效資料寫入，不同步 date_span"
            )

        df_sid = _query_stock_daily_from_db(
            sid=sid,
            q_s=req_s,
            q_e=fetch_req_e,
        )

        if df_sid is not None and not df_sid.empty:
            all_dfs.append(df_sid)

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)


def get_tw_institutional_total(
    start_date: datetime,
    end_date: datetime,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    取得台灣市場整體三大法人買賣超（TaiwanStockTotalInstitutionalInvestors）
    並快取到 fm_taiwan_stock_institutional_total
    span 表使用：target_table='fm_taiwan_stock_institutional_total', stock_id='ALL'
    """
    print("--- run finMind.get_tw_institutional_total ---")
    target_table = "fm_taiwan_stock_institutional_total"
    span_key = "ALL"

    req_s = pd.Timestamp(start_date).normalize()
    req_e = pd.Timestamp(end_date).normalize()
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")

    def dstr(t: pd.Timestamp) -> str:
        return t.strftime("%Y-%m-%d")

    # === 1) 查 span ===
    span_row = db.query_to_df(
        """
        SELECT start_date, end_date
        FROM date_span
        WHERE target_table = ? AND idx_key = ?
        """,
        (target_table, span_key),
    )

    mem_s = (
        pd.Timestamp(span_row.loc[0, "start_date"]).normalize()
        if not span_row.empty
        else None
    )
    mem_e = (
        pd.Timestamp(span_row.loc[0, "end_date"]).normalize()
        if not span_row.empty
        else None
    )

    fetch_ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []

    if force_refresh:
        # 維護修補：即使日期已被 date_span 涵蓋，仍重新向 API 取得並 upsert。
        fetch_ranges = [(req_s, req_e)]
    elif mem_s is None:
        # 沒有任何快取 -> 全段補
        fetch_ranges = [(req_s, req_e)]
    else:
        if req_s >= mem_s and req_e <= mem_e:
            # 完全被舊資料涵蓋
            fetch_ranges = []
        elif req_s > mem_e:
            # 完全在舊資料右邊
            fetch_ranges = [(mem_e + pd.Timedelta(days=1), req_e)]
        elif req_e < mem_s:
            # 完全在舊資料左邊
            fetch_ranges = [(req_s, mem_s - pd.Timedelta(days=1))]
        else:
            # 有重疊：左右各缺一段
            if req_s < mem_s:
                fetch_ranges.append((req_s, mem_s - pd.Timedelta(days=1)))
            if req_e > mem_e:
                fetch_ranges.append((mem_e + pd.Timedelta(days=1), req_e))

    # === 2) 向 FinMind 補資料並寫入快取 ===
    upsert_sql = f"""
    INSERT INTO {target_table}
      (date, name, buy, sell)
    VALUES (?,?,?,?)
    ON CONFLICT(date, name) DO UPDATE SET
      buy        = excluded.buy,
      sell       = excluded.sell,
      updated_at = CURRENT_TIMESTAMP;
    """

    for fs, fe in fetch_ranges:
        if fs > fe:
            continue

        df_api = api.taiwan_stock_institutional_investors_total(
            start_date=dstr(fs),
            end_date=dstr(fe),
        )

        if df_api is not None and not df_api.empty:
            df_api = df_api.copy()
            df_api["date"] = pd.to_datetime(df_api["date"]).dt.strftime("%Y-%m-%d")
            params = list(
                df_api[["date", "name", "buy", "sell"]].itertuples(
                    index=False, name=None
                )
            )
            db.execute_sql(upsert_sql, params)

    # === 3) 更新 span ===
    # 重點：用 DB 實際資料同步 date_span
    utils._sync_date_span_from_actual_table(
        target_table=target_table,
        idx_key=span_key,
        idx_field=None,
        date_field="date",
    )

    # === 4) 從快取表讀出需求區間 ===
    df = db.query_to_df(
        f"""
        SELECT date, name, buy, sell
        FROM {target_table}
        WHERE date >= ?
          AND date <= ?
        ORDER BY date, name
        """,
        (dstr(req_s), dstr(req_e)),
    )

    return df

# 台灣市場整體融資融劵表 
def get_tw_margin_total(
    start_date: datetime,
    end_date: datetime = datetime.now(),
) -> pd.DataFrame:
    """
    FinMind - TaiwanStockTotalMarginPurchaseShortSale
    快取表：fm_taiwan_stock_margin_total
    span 表：date_span
      - target_table = 'fm_taiwan_stock_margin_total'
      - idx_key = NULL
    """
    print("--- run finMind.get_tw_margin_total_finmind ---")

    target_table = "fm_taiwan_stock_margin_total"
    span_key = "date"   # 不需要 idx_key

    req_s = pd.Timestamp(start_date).normalize()
    req_e = pd.Timestamp(end_date).normalize()
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")

    def dstr(t: pd.Timestamp) -> str:
        return t.strftime("%Y-%m-%d")

    # === 1) 查 span ===
    span_row = db.query_to_df(
        """
        SELECT start_date, end_date
        FROM date_span
        WHERE target_table = ?
          AND idx_key = ?
        """,
        (target_table, span_key),
    )

    mem_s = pd.Timestamp(span_row.loc[0, "start_date"]).normalize() if not span_row.empty else None
    mem_e = pd.Timestamp(span_row.loc[0, "end_date"]).normalize() if not span_row.empty else None

    fetch_ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []

    if mem_s is None:
        fetch_ranges = [(req_s, req_e)]
    else:
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

    # === 2) 補資料（一次一段，避免 ban） ===
    upsert_sql = f"""
    INSERT INTO {target_table}
    (
        date,
        name,
        buy,
        sell,
        "TodayBalance",
        "YesBalance",
        "Return"
    )
    VALUES (?,?,?,?,?,?,?)
    ON CONFLICT(date, name) DO UPDATE SET
        buy = EXCLUDED.buy,
        sell = EXCLUDED.sell,
        "TodayBalance" = EXCLUDED."TodayBalance",
        "YesBalance" = EXCLUDED."YesBalance",
        "Return" = EXCLUDED."Return",
        updated_at = CURRENT_TIMESTAMP
    """

    for fs, fe in fetch_ranges:
        if fs > fe:
            continue

        df_api = api.taiwan_stock_margin_purchase_short_sale_total(
            start_date=dstr(fs),
            end_date=dstr(fe),
        )

        if df_api is None or df_api.empty:
            continue

        df_api = df_api.copy()
        df_api["date"] = pd.to_datetime(df_api["date"]).dt.strftime("%Y-%m-%d")

        params = list(
            df_api[
                ["date", "name", "buy", "sell", "TodayBalance", "YesBalance", "Return"]
            ].itertuples(index=False, name=None)
        )

        db.execute_sql(upsert_sql, params)

    # === 3) 重新同步 date_span：只能用 DB 實際資料，不可用請求日期 ===
    utils._sync_date_span_from_actual_table(
        target_table=target_table,
        idx_key=span_key,
        idx_field=None,
        date_field="date",
    )

    # === 4) 從 DB 回傳 ===
    df = db.query_to_df(
        f"""
        SELECT
            date,
            name,
            buy,
            sell,
            "TodayBalance",
            "YesBalance",
            "Return"
        FROM {target_table}
        WHERE date >= ?
        AND date <= ?
        ORDER BY date, name
        """,
        (dstr(req_s), dstr(req_e)),
    )

    return df

# =========================================================
# 1. 期貨日成交資訊
# =========================================================
def get_tw_futures_daily(
    futures_id: Union[str, list[str]],
    start_date: datetime,
    end_date: datetime,
    *,
    recent_refresh_days: int = 14,
    force_refresh: bool = False,
) -> pd.DataFrame:
    print(f"--- run finMind.get_tw_futures_daily--[{futures_id}]")
    api = getDataLoader()
    target_table = "fm_taiwan_futures_daily"

    req_s = utils._to_norm_ts(start_date)
    req_e = utils._to_norm_ts(end_date)
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")

    utils._fetch_with_cache.req_s = req_s
    utils._fetch_with_cache.req_e = req_e

    def fetcher(fid: str, fs: pd.Timestamp, fe: pd.Timestamp):
        df = api.taiwan_futures_daily(
            futures_id=fid,
            start_date=utils._dstr(fs),
            end_date=utils._dstr(fe),
        )
        if df is None or df.empty:
            return pd.DataFrame()
        else:
            print(df.head(), df.tail())

        df = utils._normalize_date_col(df, "date")
        df = utils._to_str_cols(df, ["futures_id", "contract_date", "trading_session"])
        df = utils._to_num_cols(
            df,
            [
                "open", "max", "min", "close",
                "spread", "spread_per",
                "volume", "settlement_price", "open_interest",
            ],
        )
        return df

    upsert_sql = f"""
    INSERT INTO {target_table}
    (
        date, futures_id, contract_date, open, max, min, close,
        spread, spread_per, volume, settlement_price, open_interest, trading_session
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(date, futures_id, contract_date, trading_session) DO UPDATE SET
      open             = excluded.open,
      max              = excluded.max,
      min              = excluded.min,
      close            = excluded.close,
      spread           = excluded.spread,
      spread_per       = excluded.spread_per,
      volume           = excluded.volume,
      settlement_price = excluded.settlement_price,
      open_interest    = excluded.open_interest
    """

    def upsert_df(df: pd.DataFrame):
        params = list(
            df[
                [
                    "date", "futures_id", "contract_date",
                    "open", "max", "min", "close",
                    "spread", "spread_per", "volume",
                    "settlement_price", "open_interest", "trading_session",
                ]
            ].itertuples(index=False, name=None)
        )
        if params:
            db.execute_sql(upsert_sql, params)

    return utils._fetch_with_cache(
        ids=futures_id,
        target_table=target_table,
        idx_field="futures_id",
        fetcher=fetcher,
        upsert_df=upsert_df,
        select_cols_sql="""
            date, futures_id, contract_date, open, max, min, close,
            spread, spread_per, volume, settlement_price, open_interest, trading_session
        """,
        order_by_sql="date, contract_date, trading_session",
        debug_name="futures_daily",
        output_csv="finMind.csv",
        # 盤後行情以次一交易日為 date；週五夜盤會標成下週一，
        # 春節等連假則可能跨更多日曆日，因此只對本資料集放寬查詢範圍。
        max_future_days=14,
        # 夜盤可能先建立未來交易日，讓 date_span 越過缺漏的日盤日期。
        # 固定覆抓近期資料，週末再次更新時即可補回週五日盤。
        recent_refresh_days=recent_refresh_days,
        force_refresh=force_refresh,
    )

# =========================================================
# 2. 期貨三大法人買賣
# =========================================================
def get_tw_futures_institutional_investors(
    futures_id: Union[str, list[str]],
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    print(f"--- run finMind.get_tw_futures_institutional_investors--[{futures_id}]")
    api = getDataLoader()
    target_table = "fm_taiwan_futures_institutional_investors"

    req_s = utils._to_norm_ts(start_date)
    req_e = utils._to_norm_ts(end_date)
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")
    utils._fetch_with_cache.req_s = req_s
    utils._fetch_with_cache.req_e = req_e

    def fetcher(did: str, fs: pd.Timestamp, fe: pd.Timestamp):
        df = api.taiwan_futures_institutional_investors(
            data_id=did,
            start_date=utils._dstr(fs),
            end_date=utils._dstr(fe),
        )
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        if "data_id" not in df.columns:
            if "name" in df.columns:
                df["data_id"] = df["name"]
            else:
                df["data_id"] = did

        df = utils._normalize_date_col(df, "date")
        df = utils._to_str_cols(df, ["data_id", "institutional_investors"])
        df = utils._to_num_cols(
            df,
            [
                "long_deal_volume", "long_deal_amount",
                "short_deal_volume", "short_deal_amount",
                "long_open_interest_balance_volume", "long_open_interest_balance_amount",
                "short_open_interest_balance_volume", "short_open_interest_balance_amount",
            ],
        )
        return df

    upsert_sql = f"""
    INSERT INTO {target_table}
    (
        date, data_id, institutional_investors,
        long_deal_volume, long_deal_amount,
        short_deal_volume, short_deal_amount,
        long_open_interest_balance_volume, long_open_interest_balance_amount,
        short_open_interest_balance_volume, short_open_interest_balance_amount
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(date, data_id, institutional_investors) DO UPDATE SET
        long_deal_volume                    = excluded.long_deal_volume,
        long_deal_amount                    = excluded.long_deal_amount,
        short_deal_volume                   = excluded.short_deal_volume,
        short_deal_amount                   = excluded.short_deal_amount,
        long_open_interest_balance_volume   = excluded.long_open_interest_balance_volume,
        long_open_interest_balance_amount   = excluded.long_open_interest_balance_amount,
        short_open_interest_balance_volume  = excluded.short_open_interest_balance_volume,
        short_open_interest_balance_amount  = excluded.short_open_interest_balance_amount
    """

    def upsert_df(df: pd.DataFrame):
        params = list(
            df[
                [
                    "date", "data_id", "institutional_investors",
                    "long_deal_volume", "long_deal_amount",
                    "short_deal_volume", "short_deal_amount",
                    "long_open_interest_balance_volume", "long_open_interest_balance_amount",
                    "short_open_interest_balance_volume", "short_open_interest_balance_amount",
                ]
            ].itertuples(index=False, name=None)
        )
        if params:
            db.execute_sql(upsert_sql, params)

    return utils._fetch_with_cache(
        ids=futures_id,
        target_table=target_table,
        idx_field="data_id",
        fetcher=fetcher,
        upsert_df=upsert_df,
        select_cols_sql="""
            date, data_id, institutional_investors,
            long_deal_volume, long_deal_amount,
            short_deal_volume, short_deal_amount,
            long_open_interest_balance_volume, long_open_interest_balance_amount,
            short_open_interest_balance_volume, short_open_interest_balance_amount
        """,
        order_by_sql="date, institutional_investors",
        debug_name="futures_inst",
        output_csv="finMind_futures_institutional_investors.csv",
    )


# =========================================================
# 3. 期貨夜盤三大法人買賣
# =========================================================
def get_tw_futures_institutional_investors_after_hours(
    futures_id: Union[str, list[str]],
    start_date: datetime,
    end_date: datetime,
    *,
    recent_refresh_days: int = 14,
    force_refresh: bool = False,
) -> pd.DataFrame:
    print(f"--- run finMind.get_tw_futures_institutional_investors_after_hours--[{futures_id}]")
    target_table = "fm_taiwan_futures_institutional_investors_after_hours"

    req_s = utils._to_norm_ts(start_date)
    req_e = utils._to_norm_ts(end_date)
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")
    utils._fetch_with_cache.req_s = req_s
    utils._fetch_with_cache.req_e = req_e

    def fetcher(fid: str, fs: pd.Timestamp, fe: pd.Timestamp):
        params = {
            "dataset": "TaiwanFuturesInstitutionalInvestorsAfterHours",
            "data_id": fid,
            "start_date": utils._dstr(fs),
            "end_date": utils._dstr(fe),
            "token": token,
        }
        resp = requests.get(apiUrl, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if "data" not in data:
            raise ValueError(f"FinMind API 回傳格式異常: {data}")

        df = pd.DataFrame(data["data"])
        if df.empty:
            return pd.DataFrame()

        df = utils._normalize_date_col(df, "date")
        df = utils._to_str_cols(df, ["futures_id", "institutional_investors"])
        df = utils._to_num_cols(df, ["long_deal_volume", "long_deal_amount", "short_deal_volume", "short_deal_amount"])
        return df

    upsert_sql = f"""
    INSERT INTO {target_table}
    (
        date, futures_id, institutional_investors,
        long_deal_volume, long_deal_amount,
        short_deal_volume, short_deal_amount
    )
    VALUES (?,?,?,?,?,?,?)
    ON CONFLICT(date, futures_id, institutional_investors) DO UPDATE SET
        long_deal_volume  = excluded.long_deal_volume,
        long_deal_amount  = excluded.long_deal_amount,
        short_deal_volume = excluded.short_deal_volume,
        short_deal_amount = excluded.short_deal_amount
    """

    def upsert_df(df: pd.DataFrame):
        params = list(
            df[
                [
                    "date", "futures_id", "institutional_investors",
                    "long_deal_volume", "long_deal_amount",
                    "short_deal_volume", "short_deal_amount",
                ]
            ].itertuples(index=False, name=None)
        )
        if params:
            db.execute_sql(upsert_sql, params)

    return utils._fetch_with_cache(
        ids=futures_id,
        target_table=target_table,
        idx_field="futures_id",
        fetcher=fetcher,
        upsert_df=upsert_df,
        select_cols_sql="""
            date, futures_id, institutional_investors,
            long_deal_volume, long_deal_amount,
            short_deal_volume, short_deal_amount
        """,
        order_by_sql="date, institutional_investors",
        debug_name="futures_inst_ah",
        output_csv="finMind_futures_institutional_investors_after_hours.csv",
        # 夜盤法人使用報表交易日本身的 date；與期貨夜盤行情的
        # 「下一交易日」歸屬不同。固定覆抓近期資料避免中間缺口。
        recent_refresh_days=recent_refresh_days,
        force_refresh=force_refresh,
    )


# =========================================================
# 4. 期貨最後結算價
# =========================================================
def get_tw_futures_final_settlement_price(
    futures_id: Union[str, list[str]],
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    print(f"--- run finMind.get_tw_futures_final_settlement_price--[{futures_id}]")
    target_table = "fm_taiwan_futures_final_settlement_price"

    req_s = utils._to_norm_ts(start_date)
    req_e = utils._to_norm_ts(end_date)
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")
    utils._fetch_with_cache.req_s = req_s
    utils._fetch_with_cache.req_e = req_e

    def fetcher(fid: str, fs: pd.Timestamp, fe: pd.Timestamp):
        params = {
            "dataset": "TaiwanFuturesFinalSettlementPrice",
            "data_id": fid,
            "start_date": utils._dstr(fs),
            "end_date": utils._dstr(fe),
            "token": token,
        }
        resp = requests.get(apiUrl, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if "data" not in data:
            raise ValueError(f"FinMind API 回傳格式異常: {data}")

        df = pd.DataFrame(data["data"])
        if df.empty:
            return pd.DataFrame()

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df[(df["date"] >= fs) & (df["date"] <= fe)]
        if df.empty:
            return pd.DataFrame()

        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        df = utils._to_str_cols(df, ["contract_month", "futures_id"])
        df = utils._fill_text_cols(df, ["futures_type", "futures_name", "underlying_code"])
        df = utils._to_num_cols(df, ["settlement_price", "notional_value"])
        return df

    upsert_sql = f"""
    INSERT INTO {target_table}
    (
        date, contract_month, futures_type, futures_id, futures_name,
        settlement_price, underlying_code, notional_value
    )
    VALUES (?,?,?,?,?,?,?,?)
    ON CONFLICT(date, contract_month, futures_id) DO UPDATE SET
        futures_type      = excluded.futures_type,
        futures_name      = excluded.futures_name,
        settlement_price  = excluded.settlement_price,
        underlying_code   = excluded.underlying_code,
        notional_value    = excluded.notional_value
    """

    def upsert_df(df: pd.DataFrame):
        params = list(
            df[
                [
                    "date", "contract_month", "futures_type", "futures_id", "futures_name",
                    "settlement_price", "underlying_code", "notional_value",
                ]
            ].itertuples(index=False, name=None)
        )
        if params:
            db.execute_sql(upsert_sql, params)

    return utils._fetch_with_cache(
        ids=futures_id,
        target_table=target_table,
        idx_field="futures_id",
        fetcher=fetcher,
        upsert_df=upsert_df,
        select_cols_sql="""
            date, contract_month, futures_type, futures_id, futures_name,
            settlement_price, underlying_code, notional_value
        """,
        order_by_sql="date, contract_month",
        debug_name="futures_final_settlement",
        output_csv=None,
    )


# =========================================================
# 5. 選擇權日成交資訊
# =========================================================
def get_tw_option_daily(
    option_id: Union[str, list[str]],
    start_date: datetime,
    end_date: datetime,
    force_refresh: bool = False,
) -> pd.DataFrame:
    print(f"--- run finMind.get_tw_option_daily--[{option_id}]")
    api = getDataLoader()
    target_table = "fm_taiwan_option_daily"

    req_s = utils._to_norm_ts(start_date)
    req_e = utils._to_norm_ts(end_date)
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")

    utils._fetch_with_cache.req_s = req_s
    utils._fetch_with_cache.req_e = req_e

    def fetcher(oid: str, fs: pd.Timestamp, fe: pd.Timestamp):
        all_month_df = []
        current = fs.normalize()

        while current <= fe:
            month_end = (current + pd.offsets.MonthEnd(0)).normalize()
            if month_end > fe:
                month_end = fe

            print(f"抓取 {oid} {current.date()} ~ {month_end.date()}")

            try:
                df = api.taiwan_option_daily(
                    option_id=oid,
                    start_date=utils._dstr(current),
                    end_date=utils._dstr(month_end),
                )
            except Exception as e:
                print(f"⚠️ 當月失敗：{e}")
                current = month_end + pd.Timedelta(days=1)
                continue

            if df is not None and not df.empty:
                df = utils._normalize_date_col(df, "date")
                df = utils._to_str_cols(df, ["option_id", "contract_date", "call_put", "trading_session"])
                df = utils._to_num_cols(
                    df,
                    [
                        "strike_price", "open", "max", "min", "close",
                        "volume", "settlement_price", "open_interest",
                    ],
                )
                all_month_df.append(df)

            current = month_end + pd.Timedelta(days=1)

        return pd.concat(all_month_df, ignore_index=True) if all_month_df else pd.DataFrame()

    upsert_sql = f"""
    INSERT INTO {target_table}
    (
        date, option_id, contract_date, strike_price, call_put,
        open, max, min, close, volume,
        settlement_price, open_interest, trading_session
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(date, option_id, contract_date, strike_price, call_put, trading_session) DO UPDATE SET
      open             = excluded.open,
      max              = excluded.max,
      min              = excluded.min,
      close            = excluded.close,
      volume           = excluded.volume,
      settlement_price = excluded.settlement_price,
      open_interest    = excluded.open_interest
    """

    def upsert_df(df: pd.DataFrame):
        params = list(
            df[
                [
                    "date", "option_id", "contract_date",
                    "strike_price", "call_put",
                    "open", "max", "min", "close",
                    "volume", "settlement_price", "open_interest", "trading_session",
                ]
            ].itertuples(index=False, name=None)
        )
        if params:
            db.execute_sql(upsert_sql, params)

    return utils._fetch_with_cache(
        ids=option_id,
        target_table=target_table,
        idx_field="option_id",
        fetcher=fetcher,
        upsert_df=upsert_df,
        select_cols_sql="""
            date, option_id, contract_date, strike_price, call_put,
            open, max, min, close, volume,
            settlement_price, open_interest, trading_session
        """,
        order_by_sql="date, contract_date, strike_price, call_put, trading_session",
        debug_name="option_daily",
        output_csv=None,
        force_refresh=force_refresh,
    )

# =========================================================
# 6. 選擇權最後結算價
# =========================================================
def get_tw_option_final_settlement_price(
    option_id: Union[str, list[str]],
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    print(f"--- run finMind.get_tw_option_final_settlement_price--[{option_id}]")
    target_table = "fm_taiwan_option_final_settlement_price"

    req_s = utils._to_norm_ts(start_date)
    req_e = utils._to_norm_ts(end_date)
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")
    utils._fetch_with_cache.req_s = req_s
    utils._fetch_with_cache.req_e = req_e

    def fetcher(oid: str, fs: pd.Timestamp, fe: pd.Timestamp):
        params = {
            "dataset": "TaiwanOptionFinalSettlementPrice",
            "data_id": oid,
            "start_date": utils._dstr(fs),
            "end_date": utils._dstr(fe),
            "token": token,
        }
        resp = requests.get(apiUrl, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if "data" not in data:
            raise ValueError(f"FinMind API 回傳格式異常: {data}")

        df = pd.DataFrame(data["data"])
        if df.empty:
            return pd.DataFrame()

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df[(df["date"] >= fs) & (df["date"] <= fe)]
        if df.empty:
            return pd.DataFrame()

        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        df = utils._to_str_cols(df, ["contract_month", "option_id"])
        df = utils._fill_text_cols(df, ["option_type", "option_name", "underlying_code"])
        df = utils._to_num_cols(df, ["settlement_price", "notional_value"])
        return df

    upsert_sql = f"""
    INSERT INTO {target_table}
    (
        date, contract_month, option_type, option_id, option_name,
        settlement_price, underlying_code, notional_value
    )
    VALUES (?,?,?,?,?,?,?,?)
    ON CONFLICT(date, contract_month, option_id) DO UPDATE SET
        option_type       = excluded.option_type,
        option_name       = excluded.option_name,
        settlement_price  = excluded.settlement_price,
        underlying_code   = excluded.underlying_code,
        notional_value    = excluded.notional_value
    """

    def upsert_df(df: pd.DataFrame):
        params = list(
            df[
                [
                    "date", "contract_month", "option_type", "option_id", "option_name",
                    "settlement_price", "underlying_code", "notional_value",
                ]
            ].itertuples(index=False, name=None)
        )
        if params:
            db.execute_sql(upsert_sql, params)

    return utils._fetch_with_cache(
        ids=option_id,
        target_table=target_table,
        idx_field="option_id",
        fetcher=fetcher,
        upsert_df=upsert_df,
        select_cols_sql="""
            date, contract_month, option_type, option_id, option_name,
            settlement_price, underlying_code, notional_value
        """,
        order_by_sql="date, contract_month",
        debug_name="option_final_settlement",
        output_csv="finMind_option_final_settlement_price.csv",
    )


# =========================================================
# 7. 台股／加權指數 1 分 K（TaiwanStockKBar）
# =========================================================
def _intraday_request_dates(
    start_date: datetime,
    end_date: datetime,
) -> list[pd.Timestamp]:
    """从交易日表取得 API 应请求日期，避免浪费每日请求额度。"""
    req_s = utils._to_norm_ts(start_date)
    req_e = min(utils._to_norm_ts(end_date), utils._taipei_today())
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date，且不可全部位於未來")

    trading_dates = db.query_to_df(
        """
        SELECT date
        FROM fm_taiwan_stock_trading_date
        WHERE date >= ? AND date <= ?
        ORDER BY date
        """,
        (utils._dstr(req_s), utils._dstr(req_e)),
    )
    if trading_dates is None or trading_dates.empty:
        raise RuntimeError(
            "fm_taiwan_stock_trading_date 在指定区间没有资料；"
            "请先执行 get_tw_stock_trading_dates()，再下载分 K／期货 Tick。"
        )
    return [pd.Timestamp(value).normalize() for value in trading_dates["date"]]


def _futures_tick_request_dates(
    start_date: datetime,
    end_date: datetime,
) -> list[pd.Timestamp]:
    """取得期貨 Tick 所需日曆日期，包含每個交易日的隔日凌晨檔。"""
    req_s = utils._to_norm_ts(start_date)
    req_e = min(utils._to_norm_ts(end_date), utils._taipei_today())
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date，且不可全部位於未來")

    trading_dates = db.query_to_df(
        """
        SELECT date
        FROM fm_taiwan_stock_trading_date
        WHERE date >= ? AND date <= ?
        ORDER BY date
        """,
        (utils._dstr(req_s - pd.Timedelta(days=1)), utils._dstr(req_e)),
    )
    if trading_dates is None or trading_dates.empty:
        raise RuntimeError("交易日表在指定區間沒有資料，請先更新交易日表")

    dates: set[pd.Timestamp] = set()
    for value in trading_dates["date"]:
        trading_date = pd.Timestamp(value).normalize()
        for request_date in (trading_date, trading_date + pd.Timedelta(days=1)):
            if req_s <= request_date <= req_e:
                dates.add(request_date)
    return sorted(dates)


def _stored_intraday_dates(
    target_table: str,
    id_field: str,
    id_value: str,
    date_field: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> set[pd.Timestamp]:
    df = db.query_to_df(
        f"""
        SELECT DISTINCT {date_field} AS stored_date
        FROM {target_table}
        WHERE {id_field} = ?
          AND {date_field} >= ?
          AND {date_field} <= ?
        """,
        (id_value, utils._dstr(start_date), utils._dstr(end_date)),
    )
    if df is None or df.empty:
        return set()
    return {
        pd.Timestamp(value).normalize()
        for value in df["stored_date"]
        if pd.notna(value)
    }


def get_tw_stock_kbar(
    stock_id: Union[str, list[str]],
    start_date: datetime,
    end_date: datetime,
    *,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """下载 TaiwanStockKBar 并写入 fm_taiwan_stock_kbar_1m。

    FinMind 一次只提供一天，因此函数会依交易日逐日检查缺口。
    已有日期自动跳过；今天会固定重抓，避免盘中半成品被视为完成。
    回传值是每个商品、日期的同步结果摘要，不会把多年分 K 全部载回内存。
    """
    print(f"--- run finMind.get_tw_stock_kbar--[{stock_id}]")
    target_table = "fm_taiwan_stock_kbar_1m"
    ids = [str(value).strip() for value in utils._ensure_list(stock_id)]
    request_dates = _intraday_request_dates(start_date, end_date)
    today = utils._taipei_today()
    recent_refresh_dates = (
        set(request_dates[-1:])
        if request_dates[-1] >= today - pd.Timedelta(days=3)
        else set()
    )
    summary: list[dict] = []

    upsert_sql = f"""
    INSERT INTO {target_table}
    (
        date, minute, stock_id, open, high, low, close, volume, updated_at
    )
    VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
    ON CONFLICT(date, minute, stock_id) DO UPDATE SET
        open       = EXCLUDED.open,
        high       = EXCLUDED.high,
        low        = EXCLUDED.low,
        close      = EXCLUDED.close,
        volume     = EXCLUDED.volume,
        updated_at = CURRENT_TIMESTAMP
    """

    for sid in ids:
        stored_dates = _stored_intraday_dates(
            target_table,
            "stock_id",
            sid,
            "date",
            request_dates[0],
            request_dates[-1],
        )

        for request_date in request_dates:
            date_str = utils._dstr(request_date)
            if not force_refresh and request_date not in recent_refresh_dates and request_date in stored_dates:
                summary.append({"stock_id": sid, "date": date_str, "status": "cached", "rows": 0})
                continue

            df = api.taiwan_stock_kbar(stock_id=sid, date=date_str)
            if df is None or df.empty:
                summary.append({"stock_id": sid, "date": date_str, "status": "empty", "rows": 0})
                print(f"⚠️ TaiwanStockKBar [{sid}] {date_str} 回传空资料")
                continue

            df = df.copy()
            required = {"date", "minute", "stock_id", "open", "high", "low", "close", "volume"}
            missing = required.difference(df.columns)
            if missing:
                raise ValueError(f"TaiwanStockKBar 缺少栏位：{sorted(missing)}")

            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
            df["minute"] = pd.to_datetime(df["minute"], format="%H:%M:%S", errors="coerce").dt.time
            df["stock_id"] = df["stock_id"].fillna(sid).astype(str)
            for column in ["open", "high", "low", "close", "volume"]:
                df[column] = pd.to_numeric(df[column], errors="coerce")
            df = df.dropna(subset=["date", "minute", "open", "high", "low", "close"])
            df["volume"] = df["volume"].fillna(0).round().astype("int64")
            df = df.drop_duplicates(subset=["date", "minute", "stock_id"], keep="last")

            params = list(
                df[["date", "minute", "stock_id", "open", "high", "low", "close", "volume"]]
                .itertuples(index=False, name=None)
            )
            if params and not db.execute_sql(upsert_sql, params):
                raise RuntimeError(f"写入 {target_table} 失败：{sid} {date_str}")

            summary.append({"stock_id": sid, "date": date_str, "status": "stored", "rows": len(params)})
            print(f"✅ TaiwanStockKBar [{sid}] {date_str}：写入 {len(params)} 笔")

        utils._sync_date_span_from_actual_table(
            target_table=target_table,
            idx_key=sid,
            idx_field="stock_id",
            date_field="date",
        )

    return pd.DataFrame(summary)


# =========================================================
# 8. 期货逐笔成交（TaiwanFuturesTick）
# =========================================================
def _replace_futures_tick_day(
    target_table: str,
    futures_id: str,
    source_date: str,
    df: pd.DataFrame,
) -> int:
    """在同一交易中替换单一 API 日期，避免重复执行产生重复 Tick。"""
    rows = []
    for row in df.itertuples(index=False):
        rows.append(
            (
                source_date,
                row.trade_datetime.to_pydatetime(),
                str(row.futures_id),
                str(row.contract_date),
                float(row.price),
                int(row.volume),
                int(row.source_row_no),
            )
        )

    with db.get_connection() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {target_table} WHERE futures_id = %s AND source_date = %s",
                    (futures_id, source_date),
                )
                with cur.copy(
                    f"""COPY {target_table}
                    (source_date, trade_datetime, futures_id, contract_date, price, volume, source_row_no)
                    FROM STDIN"""
                ) as copy:
                    for row in rows:
                        copy.write_row(row)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return len(rows)


def get_tw_futures_tick(
    futures_id: Union[str, list[str]],
    start_date: datetime,
    end_date: datetime,
    *,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """下载 TaiwanFuturesTick 并写入 fm_taiwan_futures_tick。

    完整保留 FinMind 的双边 volume，不在原始层除以 2。
    source_row_no 只用于保留同秒、同价、同量的重复成交及重复执行去重。
    回传值是每日同步摘要，不会把大量 Tick 重新读取到 Python。
    """
    print(f"--- run finMind.get_tw_futures_tick--[{futures_id}]")
    target_table = "fm_taiwan_futures_tick"
    ids = [str(value).strip() for value in utils._ensure_list(futures_id)]
    # 夜盤跨越午夜；除了交易日本身，也必須下載隔日 00:00～05:00 的日曆檔。
    request_dates = _futures_tick_request_dates(start_date, end_date)
    if not request_dates:
        return pd.DataFrame(columns=["futures_id", "date", "status", "rows"])
    today = utils._taipei_today()
    # 週一的尾端請求日通常是 [週五, 週六, 週一]。三個都必須
    # 重抓，否則只抓最後兩個會漏掉週五 15:00～24:00。
    recent_refresh_dates = (
        set(request_dates[-3:])
        if request_dates[-1] >= today - pd.Timedelta(days=4)
        else set()
    )
    summary: list[dict] = []

    for fid in ids:
        stored_dates = _stored_intraday_dates(
            target_table,
            "futures_id",
            fid,
            "source_date",
            request_dates[0],
            request_dates[-1],
        )

        for request_date in request_dates:
            date_str = utils._dstr(request_date)
            if not force_refresh and request_date not in recent_refresh_dates and request_date in stored_dates:
                summary.append({"futures_id": fid, "date": date_str, "status": "cached", "rows": 0})
                continue

            df = api.taiwan_futures_tick(futures_id=fid, date=date_str)
            if df is None or df.empty:
                summary.append({"futures_id": fid, "date": date_str, "status": "empty", "rows": 0})
                print(f"⚠️ TaiwanFuturesTick [{fid}] {date_str} 回传空资料")
                continue

            df = df.copy()
            required = {"contract_date", "date", "futures_id", "price", "volume"}
            missing = required.difference(df.columns)
            if missing:
                raise ValueError(f"TaiwanFuturesTick 缺少栏位：{sorted(missing)}")

            df["trade_datetime"] = pd.to_datetime(df["date"], errors="coerce")
            df["futures_id"] = df["futures_id"].fillna(fid).astype(str)
            df["price"] = pd.to_numeric(df["price"], errors="coerce")
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
            df = df.dropna(subset=["trade_datetime", "contract_date", "price", "volume"])
            df["contract_date"] = df["contract_date"].astype(str)
            df["volume"] = df["volume"].round().astype("int64")
            df = df.reset_index(drop=True)
            df["source_row_no"] = df.index.astype("int64") + 1
            df = df[["trade_datetime", "futures_id", "contract_date", "price", "volume", "source_row_no"]]

            written = _replace_futures_tick_day(target_table, fid, date_str, df)
            summary.append({"futures_id": fid, "date": date_str, "status": "stored", "rows": written})
            print(f"✅ TaiwanFuturesTick [{fid}] {date_str}：写入 {written} 笔")

        utils._sync_date_span_from_actual_table(
            target_table=target_table,
            idx_key=fid,
            idx_field="futures_id",
            date_field="source_date",
        )

    return pd.DataFrame(summary)


# =========================================================
# 9. 台指期 1 分 K（由 TaiwanFuturesTick 汇总）
# =========================================================
def build_tw_futures_kbar_1m(
    futures_id: str,
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    """将已入库的期货 Tick 汇总成各契约一分钟 K。

    规则：
    - 00:00～05:00：source_date 当日夜盘，交易日为 source_date。
    - 08:45～13:45：source_date 当日日盘，交易日为 source_date。
    - 15:00～24:00：次一交易日夜盘，交易日由交易日表向后寻找。
    - 只汇总 contract_date 为 YYYYMM 的单一契约，排除价差／组合契约。
    - raw_volume 保存 FinMind 双边量；volume 除以 2 转为单边口数。
    - 指定 source_date 区间采用整段重建，确保 Tick 重抓后 K 棒同步修正。
    """
    fid = str(futures_id).strip()
    req_s = utils._to_norm_ts(start_date)
    req_e = min(utils._to_norm_ts(end_date), utils._taipei_today())
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date，且不可全部位於未來")

    source_table = "fm_taiwan_futures_tick"
    target_table = "fm_taiwan_futures_kbar_1m"
    start_str = utils._dstr(req_s)
    end_str = utils._dstr(req_e)
    print(f"--- run finMind.build_tw_futures_kbar_1m--[{fid}] {start_str}~{end_str}")

    insert_sql = f"""
    WITH source_dates AS (
        SELECT DISTINCT source_date
        FROM {source_table}
        WHERE futures_id = %s
          AND source_date >= %s
          AND source_date <= %s
    ),
    next_trading_dates AS (
        SELECT
            sd.source_date,
            (
                SELECT MIN(td.date)
                FROM fm_taiwan_stock_trading_date td
                WHERE td.date > sd.source_date
            ) AS next_trading_date,
            (
                SELECT MIN(td.date)
                FROM fm_taiwan_stock_trading_date td
                WHERE td.date >= sd.source_date
            ) AS current_or_next_trading_date
        FROM source_dates sd
    ),
    classified AS (
        SELECT
            t.source_date,
            t.trade_datetime,
            date_trunc('minute', t.trade_datetime) AS bar_datetime,
            t.futures_id,
            t.contract_date,
            t.price,
            t.volume,
            t.source_row_no,
            CASE
                WHEN t.trade_datetime::time >= TIME '15:00:00' THEN 'after_market'
                WHEN t.trade_datetime::time <= TIME '05:00:00' THEN 'after_market'
                WHEN t.trade_datetime::time BETWEEN TIME '08:45:00' AND TIME '13:45:59.999999'
                    THEN 'position'
                ELSE NULL
            END AS trading_session,
            CASE
                WHEN t.trade_datetime::time >= TIME '15:00:00'
                    THEN ntd.next_trading_date
                WHEN t.trade_datetime::time <= TIME '05:00:00'
                    THEN ntd.current_or_next_trading_date
                ELSE t.source_date
            END AS trading_date
        FROM {source_table} t
        JOIN next_trading_dates ntd
          ON ntd.source_date = t.source_date
        WHERE t.futures_id = %s
          AND t.source_date >= %s
          AND t.source_date <= %s
          AND t.contract_date ~ '^[0-9]{{6}}$'
    ),
    valid_ticks AS (
        SELECT *
        FROM classified
        WHERE trading_session IS NOT NULL
          AND trading_date IS NOT NULL
    )
    INSERT INTO {target_table}
    (
        source_date, trading_date, bar_datetime,
        futures_id, contract_date, trading_session,
        open, high, low, close,
        volume, raw_volume, tick_count, updated_at
    )
    SELECT
        source_date,
        trading_date,
        bar_datetime,
        futures_id,
        contract_date,
        trading_session,
        (array_agg(price ORDER BY trade_datetime, source_row_no))[1] AS open,
        MAX(price) AS high,
        MIN(price) AS low,
        (array_agg(price ORDER BY trade_datetime DESC, source_row_no DESC))[1] AS close,
        (SUM(volume) / 2)::bigint AS volume,
        SUM(volume)::bigint AS raw_volume,
        COUNT(*)::bigint AS tick_count,
        CURRENT_TIMESTAMP
    FROM valid_ticks
    GROUP BY
        source_date, trading_date, bar_datetime,
        futures_id, contract_date, trading_session
    ON CONFLICT(futures_id, contract_date, bar_datetime) DO UPDATE SET
        source_date     = EXCLUDED.source_date,
        trading_date    = EXCLUDED.trading_date,
        trading_session = EXCLUDED.trading_session,
        open            = EXCLUDED.open,
        high            = EXCLUDED.high,
        low             = EXCLUDED.low,
        close           = EXCLUDED.close,
        volume          = EXCLUDED.volume,
        raw_volume      = EXCLUDED.raw_volume,
        tick_count      = EXCLUDED.tick_count,
        updated_at      = CURRENT_TIMESTAMP
    """

    with db.get_connection() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    DELETE FROM {target_table}
                    WHERE futures_id = %s
                      AND source_date >= %s
                      AND source_date <= %s
                    """,
                    (fid, start_str, end_str),
                )
                deleted_rows = cur.rowcount
                cur.execute(
                    insert_sql,
                    (fid, start_str, end_str, fid, start_str, end_str),
                )
                inserted_rows = cur.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    utils._sync_date_span_from_actual_table(
        target_table=target_table,
        idx_key=fid,
        idx_field="futures_id",
        date_field="source_date",
    )

    result = db.query_to_df(
        f"""
        SELECT
            futures_id,
            MIN(source_date) AS source_min_date,
            MAX(source_date) AS source_max_date,
            MIN(trading_date) AS trading_min_date,
            MAX(trading_date) AS trading_max_date,
            COUNT(*) AS bar_count,
            COUNT(DISTINCT contract_date) AS contract_count
        FROM {target_table}
        WHERE futures_id = ?
          AND source_date >= ?
          AND source_date <= ?
        GROUP BY futures_id
        """,
        (fid, start_str, end_str),
    )
    if result.empty:
        result = pd.DataFrame([{
            "futures_id": fid,
            "source_min_date": None,
            "source_max_date": None,
            "trading_min_date": None,
            "trading_max_date": None,
            "bar_count": 0,
            "contract_count": 0,
        }])
    result["deleted_rows"] = deleted_rows
    result["inserted_rows"] = inserted_rows
    print(f"✅ {fid} 一分钟 K 重建完成：删除 {deleted_rows} 笔，写入 {inserted_rows} 笔")
    return result
    

    

# python -m module.finMind
if __name__ == "__main__":
    sDt = datetime(2007,1,1)
    eDt = datetime(2008,12,31)
    # eDt = datetime.now()
    
    # df = get_tw_stock_trading_dates()

    # 加权指数一分钟 K：
    print(get_tw_stock_kbar("TAIEX", sDt, eDt))

    # 台指期逐笔成交：
    print(get_tw_futures_tick("TX", sDt, eDt))

    # 将已储存的台指期 Tick 转成各契约一分钟 K：
    print(build_tw_futures_kbar_1m("TX", sDt, eDt))
