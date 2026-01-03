import sys, os
sys.path.append(os.path.dirname(__file__))

import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from common.db import query_to_df, execute_sql, get_connection, query_single_value
import twse_api
from common import tools
import data_provider;

from datetime import datetime
from typing import List
import requests
from common import db, utils
import finMind

# sys.path.append(os.path.dirname(__file__))
# sys.path.append(os.path.dirname(os.path.dirname(__file__))) 

# 注意股公告
def get_notice(sDt: datetime, eDt: datetime):
    return data_provider.get_notice("twse", sDt, eDt)

# 融資餘額
def get_margin_trading(sDt: datetime, eDt: datetime):
    if (sDt > eDt):
        return None

    if eDt > datetime.today():
        eDt = datetime.today()

    table = "twse_marginTrading_miMargn"
    print("[sDt]=" , sDt, "[eDt]=", eDt)

    ### 先確認庫有資料
    sql = f"SELECT count(*) FROM {table}"
    dataCnt = query_single_value(sql)
    if (dataCnt < 1):
        maxDt = sDt - relativedelta(days=1)
        minDt = maxDt - relativedelta(days=1)        
    else:
        ### 先確認庫資料的上下界
        sql = f"SELECT min(日期), MAX(日期) FROM {table}"
        df_check = query_to_df(sql)
        minDt = datetime.strptime(df_check["min(日期)"].iloc[0], "%Y%m%d")
        maxDt = datetime.strptime(df_check["MAX(日期)"].iloc[0], "%Y%m%d")
        print(minDt, maxDt)

    ### 1.資料完全落在庫的範圍，直接搜庫
    if (tools._is_fully_in_range(sDt, eDt, minDt, maxDt)):
        print("*** 直接從庫提取資料")
        sql = f"SELECT * FROM {table}"
        sql += f" WHERE 日期 between '{sDt.strftime('%Y%m%d')}' AND '{eDt.strftime('%Y%m%d')}'"
        df = query_to_df(sql)
        return df
    
    ### 2.資料完全落在庫的範圍之外，fetch API
    if (tools._is_no_overlap(sDt, eDt, minDt, maxDt)):
        print("*** 從API提取資料")
        raw_data = twse_api.fetch_margin_trading_range(sDt, eDt)
        if raw_data is None:
            print("fetch_margin_trading 回傳 None")
            return None
        data = raw_data["data"]
        print(data)

        ### 存到db裡
        values = []
        for r in data:
            try:
                values.append((
                    r[0].strip(),
                    r[1].strip(),
                    int(r[2].replace(',', '')),
                    int(r[3].replace(',', '')),
                    int(r[4].replace(',', '')),
                    int(r[5].replace(',', '')),
                    int(r[6].replace(',', ''))
                ))
            except:
                continue

        sql = f"""
        INSERT OR REPLACE INTO {table}
        (日期, 項目, 買進, 賣出, 現金_券_償還, 前日餘額, 今日餘額)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        if not execute_sql(sql, values):
            print("存庫失敗！")
        df = pd.DataFrame(raw_data.get("data", []), columns=raw_data.get("fields", []))
        return df
    
    ### 3.重疊的部分取庫，超出的部分用API，最後merge
    ## 找出重疊部分(From 庫)
    overlap_star, overlap_end = tools._overlap_period(sDt, eDt, minDt, maxDt)
    if overlap_star is not None:
        df_exist = get_margin_trading(overlap_star, overlap_end)

    ## 找出需要獲取的部分
    if (sDt < minDt):
        df_new_left = get_margin_trading(sDt, minDt - relativedelta(days=1))
    if (maxDt < eDt):
        df_new_right = get_margin_trading(maxDt + relativedelta(days=1), eDt)
    
    ## 合併 DataFrames
    dfs = []
    if 'df_exist' in locals() and isinstance(df_exist, pd.DataFrame):
        dfs.append(df_exist)
    if 'df_new_left' in locals() and isinstance(df_new_left, pd.DataFrame):
        dfs.append(df_new_left)
    if 'df_new_right' in locals() and isinstance(df_new_right, pd.DataFrame):
        dfs.append(df_new_right)

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
    else:
        df = None  # 如果都不存在，回傳空 DataFrame
    return df

# 補闕漏的 融資餘額 資料
def repair_margin_trading_gaps(
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
    """
    修補 twse_marginTrading_miMargn 中間缺漏的交易日資料
    - 若未指定 start/end，則以 DB 中 min/max 日期為範圍
    - 僅針對缺口呼叫 API
    """
    table = "twse_marginTrading_miMargn"
    span_table = "date_span"
    target_table = table
    idx_key = None  # 依你要求，融資不需要 idx_key

    # === 1) 決定修補範圍 ===
    if start_date is None or end_date is None:
        row = db.query_to_df(f"""
            SELECT MIN(日期) AS min_d, MAX(日期) AS max_d
            FROM {table}
        """)
        if row.empty or row.iloc[0]["min_d"] is None:
            print("⚠ DB 無任何融資資料，無法自動修補")
            return
        s = datetime.strptime(row.iloc[0]["min_d"], "%Y%m%d")
        e = datetime.strptime(row.iloc[0]["max_d"], "%Y%m%d")
    else:
        s = pd.Timestamp(start_date).normalize().to_pydatetime()
        e = pd.Timestamp(end_date).normalize().to_pydatetime()

    print(f"🔍 修補範圍: {s.date()} ~ {e.date()}")

    # === 2) 取得交易日曆 ===
    df_trade = finMind.getTwStockTradingDates()
    trade_days = set(
        pd.to_datetime(df_trade["date"]).dt.strftime("%Y%m%d")
    )

    # === 3) 取 DB 已有日期 ===
    df_exist = db.query_to_df(
        f"""
        SELECT DISTINCT 日期
        FROM {table}
        WHERE 日期 BETWEEN ? AND ?
        """,
        (s.strftime("%Y%m%d"), e.strftime("%Y%m%d")),
    )
    exist_days = set(df_exist["日期"]) if not df_exist.empty else set()

    # === 4) 找缺口交易日 ===
    need_days = sorted([
        d for d in trade_days
        if s.strftime("%Y%m%d") <= d <= e.strftime("%Y%m%d")
        and d not in exist_days
    ])

    if not need_days:
        print("✅ 無缺漏交易日")
        return

    # === 5) 將缺日合併為連續區段（最少 API call） ===
    def to_ranges(days: list[str]):
        ranges = []
        start = prev = datetime.strptime(days[0], "%Y%m%d")
        for d in days[1:]:
            cur = datetime.strptime(d, "%Y%m%d")
            if (cur - prev).days > 1:
                ranges.append((start, prev))
                start = cur
            prev = cur
        ranges.append((start, prev))
        return ranges

    fetch_ranges = to_ranges(need_days)

    print(f"🚑 發現 {len(fetch_ranges)} 段缺口，開始補資料")

    # === 6) 補資料 ===
    for fs, fe in fetch_ranges:
        print(f"📡 補 {fs.date()} ~ {fe.date()}")
        raw = twse_api.fetch_margin_trading_range(fs, fe)
        if raw is None or not raw.get("data"):
            print("⚠ API 無回傳資料")
            continue

        values = []
        for r in raw["data"]:
            try:
                values.append((
                    r[0].strip(),  # 日期 YYYYMMDD
                    r[1].strip(),  # 項目
                    int(r[2].replace(",", "")),
                    int(r[3].replace(",", "")),
                    int(r[4].replace(",", "")),
                    int(r[5].replace(",", "")),
                    int(r[6].replace(",", "")),
                ))
            except Exception:
                continue

        db.execute_sql(f"""
        INSERT OR REPLACE INTO {table}
          (日期, 項目, 買進, 賣出, 現金_券_償還, 前日餘額, 今日餘額)
        VALUES (?,?,?,?,?,?,?)
        """, values)

    # === 7) 更新 date_span ===
    span = db.query_to_df("""
        SELECT start_date, end_date
        FROM date_span
        WHERE target_table = ? AND idx_key = ?
    """, (target_table, idx_key))

    new_s = s.strftime("%Y%m%d")
    new_e = e.strftime("%Y%m%d")

    if span.empty:
        db.execute_sql("""
        INSERT INTO date_span (target_table, idx_key, start_date, end_date)
        VALUES (?, ?, ?, ?)
        """, (target_table, idx_key, new_s, new_e))
    else:
        cur_s = span.loc[0, "start_date"]
        cur_e = span.loc[0, "end_date"]
        db.execute_sql("""
        UPDATE date_span
        SET start_date = ?, end_date = ?, updated_at = strftime('%s','now')
        WHERE target_table = ? AND idx_key = ?
        """, (
            min(cur_s, new_s),
            max(cur_e, new_e),
            target_table,
            idx_key
        ))

    print("✅ 缺漏修補完成，date_span 已更新")


# =========================
# 共用小工具
# =========================
_TWSE_HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def _to_int(s: str) -> int:
    if s is None:
        return 0
    s = str(s).strip().replace(",", "")
    if s in ("", "-", "--"):
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def _to_float(s: str) -> float:
    if s is None:
        return 0.0
    s = str(s).strip().replace(",", "")
    if s in ("", "-", "--"):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _iter_month_starts(start: pd.Timestamp, end: pd.Timestamp) -> List[pd.Timestamp]:
    """
    給定 [start, end]，回傳所有「每月一號」的 Timestamp（含頭尾月）。
    """
    cur = start.replace(day=1)
    cur = pd.Timestamp(cur.year, cur.month, 1)

    res: List[pd.Timestamp] = []
    while cur <= end:
        res.append(cur)
        if cur.month == 12:
            cur = pd.Timestamp(cur.year + 1, 1, 1)
        else:
            cur = pd.Timestamp(cur.year, cur.month + 1, 1)
    return res


# =========================
# 1) FMTQIK：加權市場成交量 / 成交值 / 交易筆數 / 指數收盤
#    table: twse_exchangeReport_fmtqik
# =========================
def get_twse_exchangeReport_fmtqik(
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    """
    來源： https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date=YYYYMMDD

    資料庫快取表：twse_exchangeReport_fmtqik
      - date       TEXT   (ROC 原始日期, e.g. '114/12/24')
      - date_ad    TEXT   (西元 yyyy-mm-dd)
      - date_ts    INTEGER (該日 00:00:00 的 unix ts)
      - market_volume  INTEGER
      - market_money   INTEGER
      - trade_count    INTEGER
      - taiex_close    REAL
      - taiex_spread   REAL
      - created_at     INTEGER (DEFAULT strftime('%s','now'))
      - UNIQUE(date_ad)

    span 記憶表：stock_span
      - target_table = 'twse_exchangeReport_fmtqik'
      - stock_id     = 'MARKET'
      - start_date / end_date 為 date_ad
    """
    print(f"--- run twse.get_twse_exchangeReport_fmtqik ---")

    target_table = "twse_exchangeReport_fmtqik"
    span_sid = "MARKET"  # 當作 stock_id 用在 date_span

    req_s = pd.Timestamp(start_date).normalize()
    req_e = pd.Timestamp(end_date).normalize()
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")

    def dstr(t: pd.Timestamp) -> str:
        return t.strftime("%Y-%m-%d")

    # ---- 1) 查 span ----
    span_row = db.query_to_df(
        """
        SELECT start_date, end_date
        FROM date_span
        WHERE target_table = ? AND idx_key = ?
        """,
        (target_table, span_sid),
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

    # ---- 2) 補資料：依「缺的日期區間」→ 拆成月份呼叫 API ----
    insert_sql = """
    INSERT OR IGNORE INTO twse_exchangeReport_fmtqik
      (date, date_ad, date_ts,
       market_volume, market_money, trade_count,
       taiex_close, taiex_spread)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    for fs, fe in fetch_ranges:
        if fs > fe:
            continue

        # 該缺口涵蓋的所有月份
        for m_start in _iter_month_starts(fs, fe):
            ymd = m_start.strftime("%Y%m01")
            url = "https://www.twse.com.tw/exchangeReport/FMTQIK"
            params = {"response": "json", "date": ymd}

            try:
                res = requests.get(url, params=params, headers=_TWSE_HEADERS, timeout=10)
                res.raise_for_status()
                body = res.json()
            except Exception as e:
                print(f"[TWSE FMTQIK] request error ({ymd}): {e}")
                continue

            if body.get("stat") != "OK":
                # 沒資料就跳過該月
                continue

            data_rows = body.get("data", [])
            if not data_rows:
                continue

            insert_params = []
            for row in data_rows:
                # row[0]: 民國日期 '114/12/24'
                roc_date = str(row[0]).strip()
                ts = utils.roc_to_unix(roc_date)
                if ts is None:
                    continue
                dt_ad = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")

                # 不在目前缺口的日期，仍然可以收進來，因為我們是把 span 擴大
                market_volume = _to_int(row[1])   # 成交股數
                market_money = _to_int(row[2])    # 成交金額
                trade_count = _to_int(row[3])     # 交易筆數
                taiex_close = _to_float(row[4])   # 收盤指數
                taiex_spread = _to_float(row[5])  # 漲跌點數

                insert_params.append((
                    roc_date,
                    dt_ad,
                    ts,
                    market_volume,
                    market_money,
                    trade_count,
                    taiex_close,
                    taiex_spread,
                ))

            if insert_params:
                db.execute_sql(insert_sql, insert_params)

    # ---- 3) 依「實際有的資料」重新決定 span ----
    span_cov = db.query_to_df(
        f"""
        SELECT MIN(date_ad) AS min_d, MAX(date_ad) AS max_d
        FROM {target_table}
        """
    )

    if not span_cov.empty and span_cov.loc[0, "max_d"] is not None:
        real_s = pd.Timestamp(span_cov.loc[0, "min_d"]).normalize()
        real_e = pd.Timestamp(span_cov.loc[0, "max_d"]).normalize()
        # 避免把 span 設到超過這次要求的 end_date（API 可能還沒開出未來的日子）
        real_e = min(real_e, req_e)
    else:
        # 沒資料就乾脆設成這次要求範圍
        real_s, real_e = req_s, req_e

    db.execute_sql(
        """
        INSERT INTO date_span (target_table, idx_key, start_date, end_date, updated_at)
        VALUES (?, ?, ?, ?, strftime('%s','now'))
        ON CONFLICT(target_table, idx_key) DO UPDATE SET
          start_date = excluded.start_date,
          end_date   = excluded.end_date,
          updated_at = strftime('%s','now')
        """,
        (target_table, span_sid, dstr(real_s), dstr(real_e)),
    )

    # ---- 4) 一律從 DB 回傳 ----
    df = db.query_to_df(
        """
        SELECT
          date,
          date_ad,
          date_ts,
          market_volume,
          market_money,
          trade_count,
          taiex_close,
          taiex_spread
        FROM twse_exchangeReport_fmtqik
        WHERE date_ad >= ?
          AND date_ad <= ?
        ORDER BY date_ad
        """,
        (dstr(req_s), dstr(req_e)),
    )
    return df


# =========================
# 2) MI_5MINS_HIST：加權指數日 OHLC (由 5 分鐘資料整理, 官方每日盤後)
#    table: twse_indicesReport_mi_5mins_hist
# =========================
def get_twse_indicesReport_mi_5mins_hist(
    start_date: datetime,
    end_date: datetime,
) -> pd.DataFrame:
    """
    來源： https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date=YYYYMMDD

    資料庫快取表：twse_indicesReport_mi_5mins_hist
      - date        TEXT  (ROC 原始日期, e.g. '114/12/24')
      - date_ad     TEXT  (西元 yyyy-mm-dd)
      - date_ts     INTEGER
      - open_index  REAL
      - high_index  REAL
      - low_index   REAL
      - close_index REAL
      - created_at  INTEGER DEFAULT strftime('%s','now')
      - UNIQUE(date_ad)

    span 記憶表：stock_span
      - target_table = 'twse_indicesReport_mi_5mins_hist'
      - stock_id     = 'TAIEX'
    """
    print(f"--- run twse.get_twse_indicesReport_mi_5mins_hist ---")

    target_table = "twse_indicesReport_mi_5mins_hist"
    span_sid = "TAIEX"

    req_s = pd.Timestamp(start_date).normalize()
    req_e = pd.Timestamp(end_date).normalize()
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")

    def dstr(t: pd.Timestamp) -> str:
        return t.strftime("%Y-%m-%d")

    # ---- 1) 查 span ----
    span_row = db.query_to_df(
        """
        SELECT start_date, end_date
        FROM date_span
        WHERE target_table = ? AND idx_key = ?
        """,
        (target_table, span_sid),
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

    # ---- 2) 補資料：依缺口的月份呼叫 API ----
    insert_sql = """
    INSERT OR IGNORE INTO twse_indicesReport_mi_5mins_hist
      (date, date_ad, date_ts,
       open_index, high_index, low_index, close_index)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    for fs, fe in fetch_ranges:
        if fs > fe:
            continue

        for m_start in _iter_month_starts(fs, fe):
            ymd = m_start.strftime("%Y%m01")
            url = "https://www.twse.com.tw/indicesReport/MI_5MINS_HIST"
            params = {"response": "json", "date": ymd}

            try:
                res = requests.get(url, params=params, headers=_TWSE_HEADERS, timeout=10)
                res.raise_for_status()
                body = res.json()
            except Exception as e:
                print(f"[TWSE MI_5MINS_HIST] request error ({ymd}): {e}")
                continue

            if body.get("stat") != "OK":
                continue

            data_rows = body.get("data", [])
            if not data_rows:
                continue

            insert_params = []
            for record in data_rows:
                # record[0]: 民國日期 '108/01/02'
                roc_date = str(record[0]).strip()
                ts = utils.roc_to_unix(roc_date)
                if ts is None:
                    continue
                dt_ad = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")

                open_price = _to_float(record[1])
                high_price = _to_float(record[2])
                low_price = _to_float(record[3])
                close_price = _to_float(record[4])

                insert_params.append((
                    roc_date,
                    dt_ad,
                    ts,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                ))

            if insert_params:
                db.execute_sql(insert_sql, insert_params)

    # ---- 3) 依「實際有的資料」重新決定 span ----
    span_cov = db.query_to_df(
        f"""
        SELECT MIN(date_ad) AS min_d, MAX(date_ad) AS max_d
        FROM {target_table}
        """
    )

    if not span_cov.empty and span_cov.loc[0, "max_d"] is not None:
        real_s = pd.Timestamp(span_cov.loc[0, "min_d"]).normalize()
        real_e = pd.Timestamp(span_cov.loc[0, "max_d"]).normalize()
        # 避免把 span 設到超過這次要求的 end_date（API 可能還沒開出未來的日子）
        real_e = min(real_e, req_e)
    else:
        # 沒資料就乾脆設成這次要求範圍
        real_s, real_e = req_s, req_e

    db.execute_sql(
        """
        INSERT INTO date_span (target_table, idx_key, start_date, end_date, updated_at)
        VALUES (?, ?, ?, ?, strftime('%s','now'))
        ON CONFLICT(target_table, idx_key) DO UPDATE SET
          start_date = excluded.start_date,
          end_date   = excluded.end_date,
          updated_at = strftime('%s','now')
        """,
        (target_table, span_sid, dstr(real_s), dstr(real_e)),
    )

    # ---- 4) 一律從 DB 回傳 ----
    df = db.query_to_df(
        """
        SELECT
          date,
          date_ad,
          date_ts,
          open_index,
          high_index,
          low_index,
          close_index
        FROM twse_indicesReport_mi_5mins_hist
        WHERE date_ad >= ?
          AND date_ad <= ?
        ORDER BY date_ad
        """,
        (dstr(req_s), dstr(req_e)),
    )
    return df


# python -m module.twse
if __name__ == "__main__":
    repair_margin_trading_gaps()
