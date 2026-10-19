
import re
import time
import requests
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple, List
from common import db  # 你原本的 DB 模組

# ============================================================
# helpers
# ============================================================

def _dstr(ts: pd.Timestamp) -> str:
    return ts.strftime("%Y-%m-%d")


def _to_unix_ts(date_yyyy_mm_dd: str) -> int:
    # date string -> unix seconds (00:00:00 local)
    dt = pd.Timestamp(date_yyyy_mm_dd).to_pydatetime()
    return int(time.mktime(dt.timetuple()))


def _clean_float(x) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.upper() in {"N/A", "NA", "NULL"} or s in {"-----", "--", "-"}:
        return None
    s = s.replace(",", "")
    # 有些是 "18.90     " 這種
    try:
        return float(s)
    except Exception:
        return None


def _clean_int(x) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.upper() in {"N/A", "NA", "NULL"} or s in {"-----", "--", "-"}:
        return None
    s = s.replace(",", "")
    try:
        return int(float(s))
    except Exception:
        return None


def _roc_to_ad_yyyy_mm_dd(s: str) -> Optional[str]:
    """
    支援：
    - "115/01/15"
    - "100.01.03"
    - "114/12/31"
    - "20110103" / "2026/01/15" / "2026-01-15"（若本來就是西元也盡量吃）
    回傳 "YYYY-MM-DD"
    """
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None

    # 先抓純數字 YYYYMMDD
    m = re.fullmatch(r"(\d{8})", s)
    if m:
        y = int(s[:4])
        mm = int(s[4:6])
        dd = int(s[6:8])
        return f"{y:04d}-{mm:02d}-{dd:02d}"

    # 支援 YYYY/MM/DD 或 YYYY-MM-DD
    m = re.fullmatch(r"(\d{4})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})", s)
    if m:
        y = int(m.group(1))
        mm = int(m.group(2))
        dd = int(m.group(3))
        # 若 y 很小(<=1911) 才可能是民國？一般不會，這裡當西元
        return f"{y:04d}-{mm:02d}-{dd:02d}"

    # 民國 "115/01/15" 或 "100.01.03"
    m = re.fullmatch(r"(\d{2,3})[\/\.](\d{1,2})[\/\.](\d{1,2})", s)
    if m:
        roc_y = int(m.group(1))
        y = roc_y + 1911
        mm = int(m.group(2))
        dd = int(m.group(3))
        return f"{y:04d}-{mm:02d}-{dd:02d}"

    return None


def _parse_range_roc(s: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """
    解析 "115/01/16～115/01/29" 或 "115/01/16~115/01/29" 或 "115/01/16～115/01/29"
    回傳 (start_yyyy_mm_dd, end_yyyy_mm_dd, total_days_inclusive_or_none)
    total_days 以「日曆日含頭尾」粗算（不是交易日）
    """
    if s is None:
        return (None, None, None)
    t = str(s).strip()
    if not t:
        return (None, None, None)

    # 各種分隔符
    t = t.replace("～", "~").replace("—", "-").replace("–", "-")
    parts = t.split("~")
    if len(parts) != 2:
        return (None, None, None)

    s1 = _roc_to_ad_yyyy_mm_dd(parts[0].strip())
    s2 = _roc_to_ad_yyyy_mm_dd(parts[1].strip())
    if not s1 or not s2:
        return (s1, s2, None)

    try:
        d1 = pd.Timestamp(s1)
        d2 = pd.Timestamp(s2)
        total = int((d2 - d1).days) + 1
    except Exception:
        total = None
    return (s1, s2, total)


def _get_span(target_table: str) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    span_row = db.query_to_df(
        """
        SELECT start_date, end_date
        FROM date_span
        WHERE target_table = ? AND idx_key = ?
        """,
        (target_table, "date"),
    )
    if span_row is None or span_row.empty:
        return (None, None)
    mem_s = pd.Timestamp(span_row.loc[0, "start_date"]).normalize()
    mem_e = pd.Timestamp(span_row.loc[0, "end_date"]).normalize()
    return (mem_s, mem_e)


def _calc_fetch_ranges(
    req_s: pd.Timestamp,
    req_e: pd.Timestamp,
    mem_s: Optional[pd.Timestamp],
    mem_e: Optional[pd.Timestamp],
) -> Tuple[List[Tuple[pd.Timestamp, pd.Timestamp]], pd.Timestamp, pd.Timestamp]:
    fetch_ranges: List[Tuple[pd.Timestamp, pd.Timestamp]] = []

    if mem_s is None or mem_e is None:
        return [(req_s, req_e)], req_s, req_e

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

    return fetch_ranges, new_s, new_e


def _update_span(target_table: str, new_s: pd.Timestamp, new_e: pd.Timestamp) -> None:
    db.execute_sql(
        """
        INSERT INTO date_span (target_table, idx_key, start_date, end_date, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(target_table, idx_key) DO UPDATE SET
          start_date = excluded.start_date,
          end_date   = excluded.end_date,
          updated_at = CURRENT_TIMESTAMP
        """,
        (target_table, "date", _dstr(new_s), _dstr(new_e)),
    )


# ============================================================
# 3) TPEX attention (上櫃公布注意有價證券)
# ============================================================

def get_tpex_bulletin_attention(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    對應：
      POST https://www.tpex.org.tw/www/zh-tw/bulletin/attention
      payload: startDate=YYYY/MM/DD&endDate=YYYY/MM/DD&...&response=json

    入庫表：tpex_bulletin_attention
    快取：date_span(target_table='tpex_bulletin_attention', idx_key='date')
    """
    target_table = "tpex_bulletin_attention"
    req_s = pd.Timestamp(start_date).normalize()
    req_e = pd.Timestamp(end_date).normalize()
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")

    mem_s, mem_e = _get_span(target_table)
    fetch_ranges, new_s, new_e = _calc_fetch_ranges(req_s, req_e, mem_s, mem_e)

    url = "https://www.tpex.org.tw/www/zh-tw/bulletin/attention"

    delete_sql = f"DELETE FROM {target_table} WHERE 公告日期 >= ? AND 公告日期 <= ?"

    insert_sql = f"""
    INSERT INTO {target_table}
      (證券代號, 證券名稱, 累計, 注意交易資訊, 公告日期, 收盤價, 本益比, link, created_at, 公告日期_ts)
    VALUES (?,?,?,?,?,?,?,?,?,?)
    """

    for fs, fe in fetch_ranges:
        if fs > fe:
            continue

        payload = {
            "startDate": fs.strftime("%Y/%m/%d"),
            "endDate": fe.strftime("%Y/%m/%d"),
            "code": "",
            "cate": "",
            "type": "all",
            "order": "date",
            "id": "",
            "response": "json",
        }

        r = requests.post(url, data=payload, timeout=30)
        r.raise_for_status()
        js = r.json()

        tables = js.get("tables") or []
        if not tables:
            continue

        data = tables[0].get("data") or []
        if not data:
            continue

        rows = []
        for item in data:
            # fields:
            # ["編號","證券代號","證券名稱","累計","注意交易資訊","公告日期","收盤價","本益比","link"]
            sid = str(item[1]).strip()
            name = str(item[2]).strip()
            acc = _clean_int(item[3])
            info = str(item[4]).strip()
            pub_roc = str(item[5]).strip()  # 115/01/15
            pub_ad = _roc_to_ad_yyyy_mm_dd(pub_roc)
            close_p = _clean_float(item[6])
            pe = _clean_float(item[7])
            link = str(item[8]).strip() if item[8] is not None else None

            if not pub_ad:
                continue

            rows.append((
                sid, name, acc, info, pub_ad, close_p, pe, link,
                pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                _to_unix_ts(pub_ad),
            ))

        if rows:
            db.execute_sql(delete_sql, (_dstr(fs), _dstr(fe)))
            db.execute_sql(insert_sql, rows)

    _update_span(target_table, new_s, new_e)

    df = db.query_to_df(
        f"""
        SELECT id, 證券代號, 證券名稱, 累計, 注意交易資訊, 公告日期, 收盤價, 本益比, link, created_at, 公告日期_ts
        FROM {target_table}
        WHERE 公告日期 >= ? AND 公告日期 <= ?
        ORDER BY 公告日期, 證券代號
        """,
        (_dstr(req_s), _dstr(req_e)),
    )
    return df if df is not None else pd.DataFrame()


# ============================================================
# 4) TPEX disposal (上櫃公布處置有價證券資訊)
# ============================================================

def get_tpex_bulletin_disposal(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    對應：
      POST https://www.tpex.org.tw/www/zh-tw/bulletin/disposal
      payload: startDate=YYYY/MM/DD&endDate=YYYY/MM/DD&...&response=json

    入庫表：tpex_bulletin_disposal（UNIQUE(證券代號, 公布日期)）
    快取：date_span(target_table='tpex_bulletin_disposal', idx_key='date')
    """
    target_table = "tpex_bulletin_disposal"
    req_s = pd.Timestamp(start_date).normalize()
    req_e = pd.Timestamp(end_date).normalize()
    if req_s > req_e:
        raise ValueError("start_date 不可大於 end_date")

    mem_s, mem_e = _get_span(target_table)
    fetch_ranges, new_s, new_e = _calc_fetch_ranges(req_s, req_e, mem_s, mem_e)

    url = "https://www.tpex.org.tw/www/zh-tw/bulletin/disposal"

    upsert_sql = f"""
    INSERT INTO {target_table}
      (公布日期, 證券代號, 證券名稱, 累計, 處置起訖時間, 處置原因, 處置內容, 收盤價, 本益比, memo,
       created_at, 公布日期_ts,
       law_src, last_notice, notice_cnt, notice_dt,
       處置起始日, 處置結束日, 處置總天數)
    VALUES (?,?,?,?,?,?,?,?,?,?, CURRENT_TIMESTAMP, ?,
            ?,?,?,?,?,?,?,?)
    ON CONFLICT(證券代號, 公布日期) DO UPDATE SET
      證券名稱 = excluded.證券名稱,
      累計 = excluded.累計,
      處置起訖時間 = excluded.處置起訖時間,
      處置原因 = excluded.處置原因,
      處置內容 = excluded.處置內容,
      收盤價 = excluded.收盤價,
      本益比 = excluded.本益比,
      memo = excluded.memo,
      公布日期_ts = excluded.公布日期_ts,
      law_src = excluded.law_src,
      last_notice = excluded.last_notice,
      notice_cnt = excluded.notice_cnt,
      notice_dt = excluded.notice_dt,
      處置起始日 = excluded.處置起始日,
      處置結束日 = excluded.處置結束日,
      處置總天數 = excluded.處置總天數,
      created_at = CURRENT_TIMESTAMP
    """

    for fs, fe in fetch_ranges:
        if fs > fe:
            continue

        payload = {
            "startDate": fs.strftime("%Y/%m/%d"),
            "endDate": fe.strftime("%Y/%m/%d"),
            "code": "",
            "cate": "",
            "type": "all",
            "reason": "-1",
            "measure": "-1",
            "order": "date",
            "id": "",
            "response": "json",
        }

        r = requests.post(url, data=payload, timeout=30)
        r.raise_for_status()
        js = r.json()

        tables = js.get("tables") or []
        if not tables:
            continue

        data = tables[0].get("data") or []
        if not data:
            continue

        rows = []
        for item in data:
            # fields:
            # ["編號","公布日期","證券代號","證券名稱","累計","處置起訖時間","處置原因","處置內容","收盤價","本益比"," "]
            pub_roc = str(item[1]).strip()
            pub_ad = _roc_to_ad_yyyy_mm_dd(pub_roc)
            sid = str(item[2]).strip()

            # 證券名稱有時含 "(../../mainboard/...)" 這種
            raw_name = str(item[3]).strip()
            name = raw_name.split("(")[0].strip()

            acc = _clean_int(item[4])
            period = str(item[5]).strip()
            reason = str(item[6]).strip()
            content = str(item[7]).strip()
            close_p = _clean_float(item[8])
            pe = _clean_float(item[9])
            memo = item[10]  # 多行字串/連結
            memo_str = str(memo) if memo is not None else None

            if not pub_ad:
                continue

            s_ad, e_ad, total_days = _parse_range_roc(period)

            law_src = None
            last_notice = None
            notice_cnt = None
            notice_dt = None

            rows.append((
                pub_ad, sid, name, acc, period, reason, content, close_p, pe, memo_str,
                _to_unix_ts(pub_ad),
                law_src, last_notice, notice_cnt, notice_dt,
                s_ad, e_ad, total_days
            ))

        if rows:
            db.execute_sql(upsert_sql, rows)

    _update_span(target_table, new_s, new_e)

    df = db.query_to_df(
        f"""
        SELECT *
        FROM {target_table}
        WHERE 公布日期 >= ? AND 公布日期 <= ?
        ORDER BY 公布日期, 證券代號
        """,
        (_dstr(req_s), _dstr(req_e)),
    )
    return df if df is not None else pd.DataFrame()
