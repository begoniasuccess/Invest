from __future__ import annotations

import csv
import io
import re
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common import db


TAIFEX_DAILY_URL = "https://www.taifex.com.tw/cht/3/futDataDown"
TAIFEX_DAILY_PAGE = "https://www.taifex.com.tw/cht/3/futDailyMarketView"
TARGET_TABLE = "taifex_futures_daily"
REQUIRED_COLUMN_ALIASES = {
    "交易日期": ("交易日期", "日期"),
    "契約": ("契約",),
    "到期月份(週別)": ("到期月份(週別)", "到期月份"),
    "開盤價": ("開盤價",),
    "最高價": ("最高價",),
    "最低價": ("最低價",),
    # 期交所下載檔曾使用「收盤價」及「最後成交價」兩種名稱。
    "最後成交價": ("最後成交價", "收盤價"),
    "成交量": ("成交量", "合計成交量"),
    "交易時段": ("交易時段",),
}
OPTIONAL_COLUMN_ALIASES = {
    "結算價": ("結算價",),
    "未沖銷契約數": ("未沖銷契約數", "未沖銷契約量"),
}


def _http_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"POST"}),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) InvestDashboard/1.0",
        "Referer": TAIFEX_DAILY_PAGE,
        "Origin": "https://www.taifex.com.tw",
    })
    return session


def _number(value, *, integer: bool = False):
    text = str(value or "").strip().replace(",", "")
    if not text or text in {"-", "--"}:
        return None
    number = pd.to_numeric(text, errors="coerce")
    if pd.isna(number):
        return None
    return int(number) if integer else float(number)


def _normalise_header(value: str) -> str:
    """統一期交所 CSV 表頭的換行、空白與星號。"""
    return re.sub(r"\s+", "", str(value or "").replace("\ufeff", "")).lstrip("*")


def _canonicalise_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalised_to_actual = {
        _normalise_header(column): column
        for column in frame.columns
    }
    rename_map: dict[str, str] = {}
    missing: list[str] = []

    for canonical, aliases in REQUIRED_COLUMN_ALIASES.items():
        actual = next(
            (
                normalised_to_actual[_normalise_header(alias)]
                for alias in aliases
                if _normalise_header(alias) in normalised_to_actual
            ),
            None,
        )
        if actual is None:
            missing.append(canonical)
        else:
            rename_map[actual] = canonical

    for canonical, aliases in OPTIONAL_COLUMN_ALIASES.items():
        actual = next(
            (
                normalised_to_actual[_normalise_header(alias)]
                for alias in aliases
                if _normalise_header(alias) in normalised_to_actual
            ),
            None,
        )
        if actual is not None:
            rename_map[actual] = canonical

    if missing:
        actual_headers = ", ".join(str(column) for column in frame.columns)
        raise ValueError(
            f"期交所 CSV 缺少欄位：{', '.join(missing)}；"
            f"實際表頭：{actual_headers}"
        )
    return frame.rename(columns=rename_map)


def _parse_csv(content: bytes) -> pd.DataFrame:
    text = content.decode("cp950", errors="ignore").replace("\ufeff", "")
    lines = text.splitlines()
    header_index = next(
        (index for index, line in enumerate(lines) if line.lstrip().startswith("交易日期")),
        None,
    )
    if header_index is None:
        raise ValueError("期交所回應中找不到交易行情 CSV 表頭")

    reader = csv.reader(io.StringIO("\n".join(lines[header_index:])))
    rows = list(reader)
    header = [column.strip() for column in rows[0]]

    records: list[list[str]] = []
    for raw_row in rows[1:]:
        row = [column.strip() for column in raw_row]
        while len(row) > len(header) and row[-1] == "":
            row.pop()
        if len(row) == len(header):
            records.append(row)
    return _canonicalise_columns(pd.DataFrame(records, columns=header))


def fetch_daily_market_data(
    start_date: str | datetime | pd.Timestamp,
    end_date: str | datetime | pd.Timestamp,
    commodity_id: str = "TX",
) -> pd.DataFrame:
    """下載期交所官方期貨每日交易行情，僅回傳純月份合約。"""
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    if start > end:
        raise ValueError("開始日期不可晚於結束日期")

    payload = {
        "down_type": "1",
        "commodity_id": commodity_id,
        "commodity_id2": "",
        "queryStartDate": start.strftime("%Y/%m/%d"),
        "queryEndDate": end.strftime("%Y/%m/%d"),
        "commodity_idt": commodity_id,
        "commodity_id2t": "",
        "button3": "下載",
    }
    with _http_session() as session:
        response = session.post(TAIFEX_DAILY_URL, data=payload, timeout=(10, 60))
        response.raise_for_status()
    source = _parse_csv(response.content)
    source = source.loc[
        (source["契約"].str.strip() == commodity_id)
        & source["到期月份(週別)"].str.strip().str.fullmatch(r"\d{6}", na=False)
    ].copy()
    if source.empty:
        return pd.DataFrame(columns=[
            "trade_date", "futures_id", "contract_month", "trading_session",
            "open", "high", "low", "close", "volume", "settlement_price", "open_interest",
        ])

    session_map = {"一般": "position", "盤後": "after_market"}
    output = pd.DataFrame({
        "trade_date": pd.to_datetime(source["交易日期"], format="%Y/%m/%d", errors="coerce"),
        "futures_id": source["契約"].str.strip(),
        "contract_month": source["到期月份(週別)"].str.strip(),
        "trading_session": source["交易時段"].str.strip().map(session_map),
        "open": source["開盤價"].map(_number),
        "high": source["最高價"].map(_number),
        "low": source["最低價"].map(_number),
        "close": source["最後成交價"].map(_number),
        "volume": source["成交量"].map(lambda value: _number(value, integer=True)),
        "settlement_price": source.get("結算價", pd.Series(index=source.index, dtype=object)).map(_number),
        "open_interest": source.get("未沖銷契約數", pd.Series(index=source.index, dtype=object)).map(
            lambda value: _number(value, integer=True)
        ),
    })
    output = output.dropna(subset=["trade_date", "trading_session"])
    output["trade_date"] = output["trade_date"].dt.date
    return output.reset_index(drop=True)


def _stored_dates(futures_id: str, start: pd.Timestamp, end: pd.Timestamp) -> set[pd.Timestamp]:
    frame = db.query_to_df(
        f"""
        SELECT DISTINCT trade_date FROM {TARGET_TABLE}
        WHERE futures_id = ? AND trade_date BETWEEN ? AND ?
        """,
        (futures_id, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
    )
    return set(pd.to_datetime(frame["trade_date"]).dt.normalize()) if not frame.empty else set()


def _expected_dates(futures_id: str, start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    frame = db.query_to_df(
        """
        SELECT DISTINCT date
        FROM fm_taiwan_futures_daily
        WHERE futures_id = ? AND trading_session = 'position' AND date BETWEEN ? AND ?
        ORDER BY date
        """,
        (futures_id, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
    )
    dates = list(pd.to_datetime(frame["date"]).dt.normalize()) if not frame.empty else []
    # FinMind 尚未出現的最新幾天仍需向官方查詢；週末不會成為永久缺口。
    recent_start = max(start, end - pd.Timedelta(days=10))
    dates.extend(pd.date_range(recent_start, end, freq="B"))
    return sorted(set(dates))


def sync_taifex_futures_daily(
    futures_id: str,
    start_date,
    end_date,
    *,
    recent_refresh_days: int = 10,
    chunk_days: int = 30,
) -> pd.DataFrame:
    """依資料庫快取下載期交所行情；近期區間固定重抓以接收延遲補檔。"""
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    # 期交所範圍下載若帶入未來日期，可能回傳非 CSV 頁面。
    # FinMind 可依下一交易日歸屬日取得夜盤，但官方同步一律不查今日以後。
    end = min(end, pd.Timestamp.now().normalize())
    if start > end:
        return pd.DataFrame()

    refresh_start = max(start, end - pd.Timedelta(days=max(0, recent_refresh_days)))
    stored = _stored_dates(futures_id, start, end)
    expected = _expected_dates(futures_id, start, end)
    needed = [date for date in expected if date >= refresh_start or date not in stored]
    if needed:
        ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []
        range_start = range_end = needed[0]
        for date in needed[1:]:
            if (date - range_end).days <= 7 and (date - range_start).days < chunk_days:
                range_end = date
            else:
                ranges.append((range_start, range_end))
                range_start = range_end = date
        ranges.append((range_start, range_end))

        for first, last in ranges:
            frame = fetch_daily_market_data(first, last, futures_id)
            if not frame.empty:
                ok = db.execute_sql(
                    f"""
                    INSERT INTO {TARGET_TABLE} (
                        trade_date, futures_id, contract_month, trading_session,
                        open, high, low, close, volume, settlement_price, open_interest
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (trade_date, futures_id, contract_month, trading_session) DO UPDATE SET
                        open = excluded.open, high = excluded.high, low = excluded.low,
                        close = excluded.close, volume = excluded.volume,
                        settlement_price = excluded.settlement_price,
                        open_interest = excluded.open_interest,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    list(frame.itertuples(index=False, name=None)),
                )
                if not ok:
                    raise RuntimeError("期交所官方行情寫入 PostgreSQL 失敗")

    return db.query_to_df(
        f"SELECT * FROM {TARGET_TABLE} WHERE futures_id = ? AND trade_date BETWEEN ? AND ? ORDER BY trade_date, trading_session, contract_month",
        (futures_id, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
    )


if __name__ == "__main__":
    today = pd.Timestamp.today().normalize()
    print(sync_taifex_futures_daily("TX", today - pd.Timedelta(days=14), today + pd.Timedelta(days=4)))
