from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
DATASET = "TaiwanTotalExchangeMarginMaintenance"
TARGET_TABLE = "raw_finmind.fm_taiwan_total_exchange_margin_maintenance"
SPAN_TARGET = "fm_taiwan_total_exchange_margin_maintenance"
SPAN_KEY = "all"
AVAILABLE_FROM = date(2001, 1, 5)


@dataclass(frozen=True)
class MarginMaintenanceUpdateResult:
    requested_start: date
    requested_end: date
    rows: int
    written_rows: int
    first_date: date | None
    last_date: date | None


def _finmind_token() -> str:
    token = os.getenv("FINMIND_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            f"找不到 FINMIND_TOKEN，請確認已在 {PROJECT_ROOT / '.env'} 設定 FINMIND_TOKEN"
        )
    return token


def _as_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()


def _normalize(frame: pd.DataFrame) -> pd.DataFrame:
    columns = ["date", "total_exchange_margin_maintenance"]
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)

    out = frame.copy()
    ratio_column = next(
        (
            column
            for column in out.columns
            if str(column).lower() == "totalexchangemarginmaintenance".lower()
        ),
        None,
    )
    if "date" not in out.columns or ratio_column is None:
        raise RuntimeError(f"FinMind {DATASET} 回傳欄位異常：{list(out.columns)}")

    out = out.rename(columns={ratio_column: "total_exchange_margin_maintenance"})
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["total_exchange_margin_maintenance"] = pd.to_numeric(
        out["total_exchange_margin_maintenance"], errors="coerce"
    )
    out = out.dropna(subset=columns)
    out = out[out["total_exchange_margin_maintenance"] > 0]
    out = out.drop_duplicates(subset=["date"], keep="last")
    return out[columns].sort_values("date").reset_index(drop=True)


def fetch_taiwan_total_margin_maintenance(
    start_date: str | date | datetime,
    end_date: str | date | datetime,
    *,
    timeout_seconds: int = 60,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """下載 FinMind 台灣大盤融資維持率原始資料。"""
    start = _as_date(start_date)
    end = _as_date(end_date)
    if start > end:
        raise ValueError("start_date 不可大於 end_date")

    sess = session or requests.Session()
    response = sess.get(
        FINMIND_API_URL,
        headers={"Authorization": f"Bearer {_finmind_token()}"},
        params={
            "dataset": DATASET,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") not in (None, 200):
        raise RuntimeError(
            f"FinMind API 失敗：dataset={DATASET}, range={start}~{end}, "
            f"status={payload.get('status')}, msg={payload.get('msg', '')}"
        )
    return _normalize(pd.DataFrame(payload.get("data") or []))


def upsert_taiwan_total_margin_maintenance(frame: pd.DataFrame) -> int:
    from common import db

    if frame is None or frame.empty:
        return 0
    rows = list(
        frame[["date", "total_exchange_margin_maintenance"]].itertuples(
            index=False, name=None
        )
    )
    ok = db.execute_sql(
        f"""
        INSERT INTO {TARGET_TABLE} (date, total_exchange_margin_maintenance)
        VALUES (?, ?)
        ON CONFLICT (date) DO UPDATE SET
            total_exchange_margin_maintenance = EXCLUDED.total_exchange_margin_maintenance,
            updated_at = CURRENT_TIMESTAMP
        """,
        rows,
    )
    if not ok:
        raise RuntimeError(f"寫入 {TARGET_TABLE} 失敗")
    return len(rows)


def _cached_span() -> tuple[date | None, date | None]:
    from common import db

    frame = db.query_to_df(
        """
        SELECT start_date, end_date
        FROM core.date_span
        WHERE target_table = ? AND idx_key = ?
        """,
        (SPAN_TARGET, SPAN_KEY),
    )
    if frame.empty:
        return None, None
    return (
        pd.Timestamp(frame.loc[0, "start_date"]).date(),
        pd.Timestamp(frame.loc[0, "end_date"]).date(),
    )


def _sync_span_from_table() -> None:
    from common import db

    bounds = db.query_to_df(
        f"SELECT MIN(date) AS start_date, MAX(date) AS end_date FROM {TARGET_TABLE}"
    )
    if bounds.empty or pd.isna(bounds.loc[0, "start_date"]):
        return
    ok = db.execute_sql(
        """
        INSERT INTO core.date_span (target_table, idx_key, start_date, end_date, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (target_table, idx_key) DO UPDATE SET
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            SPAN_TARGET,
            SPAN_KEY,
            pd.Timestamp(bounds.loc[0, "start_date"]).date(),
            pd.Timestamp(bounds.loc[0, "end_date"]).date(),
        ),
    )
    if not ok:
        raise RuntimeError("更新大盤融資維持率下載區間失敗")


def update_taiwan_total_margin_maintenance(
    end_date: str | date | datetime | None = None,
    *,
    floor_date: str | date | datetime = AVAILABLE_FROM,
    lookback_days: int = 7,
) -> MarginMaintenanceUpdateResult:
    """依已下載區間增量更新，並回看近期資料以承接盤後延遲發布。"""
    end = _as_date(end_date or date.today())
    floor = max(AVAILABLE_FROM, _as_date(floor_date))
    if floor > end:
        raise ValueError("floor_date 不可大於 end_date")

    _, cached_end = _cached_span()
    if cached_end is None:
        start = floor
    else:
        start = max(floor, cached_end - timedelta(days=max(1, int(lookback_days))))
    if start > end:
        start = end

    print(f"[大盤融資維持率] FinMind 增量更新：{start}～{end}")
    started = time.perf_counter()
    frame = fetch_taiwan_total_margin_maintenance(start, end)
    written = upsert_taiwan_total_margin_maintenance(frame)
    _sync_span_from_table()
    print(
        f"[大盤融資維持率] 完成：{len(frame):,} 筆，"
        f"{time.perf_counter() - started:.2f} 秒"
    )
    return MarginMaintenanceUpdateResult(
        requested_start=start,
        requested_end=end,
        rows=len(frame),
        written_rows=written,
        first_date=None if frame.empty else frame["date"].min(),
        last_date=None if frame.empty else frame["date"].max(),
    )
