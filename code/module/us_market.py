from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
TARGET_TABLE = "raw_finmind.fm_us_stock_daily"
DATASET = "USStockPrice"

# 使用者目前指定的核心四個美股指數。
CORE_US_MARKET_SYMBOLS: dict[str, str] = {
    "^IXIC": "NASDAQ Composite",
    "^SOX": "PHLX Semiconductor Index",
    "^DJI": "Dow Jones Industrial Average",
    "^GSPC": "S&P 500",
}

# 額外建議。VIX 可表示風險情緒；TSM ADR 與台股/台積電隔日連動研究很有價值。
RECOMMENDED_US_MARKET_SYMBOLS: dict[str, str] = {
    "^VIX": "CBOE Volatility Index",
    "TSM": "Taiwan Semiconductor Manufacturing ADR",
}

DEFAULT_EXPORT_DIR = PROJECT_ROOT.parent / "data" / "FinMind" / "US" / "StockPrice"


@dataclass(frozen=True)
class FetchResult:
    stock_id: str
    requested_start: date
    requested_end: date
    rows: int
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


def _chunk_ranges(start_date: date, end_date: date, chunk_years: int = 5):
    """將長日期區間切成較小 API 請求，降低單次資料量與逾時風險。"""
    if start_date > end_date:
        raise ValueError("start_date 不可大於 end_date")
    if chunk_years <= 0:
        raise ValueError("chunk_years 必須大於 0")

    current = start_date
    while current <= end_date:
        try:
            next_boundary = current.replace(year=current.year + chunk_years)
        except ValueError:  # 2/29
            next_boundary = current.replace(month=2, day=28, year=current.year + chunk_years)
        chunk_end = min(end_date, next_boundary - timedelta(days=1))
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def _normalize_us_stock_price(df: pd.DataFrame, expected_stock_id: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=["date", "stock_id", "adj_close", "close", "high", "low", "open", "volume"]
        )

    out = df.copy()
    # FinMind 文件顯示 USStockPrice 使用首字母大寫欄位；這裡做大小寫容錯。
    lower_lookup = {str(c).lower(): c for c in out.columns}

    def col(name: str):
        return lower_lookup.get(name.lower())

    rename = {}
    for source_name, target_name in [
        ("date", "date"),
        ("stock_id", "stock_id"),
        ("Adj_Close", "adj_close"),
        ("Close", "close"),
        ("High", "high"),
        ("Low", "low"),
        ("Open", "open"),
        ("Volume", "volume"),
    ]:
        actual = col(source_name)
        if actual is not None:
            rename[actual] = target_name
    out = out.rename(columns=rename)

    if "date" not in out.columns:
        raise RuntimeError(f"FinMind {DATASET} 回傳缺少 date 欄位：{list(df.columns)}")
    if "stock_id" not in out.columns:
        out["stock_id"] = expected_stock_id

    for c in ["adj_close", "close", "high", "low", "open", "volume"]:
        if c not in out.columns:
            out[c] = pd.NA

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["stock_id"] = out["stock_id"].astype(str).str.strip()
    out.loc[out["stock_id"] == "", "stock_id"] = expected_stock_id

    for c in ["adj_close", "close", "high", "low", "open", "volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["date", "stock_id"])
    out = out.drop_duplicates(subset=["date", "stock_id"], keep="last")
    out = out.sort_values(["stock_id", "date"]).reset_index(drop=True)
    return out[["date", "stock_id", "adj_close", "close", "high", "low", "open", "volume"]]


def fetch_us_stock_daily(
    stock_id: str,
    start_date: str | date | datetime,
    end_date: str | date | datetime,
    *,
    chunk_years: int = 5,
    sleep_seconds: float = 0.15,
    timeout_seconds: int = 60,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """從 FinMind USStockPrice 下載單一美股/指數的日資料。"""
    sid = str(stock_id).strip()
    if not sid:
        raise ValueError("stock_id 不可為空")

    start = _as_date(start_date)
    end = _as_date(end_date)
    if start > end:
        raise ValueError("start_date 不可大於 end_date")

    token = _finmind_token()
    sess = session or requests.Session()
    frames: list[pd.DataFrame] = []

    for chunk_start, chunk_end in _chunk_ranges(start, end, chunk_years=chunk_years):
        params = {
            "dataset": DATASET,
            "data_id": sid,
            "start_date": chunk_start.isoformat(),
            "end_date": chunk_end.isoformat(),
        }
        resp = sess.get(
            FINMIND_API_URL,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=timeout_seconds,
        )
        resp.raise_for_status()
        payload = resp.json()

        status = payload.get("status")
        msg = payload.get("msg", "")
        if status not in (None, 200):
            raise RuntimeError(
                f"FinMind API 失敗：stock_id={sid}, "
                f"range={chunk_start}~{chunk_end}, status={status}, msg={msg}"
            )

        raw = pd.DataFrame(payload.get("data") or [])
        clean = _normalize_us_stock_price(raw, sid)
        if not clean.empty:
            frames.append(clean)

        print(
            f"  {sid}  {chunk_start} ~ {chunk_end}: "
            f"{len(clean):,} rows"
        )
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    if not frames:
        return _normalize_us_stock_price(pd.DataFrame(), sid)

    result = pd.concat(frames, ignore_index=True)
    result = result.drop_duplicates(subset=["date", "stock_id"], keep="last")
    result = result.sort_values(["stock_id", "date"]).reset_index(drop=True)
    return result


def upsert_us_stock_daily(df: pd.DataFrame) -> int:
    """將已清洗的 USStockPrice 資料 upsert 到 PostgreSQL。"""
    # 延後載入，讓只輸出 CSV / --help 時不必先載入 psycopg。
    from common import db
    if df is None or df.empty:
        return 0

    rows = []
    for row in df.itertuples(index=False):
        volume = None if pd.isna(row.volume) else int(row.volume)
        values = [row.date, row.stock_id, row.adj_close, row.close, row.high, row.low, row.open, volume]
        cleaned = []
        for value in values:
            if value is pd.NA or value is pd.NaT:
                cleaned.append(None)
            elif isinstance(value, float) and pd.isna(value):
                cleaned.append(None)
            else:
                cleaned.append(value)
        rows.append(tuple(cleaned))

    sql = f"""
        INSERT INTO {TARGET_TABLE}
            (date, stock_id, adj_close, close, high, low, open, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, stock_id) DO UPDATE SET
            adj_close = EXCLUDED.adj_close,
            close = EXCLUDED.close,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            open = EXCLUDED.open,
            volume = EXCLUDED.volume,
            updated_at = CURRENT_TIMESTAMP
    """

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
    return len(rows)



def update_us_stock_daily_incremental(
    stock_ids: Sequence[str],
    end_date: str | date | datetime | None = None,
    *,
    lookback_days: int = 7,
    floor_date: str | date | datetime = "2000-01-01",
) -> dict[str, dict]:
    """Dashboard 日常用的美股日資料增量更新。

    每個代碼只從資料庫最新日期往前少量回看，避免每次從歷史起點重抓；
    多個代碼共用同一個 requests.Session，且不輸出 CSV。
    """
    from common import db

    end = _as_date(end_date or date.today())
    floor = _as_date(floor_date)
    lookback = max(1, int(lookback_days))
    results: dict[str, dict] = {}

    with requests.Session() as sess:
        for stock_id in stock_ids:
            sid = str(stock_id).strip()
            latest = db.query_single_value(
                f"SELECT MAX(date) FROM {TARGET_TABLE} WHERE stock_id = ?",
                (sid,),
            )
            if latest is None:
                start = floor
            else:
                latest_date = pd.Timestamp(latest).date()
                start = max(floor, latest_date - timedelta(days=lookback))
            if start > end:
                start = end

            print(f"[美股日資料] {sid} 增量更新：{start}～{end}")
            frame = fetch_us_stock_daily(
                sid,
                start,
                end,
                chunk_years=1,
                session=sess,
            )
            written = upsert_us_stock_daily(frame) if not frame.empty else 0
            results[sid] = {
                "stock_id": sid,
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "rows": int(len(frame)),
                "written_rows": int(written),
                "first_date": None if frame.empty else frame["date"].min().isoformat(),
                "last_date": None if frame.empty else frame["date"].max().isoformat(),
            }
    return results

def export_us_stock_daily_csv(
    df: pd.DataFrame,
    output_dir: str | Path = DEFAULT_EXPORT_DIR,
) -> list[Path]:
    """存成每代碼一份 CSV，另存 combined CSV。"""
    if df is None or df.empty:
        return []

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    def safe_name(symbol: str) -> str:
        return symbol.replace("^", "INDEX_").replace("/", "_")

    for sid, part in df.groupby("stock_id", sort=True):
        path = output / f"{safe_name(sid)}_daily.csv"
        part.sort_values("date").to_csv(path, index=False, encoding="utf-8-sig")
        written.append(path)

    combined = output / "us_market_daily_combined.csv"
    df.sort_values(["stock_id", "date"]).to_csv(combined, index=False, encoding="utf-8-sig")
    written.append(combined)
    return written


def download_us_market_daily(
    stock_ids: Sequence[str],
    start_date: str | date | datetime,
    end_date: str | date | datetime,
    *,
    save_db: bool = True,
    csv_dir: str | Path | None = DEFAULT_EXPORT_DIR,
    chunk_years: int = 5,
) -> tuple[pd.DataFrame, list[FetchResult], list[Path]]:
    """批次下載指定代碼，可同時寫 PostgreSQL 與 CSV。"""
    start = _as_date(start_date)
    end = _as_date(end_date)
    all_frames: list[pd.DataFrame] = []
    summaries: list[FetchResult] = []

    with requests.Session() as sess:
        for sid in stock_ids:
            print(f"\n=== {sid} ===")
            frame = fetch_us_stock_daily(
                sid,
                start,
                end,
                chunk_years=chunk_years,
                session=sess,
            )
            all_frames.append(frame)
            summaries.append(
                FetchResult(
                    stock_id=sid,
                    requested_start=start,
                    requested_end=end,
                    rows=len(frame),
                    first_date=None if frame.empty else frame["date"].min(),
                    last_date=None if frame.empty else frame["date"].max(),
                )
            )
            if save_db and not frame.empty:
                count = upsert_us_stock_daily(frame)
                print(f"  DB upsert: {count:,} rows -> {TARGET_TABLE}")

    combined = (
        pd.concat(all_frames, ignore_index=True)
        if all_frames
        else _normalize_us_stock_price(pd.DataFrame(), "")
    )
    if not combined.empty:
        combined = combined.drop_duplicates(["date", "stock_id"], keep="last")
        combined = combined.sort_values(["stock_id", "date"]).reset_index(drop=True)

    written = export_us_stock_daily_csv(combined, csv_dir) if csv_dir is not None else []
    return combined, summaries, written
