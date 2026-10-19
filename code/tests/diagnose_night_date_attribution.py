from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from FinMind.data import DataLoader


FILE_PATH = Path(__file__).resolve()
PROJECT_ROOT = next(
    (
        path
        for path in (*FILE_PATH.parents, Path.cwd().resolve())
        if (path / "module" / "futures_datas.py").exists()
    ),
    FILE_PATH.parents[1],
)
sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv(PROJECT_ROOT / ".env")

from module.futures_datas import fetch_daily_market_data  # noqa: E402


PURE_MONTH_PATTERN = r"^\d{6}$"


def _empty_result(source: str) -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "資料來源",
            "來源交易日期",
            "期貨代號",
            "契約月份",
            "交易時段",
            "開盤",
            "最高",
            "最低",
            "收盤",
            "成交量",
        ]
    ).assign(資料來源=source)


def fetch_taifex_raw(
    futures_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    """直接下載期交所官方 CSV，不讀寫資料庫。"""
    frame = fetch_daily_market_data(start_date, end_date, futures_id)
    if frame is None or frame.empty:
        return _empty_result("TAIFEX")

    frame = frame.loc[frame["trading_session"].eq("after_market")].copy()
    if frame.empty:
        return _empty_result("TAIFEX")

    return pd.DataFrame(
        {
            "資料來源": "TAIFEX",
            "來源交易日期": pd.to_datetime(frame["trade_date"]).dt.date,
            "期貨代號": frame["futures_id"],
            "契約月份": frame["contract_month"],
            "交易時段": frame["trading_session"],
            "開盤": frame["open"],
            "最高": frame["high"],
            "最低": frame["low"],
            "收盤": frame["close"],
            "成交量": frame["volume"],
        }
    ).sort_values(["來源交易日期", "成交量"], ascending=[True, False])


def fetch_finmind_raw(
    futures_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    """直接呼叫 FinMind，不使用專案快取，也不寫入資料庫。"""
    token = os.getenv("FINMIND_TOKEN", "").strip()
    if not token:
        raise RuntimeError(f"找不到 FINMIND_TOKEN，請檢查 {PROJECT_ROOT / '.env'}")

    api = DataLoader()
    api.login_by_token(api_token=token)
    frame = api.taiwan_futures_daily(
        futures_id=futures_id,
        start_date=start_date.strftime("%Y-%m-%d"),
        # FinMind 的區間端點行為可能隨套件版本不同，多加一天避免漏掉 end_date。
        end_date=(end_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    if frame is None or frame.empty:
        return _empty_result("FinMind")

    required = {"date", "futures_id", "contract_date", "trading_session"}
    missing = required.difference(frame.columns)
    if missing:
        raise RuntimeError(f"FinMind 回傳缺少欄位：{', '.join(sorted(missing))}")

    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["contract_date"] = frame["contract_date"].astype(str).str.strip()
    frame = frame.loc[
        frame["date"].between(start_date, end_date)
        & frame["trading_session"].astype(str).str.strip().eq("after_market")
        & frame["contract_date"].str.fullmatch(PURE_MONTH_PATTERN, na=False)
    ].copy()
    if frame.empty:
        return _empty_result("FinMind")

    return pd.DataFrame(
        {
            "資料來源": "FinMind",
            "來源交易日期": frame["date"].dt.date,
            "期貨代號": frame["futures_id"],
            "契約月份": frame["contract_date"],
            "交易時段": frame["trading_session"],
            "開盤": pd.to_numeric(frame.get("open"), errors="coerce"),
            "最高": pd.to_numeric(frame.get("max"), errors="coerce"),
            "最低": pd.to_numeric(frame.get("min"), errors="coerce"),
            "收盤": pd.to_numeric(frame.get("close"), errors="coerce"),
            "成交量": pd.to_numeric(frame.get("volume"), errors="coerce"),
        }
    ).sort_values(["來源交易日期", "成交量"], ascending=[True, False])


def _night_dates(frame: pd.DataFrame) -> list[pd.Timestamp]:
    if frame.empty:
        return []
    dates = pd.to_datetime(frame["來源交易日期"], errors="coerce").dropna()
    return sorted({date.normalize() for date in dates})


def _print_source_result(
    source: str,
    frame: pd.DataFrame | None,
    error: Exception | None,
) -> None:
    print(f"\n{'=' * 18} {source} {'=' * 18}")
    if error is not None:
        print(f"查詢失敗：{type(error).__name__}: {error}")
        return
    if frame is None or frame.empty:
        print("查詢區間內沒有純月份 TX 盤後資料。")
        return
    print(frame.to_string(index=False))


def _conclusion(
    report_date: pd.Timestamp,
    end_date: pd.Timestamp,
    official: pd.DataFrame | None,
    finmind: pd.DataFrame | None,
) -> None:
    print(f"\n{'=' * 18} 判讀結果 {'=' * 18}")
    print(f"專案報表夜盤歸屬日：{report_date.date()}")
    print(f"外部來源檢查區間：{report_date.date()} ～ {end_date.date()}")

    for source, frame in (("TAIFEX", official), ("FinMind", finmind)):
        if frame is None:
            print(f"{source}：查詢失敗，無法判讀。")
            continue
        dates = _night_dates(frame)
        if not dates:
            print(
                f"{source}：目前仍查不到這個區間的盤後資料；"
                "可能尚未發布或來源延遲，不能據此否定日期歸屬假設。"
            )
            continue

        same_date_exists = report_date in dates
        later_dates = [date for date in dates if date > report_date]
        next_date = later_dates[0] if later_dates else None

        if same_date_exists:
            print(
                f"{source}：有 {report_date.date()} 的盤後列；依下一交易日歸屬規則，"
                "這列代表前一交易日下午至本日凌晨，不是本日下午開始的夜盤。"
            )
        else:
            print(f"{source}：沒有 {report_date.date()} 的盤後列。")

        if next_date is not None:
            days = (next_date - report_date).days
            print(
                f"{source}：下一筆盤後列標在 {next_date.date()}，比報表日期晚 {days} 天；"
                f"這才是 {report_date.date()} 下午開始之夜盤的候選資料，"
                "符合『夜盤歸到下一交易日』的假設。"
            )
        else:
            print(
                f"{source}：目前尚未出現 {report_date.date()} 之後的盤後列；"
                "若下一交易日尚未發布，這是正常現象，稍後再測即可。"
            )

    official_later = (
        [date for date in _night_dates(official) if date > report_date]
        if official is not None
        else []
    )
    finmind_later = (
        [date for date in _night_dates(finmind) if date > report_date]
        if finmind is not None
        else []
    )
    if official_later and finmind_later:
        if official_later[0] == finmind_later[0]:
            print(f"兩個來源的下一個盤後歸屬日一致：{official_later[0].date()}。")
        else:
            print(
                "兩個來源的下一個盤後歸屬日不一致："
                f"TAIFEX={official_later[0].date()}、FinMind={finmind_later[0].date()}。"
            )


def run_diagnostic(report_date: str, futures_id: str = "TX", lookahead_days: int = 7) -> int:
    start = pd.Timestamp(report_date).normalize()
    end = start + pd.Timedelta(days=lookahead_days)

    official = finmind = None
    official_error = finmind_error = None
    try:
        official = fetch_taifex_raw(futures_id, start, end)
    except Exception as exc:  # 診斷時必須保留另一來源的查詢結果
        official_error = exc

    try:
        finmind = fetch_finmind_raw(futures_id, start, end)
    except Exception as exc:  # 診斷時必須保留另一來源的查詢結果
        finmind_error = exc

    _print_source_result("期交所官方 CSV", official, official_error)
    _print_source_result("FinMind 原始 API", finmind, finmind_error)
    _conclusion(start, end, official, finmind)

    return 1 if official_error is not None or finmind_error is not None else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="比較期交所與 FinMind 的 TX 盤後資料交易日歸屬（只讀，不寫資料庫）"
    )
    parser.add_argument(
        "--report-date",
        default="2026-08-21",
        help="專案報表所使用的夜盤日期，格式 YYYY-MM-DD",
    )
    parser.add_argument("--futures-id", default="TX", help="期貨代號，預設 TX")
    parser.add_argument(
        "--lookahead-days",
        type=int,
        default=7,
        help="從報表日期向後檢查幾個日曆日，預設 7",
    )
    args = parser.parse_args()
    return run_diagnostic(args.report_date, args.futures_id, args.lookahead_days)


if __name__ == "__main__":
    raise SystemExit(main())
