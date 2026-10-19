from __future__ import annotations

from datetime import datetime

import pandas as pd

from common import db, utils
from module import finMind as fm


DEFAULT_LOOKBACK_DAYS = 10


def _latest_date(table: str, date_column: str, id_column: str, id_value: str) -> pd.Timestamp | None:
    value = db.query_single_value(
        f"SELECT MAX({date_column}) FROM {table} WHERE {id_column} = ?",
        (id_value,),
    )
    return pd.Timestamp(value).normalize() if value is not None else None


def _incremental_start(
    table: str,
    date_column: str,
    id_column: str,
    id_value: str,
    end_date: pd.Timestamp,
    lookback_days: int,
) -> pd.Timestamp:
    latest = _latest_date(table, date_column, id_column, id_value)
    if latest is None:
        return end_date - pd.Timedelta(days=max(30, lookback_days))
    return min(latest, end_date) - pd.Timedelta(days=max(1, lookback_days))


def _refresh_trading_calendar() -> None:
    """先更新交易日表，避免最新交易日尚未進入分 K 請求清單。"""
    fm.get_tw_stock_trading_dates()


def update_taiex_kbar_1m(
    end_date=None,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> pd.DataFrame:
    """增量更新加權指數一分 K；近期資料固定重抓，歷史資料沿用快取。"""
    end = utils._to_norm_ts(end_date) if end_date is not None else utils._taipei_today()
    _refresh_trading_calendar()
    start = _incremental_start(
        "fm_taiwan_stock_kbar_1m", "date", "stock_id", "TAIEX", end, lookback_days
    )
    print(f"[加權一分 K] 更新區間：{start:%Y-%m-%d}～{end:%Y-%m-%d}")
    return fm.get_tw_stock_kbar("TAIEX", start, end)


def update_tx_kbar_1m(
    end_date=None,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    refresh_turning: bool = True,
) -> dict[str, pd.DataFrame]:
    """增量更新 TX Tick，完成後只重建相同來源日期範圍的一分 K。"""
    end = utils._to_norm_ts(end_date) if end_date is not None else utils._taipei_today()
    _refresh_trading_calendar()
    start = _incremental_start(
        "fm_taiwan_futures_tick", "source_date", "futures_id", "TX", end, lookback_days
    )
    print(f"[台指期一分 K] Tick 更新與 K 棒重建區間：{start:%Y-%m-%d}～{end:%Y-%m-%d}")
    tick_summary = fm.get_tw_futures_tick("TX", start, end)
    stored_dates = pd.to_datetime(
        tick_summary.loc[tick_summary["status"] == "stored", "date"],
        errors="coerce",
    ).dropna() if not tick_summary.empty else pd.Series(dtype="datetime64[ns]")
    if stored_dates.empty:
        print("[台指期一分 K] 本次沒有新增或刷新 Tick，略過 K 棒重建")
        kbar_summary = pd.DataFrame()
    else:
        rebuild_start = stored_dates.min().normalize()
        rebuild_end = stored_dates.max().normalize()
        kbar_summary = fm.build_tw_futures_kbar_1m("TX", rebuild_start, rebuild_end)
    if refresh_turning:
        from module.futures_turning import refresh_turning_levels
        refresh_turning_levels(start_date=start.date())
    return {"tick": tick_summary, "kbar_1m": kbar_summary}
