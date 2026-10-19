from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from common import db


_UPDATE_LOCK = threading.Lock()


@dataclass(frozen=True)
class UpdateResult:
    taiex_max_date: str
    tx_max_date: str
    tx_rows_synced: int


def _latest_date(table: str, date_column: str, where_sql: str = "") -> pd.Timestamp | None:
    value = db.query_single_value(
        f'SELECT MAX("{date_column}") FROM {table} {where_sql}'
    )
    return pd.Timestamp(value).normalize() if value is not None else None


def _sync_recent_tx_prices(start_date: pd.Timestamp, end_date: pd.Timestamp) -> int:
    """將近期 TX 日盤及對應夜盤 OHLC 同步到既有期貨報表。"""
    from module import futures_utils as fu

    dates_df = db.query_to_df(
        """
        SELECT DISTINCT date
        FROM fm_taiwan_futures_daily
        WHERE futures_id = 'TX'
          AND trading_session = 'position'
          AND date BETWEEN ? AND ?
        ORDER BY date
        """,
        (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")),
    )
    if dates_df.empty:
        return 0

    rows: list[tuple] = []
    prior_df = db.query_to_df(
        """
        SELECT 日盤收
        FROM futures_report_daily_raw
        WHERE futures_id = 'TX' AND 日期 < ?
        ORDER BY 日期 DESC
        LIMIT 1
        """,
        (start_date.strftime("%Y-%m-%d"),),
    )
    rolling_previous_close = (
        float(prior_df.iloc[0, 0])
        if not prior_df.empty and pd.notna(prior_df.iloc[0, 0])
        else None
    )
    for trading_date in pd.to_datetime(dates_df["date"]).dt.normalize():
        contract_month = fu._get_front_contract_month(trading_date, "TX")
        row = fu._get_month_contract_row(trading_date, "position", contract_month, "TX")
        if row is None:
            continue

        next_after_market_date = fu._get_next_after_market_date(trading_date, "TX")
        night_row = None
        if next_after_market_date is not None:
            night_contract_month = fu._get_front_contract_month(next_after_market_date, "TX")
            night_row = fu._get_month_contract_row(
                next_after_market_date,
                "after_market",
                night_contract_month,
                "TX",
            )

        open_price = float(row["open"])
        high_price = float(row["max"])
        low_price = float(row["min"])
        close_price = float(row["close"])
        day_range = high_price - low_price
        close_open = close_price - open_price
        close_change = close_price - rolling_previous_close if rolling_previous_close is not None else None

        rows.append(
            (
                "TX", trading_date.strftime("%Y-%m-%d"), trading_date.year, trading_date.month,
                fu._weekday_text(trading_date), fu._week_no(trading_date), fu._day_type_text(trading_date),
                open_price, close_price, high_price, low_price,
                close_change, day_range, day_range / open_price if open_price else None,
                day_range / rolling_previous_close if rolling_previous_close else None,
                close_open, close_open / rolling_previous_close if rolling_previous_close else None,
                float(row["volume"]) if pd.notna(row["volume"]) else None,
                float(night_row["open"]) if night_row is not None and pd.notna(night_row["open"]) else None,
                float(night_row["close"]) if night_row is not None and pd.notna(night_row["close"]) else None,
                float(night_row["max"]) if night_row is not None and pd.notna(night_row["max"]) else None,
                float(night_row["min"]) if night_row is not None and pd.notna(night_row["min"]) else None,
                float(night_row["volume"]) if night_row is not None and pd.notna(night_row["volume"]) else None,
            )
        )
        rolling_previous_close = close_price

    if not rows:
        return 0

    ok = db.execute_sql(
        """
        INSERT INTO futures_report_daily_raw (
            futures_id, 日期, 年, 月, 星期, 週次, 天別,
            日盤開, 日盤收, 日盤高, 日盤低,
            日漲跌, 日振幅, 日振幅開盤比, 日振幅昨收比,
            日收減日開, 日收開昨收比, 日成交量,
            夜盤開, 夜盤收, 夜盤高, 夜盤低, 夜成交量
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (futures_id, 日期) DO UPDATE SET
            年 = excluded.年,
            月 = excluded.月,
            星期 = excluded.星期,
            週次 = excluded.週次,
            天別 = excluded.天別,
            日盤開 = excluded.日盤開,
            日盤收 = excluded.日盤收,
            日盤高 = excluded.日盤高,
            日盤低 = excluded.日盤低,
            日漲跌 = excluded.日漲跌,
            日振幅 = excluded.日振幅,
            日振幅開盤比 = excluded.日振幅開盤比,
            日振幅昨收比 = excluded.日振幅昨收比,
            日收減日開 = excluded.日收減日開,
            日收開昨收比 = excluded.日收開昨收比,
            日成交量 = excluded.日成交量,
            夜盤開 = COALESCE(excluded.夜盤開, futures_report_daily_raw.夜盤開),
            夜盤收 = COALESCE(excluded.夜盤收, futures_report_daily_raw.夜盤收),
            夜盤高 = COALESCE(excluded.夜盤高, futures_report_daily_raw.夜盤高),
            夜盤低 = COALESCE(excluded.夜盤低, futures_report_daily_raw.夜盤低),
            夜成交量 = COALESCE(excluded.夜成交量, futures_report_daily_raw.夜成交量)
        """,
        rows,
    )
    if not ok:
        raise RuntimeError("台指期日盤／夜盤資料同步失敗，請查看終端機中的 PostgreSQL 錯誤")
    return len(rows)


def update_price_data() -> UpdateResult:
    if not _UPDATE_LOCK.acquire(blocking=False):
        raise RuntimeError("資料更新正在執行中，請稍候")
    try:
        # 延後匯入，讓使用者即使暫時缺少 FinMind 套件，仍可先瀏覽資料庫既有報表。
        from module import finMind as fm

        today = pd.Timestamp(datetime.now()).normalize()
        from module import futures_utils as fu

        # 只定錨到今天之後的第一個交易日，不再重複累加日曆日。
        futures_fetch_end = fu._get_night_source_end(today, "TX")
        taiex_latest = _latest_date("fm_taiwan_stock_daily", "date", "WHERE stock_id = 'TAIEX'")
        tx_latest = _latest_date("futures_report_daily_raw", "日期", "WHERE futures_id = 'TX'")
        taiex_start = (taiex_latest - pd.Timedelta(days=10)) if taiex_latest is not None else pd.Timestamp("2000-01-01")
        tx_start = (tx_latest - pd.Timedelta(days=10)) if tx_latest is not None else pd.Timestamp("2000-01-01")

        fm.get_tw_stock_daily_price("TAIEX", taiex_start, today)
        fm.get_tw_futures_daily("TX", tx_start, futures_fetch_end)
        try:
            from module.futures_datas import sync_taifex_futures_daily

            sync_taifex_futures_daily("TX", tx_start, futures_fetch_end)
        except Exception as exc:
            # 備援來源故障不應阻斷 FinMind 的正常更新。
            print(f"[期交所備援] 本次同步失敗，暫以既有資料繼續：{exc}")
        synced = _sync_recent_tx_prices(tx_start, today)

        # 不在日常行情更新中刷新整張期貨報表物化快取。
        # 這個動作會重算完整 v_futures_report_daily，可能耗時數分鐘；
        # 若需要完整快取與 CSV，請手動執行 main.futures.rebuild_futures_report_cache_and_csv()。
        print("[報表快取] 略過完整刷新。")

        new_taiex = _latest_date("fm_taiwan_stock_daily", "date", "WHERE stock_id = 'TAIEX'")
        new_tx = _latest_date("futures_report_daily_raw", "日期", "WHERE futures_id = 'TX'")
        return UpdateResult(
            taiex_max_date=new_taiex.strftime("%Y-%m-%d") if new_taiex is not None else "無資料",
            tx_max_date=new_tx.strftime("%Y-%m-%d") if new_tx is not None else "無資料",
            tx_rows_synced=synced,
        )
    finally:
        _UPDATE_LOCK.release()
