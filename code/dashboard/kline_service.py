from __future__ import annotations

import pandas as pd

from dashboard.config import MARKETS


KLINE_INTERVALS = {
    "1m": "1 分 K", "5m": "5 分 K", "15m": "15 分 K", "30m": "30 分 K", "60m": "60 分 K",
    "day": "日 K", "week": "週 K", "month": "月 K", "year": "年 K",
}
INTRADAY_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "60m": 60}
MAX_MA_PERIOD = 500


def _daily_bounds_sql(market_key: str) -> str:
    if market_key == "taiex":
        return "SELECT MIN(date), MAX(date) FROM fm_taiwan_stock_daily WHERE stock_id = 'TAIEX'"
    if market_key == "tx_day":
        return '''SELECT MIN(日期), MAX(日期) FROM futures_report_daily_raw
                  WHERE futures_id = 'TX' AND 日盤開 IS NOT NULL AND 日盤收 IS NOT NULL'''
    if market_key == "tx_night":
        return '''SELECT MIN(日期), MAX(日期) FROM futures_report_daily_raw
                  WHERE futures_id = 'TX' AND 夜盤開 IS NOT NULL AND 夜盤收 IS NOT NULL'''
    if market_key == "tx_full":
        return '''SELECT MIN(日期), MAX(日期) FROM v_futures_report_daily
                  WHERE futures_id = 'TX' AND 全日開 IS NOT NULL AND 全日收 IS NOT NULL'''
    raise ValueError(f"不支援的市場：{market_key}")


def _daily_sql(market_key: str) -> str:
    if market_key == "taiex":
        return '''SELECT date AS 日期, open AS 開盤, max AS 最高, min AS 最低,
                  close AS 收盤, "Trading_money" / 100000000.0 AS 成交量 FROM fm_taiwan_stock_daily
                  WHERE stock_id = 'TAIEX' AND date <= ? ORDER BY date'''
    if market_key == "tx_day":
        return '''SELECT 日期, 日盤開 AS 開盤, 日盤高 AS 最高, 日盤低 AS 最低,
                  日盤收 AS 收盤, 日成交量 AS 成交量 FROM futures_report_daily_raw
                  WHERE futures_id = 'TX' AND 日期 <= ? ORDER BY 日期'''
    if market_key == "tx_night":
        return '''SELECT 日期, 夜盤開 AS 開盤, 夜盤高 AS 最高, 夜盤低 AS 最低,
                  夜盤收 AS 收盤, 夜成交量 AS 成交量 FROM futures_report_daily_raw
                  WHERE futures_id = 'TX' AND 日期 <= ? AND 夜盤開 IS NOT NULL ORDER BY 日期'''
    if market_key == "tx_full":
        return '''SELECT 日期, 全日開 AS 開盤, 全日高 AS 最高, 全日低 AS 最低,
                  全日收 AS 收盤, COALESCE(日成交量, 0) + COALESCE(夜成交量, 0) AS 成交量
                  FROM v_futures_report_daily WHERE futures_id = 'TX' AND 日期 <= ?
                  AND 全日開 IS NOT NULL ORDER BY 日期'''
    raise ValueError(f"不支援的市場：{market_key}")


def _intraday_bounds_sql(market_key: str) -> str:
    if market_key == "taiex":
        return "SELECT MIN(date), MAX(date) FROM fm_taiwan_stock_kbar_1m WHERE stock_id = 'TAIEX'"
    session_filter = "trading_session = 'position'" if market_key == "tx_day" else "trading_session = 'after_market'" if market_key == "tx_night" else "trading_session IN ('position', 'after_market')"
    return f"SELECT MIN(source_date), MAX(source_date) FROM fm_taiwan_futures_kbar_1m WHERE futures_id = 'TX' AND {session_filter}"


def _load_intraday_raw(market_key: str, query_start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    from common import db

    if market_key == "taiex":
        return db.query_to_df(
            '''SELECT date + minute AS 時間, open AS 開盤, high AS 最高, low AS 最低,
                      close AS 收盤, volume AS 成交量
               FROM fm_taiwan_stock_kbar_1m
               WHERE stock_id = 'TAIEX' AND (date + minute) BETWEEN ? AND ? ORDER BY date, minute''',
            (query_start.to_pydatetime(), end.to_pydatetime()),
        )

    session_filter = "trading_session = 'position'" if market_key == "tx_day" else "trading_session = 'after_market'" if market_key == "tx_night" else "trading_session IN ('position', 'after_market')"
    return db.query_to_df(
        f'''
        WITH ranked AS (
            SELECT bar_datetime AS 時間, open AS 開盤, high AS 最高, low AS 最低,
                   close AS 收盤, volume AS 成交量, trading_session,
                   row_number() OVER (
                       PARTITION BY bar_datetime, trading_session
                       ORDER BY volume DESC, contract_date ASC
                   ) AS contract_rank
            FROM fm_taiwan_futures_kbar_1m
            WHERE futures_id = 'TX' AND {session_filter}
              AND bar_datetime BETWEEN ? AND ?
        )
        SELECT 時間, 開盤, 最高, 最低, 收盤, 成交量, trading_session
        FROM ranked WHERE contract_rank = 1 ORDER BY 時間
        ''',
        (query_start.to_pydatetime(), end.to_pydatetime()),
    )


def _prepare_intraday(frame: pd.DataFrame, market_key: str, minutes: int) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["日期", "開盤", "最高", "最低", "收盤", "成交量"])
    work = frame.copy()
    work["時間"] = pd.to_datetime(work["時間"], errors="coerce")
    for column in ["開盤", "最高", "最低", "收盤", "成交量"]:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work = work.dropna(subset=["時間", "開盤", "最高", "最低", "收盤"]).sort_values("時間")
    work["成交量"] = work["成交量"].fillna(0).clip(lower=0)

    if market_key == "taiex":
        origin = work["時間"].dt.normalize() + pd.Timedelta(hours=9)
        session_key = work["時間"].dt.normalize()
    else:
        after_market = work["trading_session"].eq("after_market")
        evening_date = work["時間"].dt.normalize().where(
            work["時間"].dt.hour >= 15,
            work["時間"].dt.normalize() - pd.Timedelta(days=1),
        )
        origin = (work["時間"].dt.normalize() + pd.Timedelta(hours=8, minutes=45)).where(
            ~after_market,
            evening_date + pd.Timedelta(hours=15),
        )
        session_key = work["時間"].dt.normalize().where(~after_market, evening_date)

    elapsed = ((work["時間"] - origin).dt.total_seconds() // 60).astype("int64")
    work["分組"] = list(zip(session_key, work.get("trading_session", pd.Series("position", index=work.index)), elapsed // minutes))
    return work.groupby("分組", sort=True, observed=True).agg(
        日期=("時間", "first"), 開盤=("開盤", "first"), 最高=("最高", "max"),
        最低=("最低", "min"), 收盤=("收盤", "last"), 成交量=("成交量", "sum"),
    ).reset_index(drop=True).sort_values("日期").reset_index(drop=True)


def prepare_kline_frame(raw: pd.DataFrame, interval: str) -> pd.DataFrame:
    required = ["日期", "開盤", "最高", "最低", "收盤", "成交量"]
    frame = raw.copy()
    frame["日期"] = pd.to_datetime(frame["日期"], errors="coerce").dt.normalize()
    for column in required[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["成交量"] = frame["成交量"].fillna(0).clip(lower=0)
    frame = frame.dropna(subset=required[:-1]).sort_values("日期").drop_duplicates("日期", keep="last")
    if frame.empty or interval == "day":
        return frame[required].reset_index(drop=True)
    frame["週期鍵"] = (
        frame["日期"].dt.to_period("W-FRI") if interval == "week" else
        frame["日期"].dt.to_period("M") if interval == "month" else
        frame["日期"].dt.to_period("Y-DEC")
    )
    return frame.groupby("週期鍵", sort=True, observed=True).agg(
        日期=("日期", "last"), 開盤=("開盤", "first"), 最高=("最高", "max"),
        最低=("最低", "min"), 收盤=("收盤", "last"), 成交量=("成交量", "sum"),
    ).reset_index(drop=True)


def load_kline_data(market: str, interval: str, start_date: str, end_date: str) -> tuple[pd.DataFrame, dict]:
    if market not in MARKETS or interval not in KLINE_INTERVALS:
        raise ValueError("不支援的市場或 K 線週期")
    intraday = interval in INTRADAY_MINUTES
    start, end = pd.Timestamp(start_date), pd.Timestamp(end_date)
    if start.tzinfo is not None or end.tzinfo is not None or pd.isna(start) or pd.isna(end):
        raise ValueError("請使用本地日期時間")
    if intraday:
        if len(end_date.strip()) == 10: end = end.normalize() + pd.Timedelta(hours=23, minutes=59)
    else:
        start, end = start.normalize(), end.normalize()
    if start > end:
        raise ValueError("開始日期不可晚於結束日期")
    from common import db

    intraday = interval in INTRADAY_MINUTES
    bounds = db.query_to_df(_intraday_bounds_sql(market) if intraday else _daily_bounds_sql(market))
    if bounds.empty or pd.isna(bounds.iloc[0, 0]) or pd.isna(bounds.iloc[0, 1]):
        raise ValueError("所選市場目前沒有可用的 K 線資料")
    source_min, source_max = pd.Timestamp(bounds.iloc[0, 0]).normalize(), pd.Timestamp(bounds.iloc[0, 1]).normalize()
    start, end = max(start, source_min), min(end, source_max + (pd.Timedelta(hours=23, minutes=59) if intraday else pd.Timedelta(0)))
    if start > end:
        raise ValueError("所選期間沒有 K 線資料")

    if intraday:
        query_start = max(source_min, start - pd.Timedelta(days=45))
        bars = _prepare_intraday(_load_intraday_raw(market, query_start.normalize(), end.normalize() + pd.Timedelta(hours=23, minutes=59)), market, INTRADAY_MINUTES[interval])
        bars = bars[bars["日期"] <= end].reset_index(drop=True)
        first = bars.index[bars["日期"] >= start]
        if not len(first): raise ValueError("所選期間沒有 K 線資料")
        first_position = int(first[0])
        output = bars.iloc[max(0, first_position - MAX_MA_PERIOD):].reset_index(drop=True)
    else:
        raw = db.query_to_df(_daily_sql(market), (end.strftime("%Y-%m-%d"),))
        bars = prepare_kline_frame(raw, interval)
        first = bars.index[bars["日期"] >= start]
        if not len(first):
            raise ValueError("所選期間沒有 K 線資料")
        first_position = int(first[0])
        output = bars.iloc[max(0, first_position - MAX_MA_PERIOD):]
        output = output[output["日期"] <= end].reset_index(drop=True)

    return output, {
        "market_key": market, "market_name": MARKETS[market],
        "interval_key": interval, "interval_name": KLINE_INTERVALS[interval],
        "intraday": intraday,
        "volume_label": "成交金額（億元）" if market == "taiex" and not intraday else "成交量",
        "source_min_date": source_min.strftime("%Y-%m-%d"), "source_max_date": source_max.strftime("%Y-%m-%d"),
        "display_start_date": start.strftime("%Y-%m-%dT%H:%M" if intraday else "%Y-%m-%d"), "display_end_date": end.strftime("%Y-%m-%dT%H:%M" if intraday else "%Y-%m-%d"),
    }


PRESET_LEVELS = (
    ('夜盤收', '夜收', '#ef4444', 2),
    ('全日開', '開', '#00d5e8', 1),
    ('全日收', '收', '#00d5e8', 2),
    ('全日高', '高', '#00d5e8', 1),
    ('全日低', '低', '#00d5e8', 1),
    ('日盤轉折支撐', '日轉撐', '#2563eb', 1),
    ('日盤轉折壓力', '日轉壓', '#2563eb', 1),
    ('夜盤轉折支撐', '夜轉撐', '#2563eb', 1),
    ('夜盤轉折壓力', '夜轉壓', '#2563eb', 1),
)


def load_preset_levels(selected_date: str, start_date: str, end_date: str) -> dict:
    from common import db
    dates = [pd.Timestamp(value) for value in (selected_date, start_date, end_date)]
    if any(pd.isna(value) or value.tzinfo is not None or value != value.normalize() for value in dates):
        raise ValueError('請指定有效的日期')
    selected, start, end = dates
    if not start <= selected <= end:
        raise ValueError('匯入日期必須在已載入的期間範圍內')
    columns = ', '.join('"' + item[0] + '"' for item in PRESET_LEVELS)
    frame = db.query_to_df(f'SELECT {columns} FROM report.v_futures_report_daily WHERE futures_id = \'TX\' AND "日期" = ?', (selected.strftime('%Y-%m-%d'),))
    if frame.empty:
        raise ValueError('指定日期沒有台指期報表資料')
    import math
    row = frame.iloc[0]
    levels, missing = [], []
    for column, short, color, width in PRESET_LEVELS:
        value = row[column]
        if pd.isna(value) or not math.isfinite(float(value)):
            missing.append(column)
            continue
        levels.append(dict(id=f'preset:{selected:%Y-%m-%d}:{column}', price=float(value),
                           name=f'{selected.month}/{selected.day}{short}', color=color,
                           width=width, style=0, visible=True))
    return dict(date=selected.strftime('%Y-%m-%d'), levels=levels, missing=missing)
