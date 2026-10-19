from __future__ import annotations

from datetime import date
from typing import Iterable

import numpy as np
import pandas as pd


SOURCE_VIEW = "report.v_futures_report_daily_corrected_turning"
CORE_COLUMN = "策略_核心_五日價量"
DEFAULT_START_DATE = "2018-01-01"
DEFAULT_END_DATE = "2099-12-31"
MINIMUM_SAMPLE_SIZE = 100
NIGHT_BUCKET_ORDER = ["明顯下跌", "小幅下跌", "小幅上漲", "明顯上漲"]
NIGHT_TREND_ORDER = ["漲勢盤", "跌勢盤"]
CORE_ORDER = ["多", "空"]


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.astype(float).div(denominator.astype(float).replace(0, np.nan))


def _night_bucket(value: object) -> str | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(number):
        return None
    if number < -1:
        return "明顯下跌"
    if number < 0:
        return "小幅下跌"
    if number < 1:
        return "小幅上漲"
    return "明顯上漲"


def _load_detail(start_date: str, end_date: str, futures_id: str = "TX") -> pd.DataFrame:
    from common import db

    sql = f'''
        WITH history AS (
            SELECT
                futures_id,
                "日期" AS signal_date,
                TRIM("{CORE_COLUMN}"::text) AS core_direction,
                TRIM("日盤走勢"::text) AS signal_day_trend,
                TRIM("夜盤走勢"::text) AS signal_night_trend,
                "日盤收" AS signal_day_close,
                "夜盤收" AS signal_night_close,
                CASE WHEN COUNT("日盤高" - "日盤低") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) = 30
                THEN AVG("日盤高" - "日盤低") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) END AS average_range_30,
                LEAD("日期") OVER (PARTITION BY futures_id ORDER BY "日期") AS target_date,
                LEAD("日盤開") OVER (PARTITION BY futures_id ORDER BY "日期") AS target_day_open,
                LEAD("日盤高") OVER (PARTITION BY futures_id ORDER BY "日期") AS target_day_high,
                LEAD("日盤低") OVER (PARTITION BY futures_id ORDER BY "日期") AS target_day_low,
                LEAD("日盤收") OVER (PARTITION BY futures_id ORDER BY "日期") AS target_day_close
            FROM {SOURCE_VIEW}
            WHERE futures_id = ?
        )
        SELECT * FROM history
        WHERE target_date BETWEEN ? AND ?
          AND core_direction IN ('多', '空')
          AND signal_day_close IS NOT NULL
          AND signal_night_close IS NOT NULL
          AND target_day_high IS NOT NULL
          AND target_day_low IS NOT NULL
          AND target_day_close IS NOT NULL
        ORDER BY target_date
    '''
    frame = db.sql_to_df(
        sql,
        params=(futures_id, date.fromisoformat(start_date), date.fromisoformat(end_date)),
        parse_dates=["signal_date", "target_date"],
    )
    if frame.empty:
        return frame
    numeric = [
        "signal_day_close", "signal_night_close", "average_range_30",
        "target_day_open", "target_day_high", "target_day_low", "target_day_close",
    ]
    frame[numeric] = frame[numeric].apply(pd.to_numeric, errors="coerce")
    frame["night_change_pct"] = _safe_divide(
        frame["signal_night_close"] - frame["signal_day_close"], frame["signal_day_close"]
    ) * 100
    frame["night_change_bucket"] = frame["night_change_pct"].map(_night_bucket)
    frame["target_close_return_pct"] = _safe_divide(
        frame["target_day_close"] - frame["signal_night_close"], frame["signal_night_close"]
    ) * 100
    frame["low_close_rebound_range_pct"] = _safe_divide(
        frame["target_day_close"] - frame["target_day_low"], frame["average_range_30"]
    ) * 100
    frame["high_close_pullback_range_pct"] = _safe_divide(
        frame["target_day_high"] - frame["target_day_close"], frame["average_range_30"]
    ) * 100
    sign = frame["core_direction"].map({"多": 1.0, "空": -1.0})
    frame["directional_return_pct"] = frame["target_close_return_pct"] * sign
    frame["direction_hit"] = frame["directional_return_pct"] > 0
    frame["year"] = frame["target_date"].dt.year
    return frame


def _number(value: object, digits: int = 4) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def _json_value(value: object) -> object:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value


def _trimmed_mean(values: pd.Series, proportion: float = 0.05) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna().sort_values()
    if clean.empty:
        return None
    count = int(len(clean) * proportion)
    if count and len(clean) > count * 2:
        clean = clean.iloc[count:-count]
    return _number(clean.mean())


def _summary(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"sample_count": 0}
    close = frame["target_close_return_pct"].dropna()
    directional = frame["directional_return_pct"].dropna()
    positive = directional[directional > 0].sum()
    negative = -directional[directional < 0].sum()
    result = {
        "sample_count": int(len(frame)),
        "sample_status": "ADEQUATE" if len(frame) >= MINIMUM_SAMPLE_SIZE else "LOW_SAMPLE",
        "target_up_rate_pct": _number((close > 0).mean() * 100),
        "target_down_rate_pct": _number((close < 0).mean() * 100),
        "direction_hit_rate_pct": _number((directional > 0).mean() * 100),
        "target_close_return_mean_pct": _number(close.mean()),
        "target_close_return_trimmed_mean_pct": _trimmed_mean(close),
        "target_close_return_median_pct": _number(close.median()),
        "directional_return_mean_pct": _number(directional.mean()),
        "directional_return_median_pct": _number(directional.median()),
        "directional_return_factor": _number(positive / negative) if negative else None,
    }
    for column, prefix in (
        ("low_close_rebound_range_pct", "low_rebound"),
        ("high_close_pullback_range_pct", "high_pullback"),
    ):
        values = frame[column].dropna()
        result[f"{prefix}_mean_pct"] = _number(values.mean())
        for percentile in (10, 25, 50, 75, 90):
            result[f"{prefix}_p{percentile}_pct"] = _number(values.quantile(percentile / 100))
    return result


def _group_rows(frame: pd.DataFrame, columns: Iterable[str]) -> list[dict]:
    rows: list[dict] = []
    columns = list(columns)
    for keys, group in frame.groupby(columns, dropna=False, sort=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        row = {column: _json_value(value) for column, value in zip(columns, keys)}
        row.update(_summary(group))
        rows.append(row)
    return rows


def _add_stability(matrix: list[dict], yearly_matrix: list[dict]) -> None:
    key_columns = ("core_direction", "night_change_bucket", "signal_night_trend")
    lookup: dict[tuple, list[dict]] = {}
    for row in yearly_matrix:
        lookup.setdefault(tuple(row.get(key) for key in key_columns), []).append(row)
    for row in matrix:
        years = lookup.get(tuple(row.get(key) for key in key_columns), [])
        favorable = [item for item in years if (item.get("directional_return_mean_pct") or 0) > 0]
        row["valid_year_count"] = len(years)
        row["favorable_year_count"] = len(favorable)
        row["favorable_year_rate_pct"] = _number(len(favorable) / len(years) * 100, 2) if years else None


def _filter(frame: pd.DataFrame, core: str, night_bucket: str, night_trend: str) -> pd.DataFrame:
    selected = frame
    if core in CORE_ORDER:
        selected = selected[selected["core_direction"] == core]
    if night_bucket in NIGHT_BUCKET_ORDER:
        selected = selected[selected["night_change_bucket"] == night_bucket]
    if night_trend in NIGHT_TREND_ORDER:
        selected = selected[selected["signal_night_trend"] == night_trend]
    return selected


def build_payload(
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    core: str = "全部",
    night_bucket: str = "全部",
    night_trend: str = "全部",
) -> dict:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if start < date(2018, 1, 1):
        raise ValueError("完整夜盤研究資料從 2018-01-01 開始")
    if start > end:
        raise ValueError("開始日期不可晚於結束日期")
    if core not in ["全部", *CORE_ORDER]:
        raise ValueError("不支援的核心方向")
    if night_bucket not in ["全部", *NIGHT_BUCKET_ORDER]:
        raise ValueError("不支援的夜盤漲跌分層")
    if night_trend not in ["全部", *NIGHT_TREND_ORDER]:
        raise ValueError("不支援的夜盤走勢")

    detail = _load_detail(start_date, end_date)
    if detail.empty:
        raise ValueError("所選期間沒有可用資料")
    selected = _filter(detail, core, night_bucket, night_trend)
    matrix_keys = ["core_direction", "night_change_bucket", "signal_night_trend"]
    matrix = _group_rows(detail, matrix_keys)
    yearly_matrix = _group_rows(detail, [*matrix_keys, "year"])
    _add_stability(matrix, yearly_matrix)
    matrix.sort(key=lambda row: (
        CORE_ORDER.index(row["core_direction"]),
        NIGHT_BUCKET_ORDER.index(row["night_change_bucket"]),
        NIGHT_TREND_ORDER.index(row["signal_night_trend"]),
    ))
    selected_yearly = _group_rows(selected, ["year"])

    return {
        "period": {
            "start": start_date,
            "end": end_date,
            "actual_start": detail["target_date"].min().strftime("%Y-%m-%d"),
            "actual_end": detail["target_date"].max().strftime("%Y-%m-%d"),
        },
        "filters": {"core": core, "night_bucket": night_bucket, "night_trend": night_trend},
        "definitions": {
            "night_change": "訊號日夜盤收相對訊號日日盤收；明顯為絕對值達 1% 以上",
            "close_return": "隔日日盤收相對訊號日夜盤收的報酬率",
            "directional_return": "核心多保留原報酬、核心空將報酬乘以 -1",
            "close_position": "低收反彈／高收回落除以訊號日往回30日日盤平均振幅",
        },
        "selected": _summary(selected),
        "selected_yearly": selected_yearly,
        "matrix": matrix,
        "yearly_matrix": yearly_matrix,
        "source": {"view": SOURCE_VIEW, "minimum_sample_size": MINIMUM_SAMPLE_SIZE},
    }

