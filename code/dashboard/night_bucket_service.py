from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from common.db import sql_to_df


METRICS = {
    "next_open_return_pct": "1. 隔日開盤漲跌幅",
    "next_close_return_pct": "2. 隔日收盤漲跌幅",
    "next_intraday_return_pct": "3. 隔日日內漲跌幅",
    "next_range_prev_close_pct": "4. 隔日最大振幅（昨收）",
    "night_close_to_next_open_pct": "5. 隔日開盤相對夜收",
    "max_up_from_open_pct": "6. 隔日最大上漲幅",
    "max_down_from_open_pct": "7. 隔日最大下跌幅",
    "next_range_open_pct": "8. 隔日最大振幅（開盤）",
    "close_location_pct": "9. 隔日收盤位置",
    "night_return_pct": "10. 夜盤漲跌幅",
}

DEFAULT_METRIC = "next_close_return_pct"
DEFAULT_START_DATE = "2018-01-01"
DEFAULT_END_DATE = "2026-09-10"

LATEST_RUN_SQL = """
SELECT
    run_id::text AS run_id,
    start_date,
    end_date,
    bucket_edges,
    metric_version,
    source_rows,
    valid_rows,
    excluded_rows,
    exclusion_summary,
    finished_at
FROM research.analysis_run
WHERE analysis_name = 'futures_night_bucket_v1'
  AND status = 'completed'
ORDER BY finished_at DESC NULLS LAST, started_at DESC
LIMIT 1
"""

DETAIL_SQL = """
SELECT *
FROM research.futures_night_bucket_daily
WHERE run_id = ?::uuid
  AND group_date BETWEEN ? AND ?
ORDER BY group_date
"""


@dataclass(frozen=True)
class NightBucketSnapshot:
    run: dict[str, Any]
    daily: pd.DataFrame


def _validate_period(start_date: str, end_date: str) -> tuple[date, date]:
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
    except ValueError as exc:
        raise ValueError("日期格式必須為 YYYY-MM-DD") from exc
    if start > end:
        raise ValueError("開始日期不可晚於結束日期")
    return start, end


def _load_latest_run() -> dict[str, Any]:
    frame = sql_to_df(LATEST_RUN_SQL)
    if frame.empty:
        raise ValueError(
            "尚無夜盤分層分析結果；請先執行 python -m analysis.futures_night_bucket"
        )
    run = frame.iloc[0].to_dict()
    for key in ("start_date", "end_date", "finished_at"):
        value = run.get(key)
        if value is not None:
            run[key] = pd.Timestamp(value).isoformat() if key == "finished_at" else pd.Timestamp(value).strftime("%Y-%m-%d")
    return run


def load_snapshot(start_date: str, end_date: str) -> NightBucketSnapshot:
    start, end = _validate_period(start_date, end_date)
    run = _load_latest_run()
    frame = sql_to_df(
        DETAIL_SQL,
        params=(run["run_id"], start, end),
        parse_dates=["group_date", "result_date"],
    )
    if frame.empty:
        raise ValueError("所選期間沒有可用的夜盤分層樣本")
    return NightBucketSnapshot(run=run, daily=frame)


def _finite_or_none(value: Any) -> float | int | None:
    if value is None or pd.isna(value) or not np.isfinite(value):
        return None
    return value.item() if isinstance(value, np.generic) else value


def _stats(values: pd.Series) -> dict[str, Any]:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    count = int(len(clean))
    positive = int((clean > 0).sum())
    return {
        "sample_count": count,
        "mean": _finite_or_none(clean.mean()),
        "median": _finite_or_none(clean.median()),
        "std": _finite_or_none(clean.std(ddof=1)) if count > 1 else None,
        "min": _finite_or_none(clean.min()),
        "q10": _finite_or_none(clean.quantile(0.10)),
        "q25": _finite_or_none(clean.quantile(0.25)),
        "q75": _finite_or_none(clean.quantile(0.75)),
        "q90": _finite_or_none(clean.quantile(0.90)),
        "max": _finite_or_none(clean.max()),
        "positive_count": positive,
        "positive_rate_pct": positive / count * 100.0 if count else None,
    }


def _bucket_labels_from_edges(edges: Any) -> dict[int, str]:
    values = [float(value) for value in edges]
    fmt = lambda value: f"{value:g}%"
    labels = {1: f"01 小於 {fmt(values[0])}"}
    for index, (lower, upper) in enumerate(zip(values, values[1:]), start=2):
        labels[index] = f"{index:02d} [{fmt(lower)}, {fmt(upper)})"
    labels[10] = f"10 大於等於 {fmt(values[-1])}"
    return labels


def build_payload(start_date: str, end_date: str, metric: str) -> dict[str, Any]:
    if metric not in METRICS:
        raise ValueError("不支援的分析指標")
    snapshot = load_snapshot(start_date, end_date)
    frame = snapshot.daily.copy()
    bucket_labels = _bucket_labels_from_edges(snapshot.run["bucket_edges"])

    bucket_rows: list[dict[str, Any]] = []
    box_rows: list[dict[str, Any]] = []
    for bucket_id in range(1, 11):
        group = frame.loc[frame["bucket_id"] == bucket_id]
        label = group["bucket_label"].iloc[0] if not group.empty else bucket_labels[bucket_id]
        stats = _stats(group[metric])
        comparable = group["direction_continued"].dropna()
        filled = group["filled_base_day_close"].dropna()
        stats.update({
            "bucket_id": bucket_id,
            "bucket_label": label,
            "continuation_rate_pct": _finite_or_none(comparable.astype(bool).mean() * 100.0) if len(comparable) else None,
            "fill_rate_pct": _finite_or_none(filled.astype(bool).mean() * 100.0) if len(filled) else None,
            "sample_warning": len(group) < 30,
        })
        bucket_rows.append(stats)
        box_rows.append({
            "bucket_id": bucket_id,
            "bucket_label": label,
            "values": [float(value) for value in pd.to_numeric(group[metric], errors="coerce").dropna()],
        })

    yearly_rows = []
    frame["result_year"] = frame["result_date"].dt.year.astype(int)
    for (year, bucket_id), group in frame.groupby(["result_year", "bucket_id"], sort=True):
        values = pd.to_numeric(group[metric], errors="coerce").dropna()
        yearly_rows.append({
            "year": int(year),
            "bucket_id": int(bucket_id),
            "sample_count": int(len(values)),
            "mean": _finite_or_none(values.mean()),
        })

    overall = _stats(frame[metric])
    comparable_all = frame["direction_continued"].dropna()
    filled_all = frame["filled_base_day_close"].dropna()
    overall.update({
        "continuation_rate_pct": _finite_or_none(comparable_all.astype(bool).mean() * 100.0) if len(comparable_all) else None,
        "fill_rate_pct": _finite_or_none(filled_all.astype(bool).mean() * 100.0) if len(filled_all) else None,
    })

    return {
        "run": snapshot.run,
        "period": {"start": start_date, "end": end_date},
        "metric": {"key": metric, "label": METRICS[metric], "unit": "%"},
        "overall": overall,
        "buckets": bucket_rows,
        "box_rows": box_rows,
        "yearly_rows": yearly_rows,
    }


def export_frame(start_date: str, end_date: str, dataset: str, metric: str) -> pd.DataFrame:
    snapshot = load_snapshot(start_date, end_date)
    if dataset == "daily":
        frame = snapshot.daily.drop(columns=["run_id", "created_at"], errors="ignore").copy()
        for column in ("group_date", "result_date"):
            frame[column] = frame[column].dt.strftime("%Y-%m-%d")
        return frame
    if dataset == "summary":
        payload = build_payload(start_date, end_date, metric)
        return pd.DataFrame(payload["buckets"])
    if dataset == "yearly":
        payload = build_payload(start_date, end_date, metric)
        return pd.DataFrame(payload["yearly_rows"])
    raise ValueError("dataset 必須是 daily、summary 或 yearly")
