from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from math import floor

import numpy as np
import pandas as pd

from dashboard.config import INDICATORS, MARKETS, IndicatorSpec

TAIL_MERGE_THRESHOLD_PCT = 0.25


@dataclass(frozen=True)
class MarketSnapshot:
    market_key: str
    market_name: str
    data: pd.DataFrame
    source_min_date: pd.Timestamp
    source_max_date: pd.Timestamp
    invalid_ohlc_count: int


def _ohlc_sql(market_key: str) -> str:
    if market_key == "taiex":
        sql = """
            SELECT
                date AS 日期,
                open AS 開盤,
                max AS 最高,
                min AS 最低,
                close AS 收盤
            FROM fm_taiwan_stock_daily
            WHERE stock_id = 'TAIEX'
            ORDER BY date
        """
    elif market_key == "tx_day":
        sql = """
            SELECT
                日期,
                日盤開 AS 開盤,
                日盤高 AS 最高,
                日盤低 AS 最低,
                日盤收 AS 收盤
            FROM futures_report_daily_raw
            WHERE futures_id = 'TX'
            ORDER BY 日期
        """
    elif market_key == "tx_night":
        sql = """
            SELECT
                日期,
                夜盤開 AS 開盤,
                夜盤高 AS 最高,
                夜盤低 AS 最低,
                夜盤收 AS 收盤
            FROM futures_report_daily_raw
            WHERE futures_id = 'TX'
              AND 夜盤開 IS NOT NULL
              AND 夜盤高 IS NOT NULL
              AND 夜盤低 IS NOT NULL
              AND 夜盤收 IS NOT NULL
            ORDER BY 日期
        """
    elif market_key == "tx_full":
        sql = """
            SELECT
                日期,
                全日開 AS 開盤,
                全日高 AS 最高,
                全日低 AS 最低,
                全日收 AS 收盤
            FROM v_futures_report_daily
            WHERE futures_id = 'TX'
              AND 全日開 IS NOT NULL
              AND 全日高 IS NOT NULL
              AND 全日低 IS NOT NULL
              AND 全日收 IS NOT NULL
            ORDER BY 日期
        """
    else:
        raise ValueError(f"不支援的市場：{market_key}")

    return sql


def _load_ohlc(market_key: str) -> pd.DataFrame:
    from common import db

    return db.query_to_df(_ohlc_sql(market_key))


def prepare_market_frame(raw: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    required = ["日期", "開盤", "最高", "最低", "收盤"]
    missing = [column for column in required if column not in raw.columns]
    if missing:
        raise ValueError(f"行情資料缺少欄位：{', '.join(missing)}")

    frame = raw[required].copy()
    frame["日期"] = pd.to_datetime(frame["日期"], errors="coerce")
    for column in ["開盤", "最高", "最低", "收盤"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.dropna(subset=required).sort_values("日期")
    frame = frame.drop_duplicates(subset=["日期"], keep="last").reset_index(drop=True)
    if frame.empty:
        raise ValueError("資料庫查不到有效行情資料")

    invalid = (
        (frame["最高"] < frame["最低"])
        | (frame["最高"] < frame[["開盤", "收盤"]].max(axis=1))
        | (frame["最低"] > frame[["開盤", "收盤"]].min(axis=1))
        | (frame[["開盤", "最高", "最低", "收盤"]] <= 0).any(axis=1)
    )
    invalid_count = int(invalid.sum())
    frame = frame.loc[~invalid].copy()

    frame["昨收"] = frame["收盤"].shift(1)
    frame["range_points"] = frame["最高"] - frame["最低"]
    frame["range_prev_pct"] = frame["range_points"] / frame["昨收"].replace(0, np.nan) * 100
    frame["range_open_pct"] = frame["range_points"] / frame["開盤"].replace(0, np.nan) * 100
    frame["close_open_points"] = frame["收盤"] - frame["開盤"]
    frame["close_open_prev_pct"] = frame["close_open_points"] / frame["昨收"].replace(0, np.nan) * 100
    frame["close_prev_points"] = frame["收盤"] - frame["昨收"]
    frame["close_prev_pct"] = frame["close_prev_points"] / frame["昨收"].replace(0, np.nan) * 100
    frame["年"] = frame["日期"].dt.year.astype(int)
    return frame, invalid_count


def load_market_snapshot(market_key: str) -> MarketSnapshot:
    raw = _load_ohlc(market_key)
    frame, invalid_count = prepare_market_frame(raw)
    return MarketSnapshot(
        market_key=market_key,
        market_name=MARKETS[market_key],
        data=frame,
        source_min_date=frame["日期"].min(),
        source_max_date=frame["日期"].max(),
        invalid_ohlc_count=invalid_count,
    )


def filter_period(frame: pd.DataFrame, start_date: str | None, end_date: str | None) -> pd.DataFrame:
    result = frame
    if start_date:
        result = result.loc[result["日期"] >= pd.Timestamp(start_date)]
    if end_date:
        result = result.loc[result["日期"] <= pd.Timestamp(end_date)]
    return result.copy()


def _dynamic_bins(values: pd.Series, spec: IndicatorSpec, bin_width: float) -> tuple[list[float], list[str]]:
    if not np.isfinite(bin_width) or bin_width <= 0:
        raise ValueError("級距寬度必須大於 0")

    width = Decimal(str(bin_width))
    minimum = Decimal(str(float(values.min())))
    maximum = Decimal(str(float(values.max())))
    lower_index = floor(minimum / width)
    upper_index = floor(maximum / width) + 1
    bucket_count = upper_index - lower_index
    if bucket_count > 200:
        suggested = float((maximum - minimum) / Decimal(200))
        raise ValueError(f"級距過小，會產生 {bucket_count} 組；請輸入至少 {suggested:.4g}{spec.unit}")

    decimals = max(1, -width.as_tuple().exponent) if spec.unit == "%" else 0
    suffix = "%" if spec.unit == "%" else ""
    decimal_edges = [Decimal(index) * width for index in range(lower_index, upper_index + 1)]

    def display(value: Decimal) -> str:
        if decimals == 0:
            return f"{value:.0f}{suffix}"
        return f"{value:.{decimals}f}{suffix}"

    labels = [f"{display(left)}~{display(right)}" for left, right in zip(decimal_edges, decimal_edges[1:])]
    return [float(value) for value in decimal_edges], labels


def _merge_sparse_edge_buckets(
    working: pd.DataFrame,
    labels: list[str],
    sample_count: int,
    threshold_pct: float = TAIL_MERGE_THRESHOLD_PCT,
) -> tuple[pd.DataFrame, list[str]]:
    """只合併分布最前端與最後端、個別占比低於門檻的級距。"""
    if len(labels) < 2:
        return working, labels

    preliminary = working.groupby("級距", observed=False).size().reindex(labels, fill_value=0)
    shares = preliminary / sample_count * 100

    head_count = 0
    while head_count < len(labels) and shares.iloc[head_count] < threshold_pct:
        head_count += 1

    tail_count = 0
    while len(labels) - tail_count - 1 >= head_count and shares.iloc[-tail_count - 1] < threshold_pct:
        tail_count += 1

    if head_count == 0 and tail_count == 0:
        return working, labels

    mapping = {label: label for label in labels}
    merged_labels: list[str] = []

    if head_count:
        head_upper = labels[head_count - 1].split("~", 1)[-1]
        head_label = f"<{head_upper}"
        merged_labels.append(head_label)
        for label in labels[:head_count]:
            mapping[label] = head_label

    middle_end = len(labels) - tail_count if tail_count else len(labels)
    merged_labels.extend(labels[head_count:middle_end])

    if tail_count:
        tail_lower = labels[-tail_count].split("~", 1)[0]
        tail_label = f">={tail_lower}"
        merged_labels.append(tail_label)
        for label in labels[-tail_count:]:
            mapping[label] = tail_label

    merged = working.copy()
    merged["級距"] = merged["級距"].map(lambda value: mapping[str(value)])
    return merged, merged_labels


def build_distribution(
    frame: pd.DataFrame,
    spec: IndicatorSpec,
    bin_width: float | None = None,
) -> tuple[pd.DataFrame, dict[str, float | int | str]]:
    values = pd.to_numeric(frame[spec.key], errors="coerce").dropna()
    if values.empty:
        raise ValueError("所選期間沒有可統計的有效資料")

    if bin_width is None:
        bins = list(spec.bins)
        labels = list(spec.labels)
    else:
        bins, labels = _dynamic_bins(values, spec, bin_width)

    bucket = pd.cut(
        values,
        bins=bins,
        labels=labels,
        right=False,
        include_lowest=True,
        ordered=True,
    )
    working = frame.loc[values.index, ["年"]].copy()
    working["級距"] = bucket
    working, labels = _merge_sparse_edge_buckets(working, labels, len(values))

    totals = working.groupby("級距", observed=False).size().reindex(labels, fill_value=0)
    yearly = pd.crosstab(working["級距"], working["年"], dropna=False).reindex(labels, fill_value=0)
    yearly = yearly.reindex(sorted(yearly.columns), axis=1)

    table = pd.DataFrame({"級距": labels, "總計": totals.to_numpy(dtype=int)})
    for year in yearly.columns:
        table[str(int(year))] = yearly[year].to_numpy(dtype=int)
    table["分組占比"] = (table["總計"] / len(values) * 100).map(lambda x: f"{x:.2f}%")

    mode_label = str(totals.idxmax())
    summary: dict[str, float | int | str] = {
        "count": int(values.size),
        "mean": float(values.mean()),
        "median": float(values.median()),
        "std": float(values.std(ddof=1)) if values.size > 1 else 0.0,
        "min": float(values.min()),
        "max": float(values.max()),
        "mode_bucket": mode_label,
    }
    return table, summary
