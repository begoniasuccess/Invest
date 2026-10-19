from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.futures_turning_level_intraday import FIVE_DAY_SIGNAL, SOURCE_VIEW


NIGHT_CHANGE_ORDER = ["明顯下跌", "小幅下跌", "小幅上漲", "明顯上漲"]


@dataclass(frozen=True)
class Settings:
    start: date
    end: date
    futures_id: str = "TX"
    range_days: int = 30
    minimum_sample_size: int = 100

    def __post_init__(self) -> None:
        if self.start != date(2018, 1, 1):
            raise ValueError("完整統計的 start 固定為 2018-01-01")
        if self.start > self.end:
            raise ValueError("start 不可晚於 end")
        if self.range_days < 1 or self.minimum_sample_size < 1:
            raise ValueError("振幅天數與最低樣本數必須大於 0")


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.astype(float).div(denominator.astype(float).replace(0, np.nan))


def classify_night_change(value: object) -> str | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    if number < -1.0:
        return "明顯下跌"
    if number < 0.0:
        return "小幅下跌"
    if number < 1.0:
        return "小幅上漲"
    return "明顯上漲"


def prepare_detail(raw: pd.DataFrame) -> pd.DataFrame:
    detail = raw.copy()
    numeric_columns = [
        "signal_day_close", "signal_night_close", "average_range_30",
        "target_day_open", "target_day_high", "target_day_low", "target_day_close",
    ]
    for column in numeric_columns:
        detail[column] = pd.to_numeric(detail[column], errors="coerce")

    detail["core_direction"] = detail["core_direction"].astype("string").str.strip()
    detail["signal_day_trend"] = detail["signal_day_trend"].astype("string").str.strip()
    detail["signal_night_trend"] = detail["signal_night_trend"].astype("string").str.strip()
    detail["night_change_pct"] = (
        _safe_divide(
            detail["signal_night_close"] - detail["signal_day_close"],
            detail["signal_day_close"],
        ) * 100.0
    )
    detail["night_change_bucket"] = detail["night_change_pct"].map(classify_night_change)
    detail["target_close_return_pct"] = (
        _safe_divide(
            detail["target_day_close"] - detail["signal_night_close"],
            detail["signal_night_close"],
        ) * 100.0
    )
    detail["target_open_to_close_pct"] = (
        _safe_divide(
            detail["target_day_close"] - detail["target_day_open"],
            detail["target_day_open"],
        ) * 100.0
    )
    detail["low_close_rebound_points"] = detail["target_day_close"] - detail["target_day_low"]
    detail["high_close_pullback_points"] = detail["target_day_high"] - detail["target_day_close"]
    detail["low_close_rebound_pct"] = (
        _safe_divide(detail["low_close_rebound_points"], detail["target_day_close"]) * 100.0
    )
    detail["high_close_pullback_pct"] = (
        _safe_divide(detail["high_close_pullback_points"], detail["target_day_close"]) * 100.0
    )
    detail["low_close_rebound_range_pct"] = (
        _safe_divide(detail["low_close_rebound_points"], detail["average_range_30"]) * 100.0
    )
    detail["high_close_pullback_range_pct"] = (
        _safe_divide(detail["high_close_pullback_points"], detail["average_range_30"]) * 100.0
    )
    direction_sign = detail["core_direction"].map({"多": 1.0, "空": -1.0})
    detail["core_directional_return_pct"] = detail["target_close_return_pct"] * direction_sign
    detail["core_direction_hit"] = detail["core_directional_return_pct"] > 0
    detail["target_close_direction"] = np.select(
        [detail["target_close_return_pct"] > 0, detail["target_close_return_pct"] < 0],
        ["收漲", "收跌"], default="平盤",
    )
    detail["year"] = pd.to_datetime(detail["target_date"]).dt.year
    return detail


def load_detail(settings: Settings) -> pd.DataFrame:
    from common import db

    sql = f'''
        WITH history AS (
            SELECT
                futures_id,
                "日期" AS signal_date,
                TRIM("{FIVE_DAY_SIGNAL}"::text) AS core_direction,
                TRIM("日盤走勢"::text) AS signal_day_trend,
                TRIM("夜盤走勢"::text) AS signal_night_trend,
                "日盤收" AS signal_day_close,
                "夜盤收" AS signal_night_close,
                CASE WHEN COUNT("日盤高" - "日盤低") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                    ROWS BETWEEN {settings.range_days - 1} PRECEDING AND CURRENT ROW
                ) = {settings.range_days}
                THEN AVG("日盤高" - "日盤低") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                    ROWS BETWEEN {settings.range_days - 1} PRECEDING AND CURRENT ROW
                ) END AS average_range_30,
                LEAD("日期") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                ) AS target_date,
                LEAD("日盤開") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                ) AS target_day_open,
                LEAD("日盤高") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                ) AS target_day_high,
                LEAD("日盤低") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                ) AS target_day_low,
                LEAD("日盤收") OVER (
                    PARTITION BY futures_id ORDER BY "日期"
                ) AS target_day_close
            FROM {SOURCE_VIEW}
            WHERE futures_id = ?
        )
        SELECT *
        FROM history
        WHERE target_date BETWEEN ? AND ?
          AND core_direction IN ('多', '空')
          AND signal_day_close IS NOT NULL
          AND signal_night_close IS NOT NULL
          AND target_day_open IS NOT NULL
          AND target_day_high IS NOT NULL
          AND target_day_low IS NOT NULL
          AND target_day_close IS NOT NULL
        ORDER BY target_date
    '''
    raw = db.sql_to_df(
        sql,
        params=(settings.futures_id, settings.start, settings.end),
        parse_dates=["signal_date", "target_date"],
    )
    return prepare_detail(raw)


def _percentile(series: pd.Series, value: float) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.quantile(value)) if len(clean) else None


def _trimmed_mean(series: pd.Series, proportion: float = 0.05) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna().sort_values()
    if not len(clean):
        return None
    trim = int(len(clean) * proportion)
    if trim and len(clean) > trim * 2:
        clean = clean.iloc[trim:-trim]
    return float(clean.mean())


def summarize(detail: pd.DataFrame, group_columns: list[str], minimum_sample_size: int) -> pd.DataFrame:
    rows: list[dict] = []
    grouped = detail.groupby(group_columns, sort=True, dropna=False)
    for keys, frame in grouped:
        keys = keys if isinstance(keys, tuple) else (keys,)
        returns = pd.to_numeric(frame["target_close_return_pct"], errors="coerce").dropna()
        directional = pd.to_numeric(frame["core_directional_return_pct"], errors="coerce").dropna()
        positive = float(directional[directional > 0].sum())
        negative = float(-directional[directional < 0].sum())
        row = dict(zip(group_columns, keys))
        row.update({
            "sample_count": len(frame),
            "sample_status": "ADEQUATE" if len(frame) >= minimum_sample_size else "LOW_SAMPLE",
            "target_up_count": int((returns > 0).sum()),
            "target_down_count": int((returns < 0).sum()),
            "target_flat_count": int((returns == 0).sum()),
            "target_up_rate_pct": float((returns > 0).mean() * 100) if len(returns) else None,
            "target_down_rate_pct": float((returns < 0).mean() * 100) if len(returns) else None,
            "core_direction_hit_rate_pct": float((directional > 0).mean() * 100) if len(directional) else None,
            "target_close_return_mean_pct": float(returns.mean()) if len(returns) else None,
            "target_close_return_trimmed_mean_pct": _trimmed_mean(returns),
            "target_close_return_median_pct": float(returns.median()) if len(returns) else None,
            "target_close_return_std_pct": float(returns.std(ddof=1)) if len(returns) > 1 else None,
            "directional_return_factor": positive / negative if negative else None,
        })
        for source, prefix in (
            ("low_close_rebound_pct", "low_close_rebound"),
            ("high_close_pullback_pct", "high_close_pullback"),
            ("low_close_rebound_range_pct", "low_close_rebound_range"),
            ("high_close_pullback_range_pct", "high_close_pullback_range"),
        ):
            values = pd.to_numeric(frame[source], errors="coerce")
            row[f"{prefix}_mean_pct"] = float(values.mean()) if values.notna().any() else None
            for percentile in (10, 25, 50, 75, 90):
                row[f"{prefix}_p{percentile}_pct"] = _percentile(values, percentile / 100)
        rows.append(row)
    result = pd.DataFrame(rows)
    numeric = result.select_dtypes(include="number").columns
    result[numeric] = result[numeric].round(4)
    return result


def add_stability(full_summary: pd.DataFrame, yearly: pd.DataFrame) -> pd.DataFrame:
    keys = ["core_direction", "night_change_bucket", "signal_night_trend"]
    rows = []
    for group_keys, frame in yearly.groupby(keys, sort=True, dropna=False):
        direction = group_keys[0]
        yearly_mean = pd.to_numeric(frame["target_close_return_mean_pct"], errors="coerce")
        favorable = yearly_mean > 0 if direction == "多" else yearly_mean < 0
        rows.append({
            **dict(zip(keys, group_keys)),
            "valid_year_count": int(yearly_mean.notna().sum()),
            "favorable_year_count": int(favorable.sum()),
            "favorable_year_rate_pct": round(float(favorable.mean() * 100), 2),
        })
    return full_summary.merge(pd.DataFrame(rows), on=keys, how="left")


def export_reports(detail: pd.DataFrame, settings: Settings, output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    actual_end = pd.to_datetime(detail["target_date"]).max().date() if not detail.empty else settings.end
    suffix = f"{settings.start:%Y%m%d}_{actual_end:%Y%m%d}"
    minimum = settings.minimum_sample_size

    by_core = summarize(detail, ["core_direction"], minimum)
    by_night_change = summarize(detail, ["core_direction", "night_change_bucket"], minimum)
    by_night_trend = summarize(detail, ["core_direction", "signal_night_trend"], minimum)
    full_groups = ["core_direction", "night_change_bucket", "signal_night_trend"]
    full = summarize(detail, full_groups, minimum)
    yearly = summarize(detail, [*full_groups, "year"], minimum)
    full = add_stability(full, yearly)

    paths = [
        output_dir / f"next_day_session_detail_{suffix}.csv",
        output_dir / f"next_day_session_by_core_{suffix}.csv",
        output_dir / f"next_day_session_by_night_change_{suffix}.csv",
        output_dir / f"next_day_session_by_night_trend_{suffix}.csv",
        output_dir / f"next_day_session_full_matrix_{suffix}.csv",
        output_dir / f"next_day_session_yearly_{suffix}.csv",
        output_dir / f"next_day_session_manifest_{suffix}.json",
    ]
    detail.to_csv(paths[0], index=False, encoding="utf-8-sig")
    by_core.to_csv(paths[1], index=False, encoding="utf-8-sig")
    by_night_change.to_csv(paths[2], index=False, encoding="utf-8-sig")
    by_night_trend.to_csv(paths[3], index=False, encoding="utf-8-sig")
    full.to_csv(paths[4], index=False, encoding="utf-8-sig")
    yearly.to_csv(paths[5], index=False, encoding="utf-8-sig")
    paths[6].write_text(json.dumps({
        "source_view": SOURCE_VIEW,
        "actual_data_end": actual_end.isoformat(),
        "settings": {**asdict(settings), "start": settings.start.isoformat(), "end": settings.end.isoformat()},
        "night_change_buckets": {
            "明顯下跌": "夜盤漲跌幅 < -1%",
            "小幅下跌": "-1% <= 夜盤漲跌幅 < 0%",
            "小幅上漲": "0% <= 夜盤漲跌幅 < 1%",
            "明顯上漲": "夜盤漲跌幅 >= 1%",
        },
        "definitions": {
            "availability": "訊號日收盤後已完成的五日核心、日盤與夜盤資料，用於研究下一交易日日盤",
            "night_change_pct": "(訊號日夜盤收-訊號日日盤收)/訊號日日盤收*100",
            "target_close_return_pct": "(隔日日盤收-訊號日夜盤收)/訊號日夜盤收*100",
            "low_close_rebound_pct": "(隔日日盤收-隔日日盤低)/隔日日盤收*100",
            "high_close_pullback_pct": "(隔日日盤高-隔日日盤收)/隔日日盤收*100",
            "range_standardization": "高低點至收盤距離/訊號日含當日往回30日平均日盤振幅*100",
            "direction_hit": "核心多且隔日收盤報酬>0，或核心空且隔日收盤報酬<0",
            "no_trade_assumption": "本程式只做條件統計，不模擬進場、停損、停利或交易成本",
        },
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return paths


def load_config(path: Path) -> tuple[Settings, Path]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    settings = Settings(
        start=date.fromisoformat(raw.get("start", "2018-01-01")),
        end=date.fromisoformat(raw.get("end", "2099-12-31")),
        futures_id=raw.get("futures_id", "TX"),
        range_days=int(raw.get("range_days", 30)),
        minimum_sample_size=int(raw.get("minimum_sample_size", 100)),
    )
    return settings, Path(raw.get("output_dir", "backtest_output/futures_next_day_session_statistics"))


def main() -> None:
    parser = argparse.ArgumentParser(description="TX 隔日日盤收盤與高低點相對收盤分層統計")
    parser.add_argument(
        "--config", type=Path,
        default=Path("backtest/configs/futures_next_day_session_statistics_v1.json"),
    )
    args = parser.parse_args()
    settings, output_dir = load_config(args.config)
    detail = load_detail(settings)
    paths = export_reports(detail, settings, output_dir)
    print(summarize(detail, ["core_direction"], settings.minimum_sample_size).to_string(index=False))
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
