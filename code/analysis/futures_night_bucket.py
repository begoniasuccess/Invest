from __future__ import annotations

import argparse
import json
import math
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "analysis" / "configs" / "futures_night_bucket_v1.json"
ANALYSIS_NAME = "futures_night_bucket_v1"
METRIC_VERSION = "1.0.0"
SOURCE_RELATION = "report.v_futures_report_daily_corrected"

METRICS = {
    "night_return_pct": "夜盤漲跌幅",
    "next_open_return_pct": "隔日開盤漲跌幅",
    "next_close_return_pct": "隔日收盤漲跌幅",
    "next_intraday_return_pct": "隔日日內漲跌幅",
    "next_range_prev_close_pct": "隔日最大振幅（昨收）",
    "next_range_open_pct": "隔日最大振幅（開盤）",
    "night_close_to_next_open_pct": "隔日開盤相對夜收",
    "max_up_from_open_pct": "隔日最大上漲幅",
    "max_down_from_open_pct": "隔日最大下跌幅",
    "close_location_pct": "隔日收盤位置",
}

SOURCE_SQL = """
WITH ordered AS (
    SELECT
        r."日期" AS group_date,
        r."日盤收" AS base_day_close,
        r."夜盤開" AS night_open,
        r."夜盤高" AS night_high,
        r."夜盤低" AS night_low,
        r."夜盤收" AS night_close,
        r."夜盤資料來源" AS night_source,
        LEAD(r."日期") OVER w AS result_date,
        LEAD(r."日盤開") OVER w AS result_day_open,
        LEAD(r."日盤高") OVER w AS result_day_high,
        LEAD(r."日盤低") OVER w AS result_day_low,
        LEAD(r."日盤收") OVER w AS result_day_close
    FROM report.v_futures_report_daily_corrected AS r
    WHERE r.futures_id = ?
    WINDOW w AS (PARTITION BY r.futures_id ORDER BY r."日期")
)
SELECT *
FROM ordered
WHERE group_date BETWEEN ? AND ?
ORDER BY group_date
"""

REQUIRED_COLUMNS = [
    "group_date", "result_date", "base_day_close", "night_close",
    "result_day_open", "result_day_high", "result_day_low", "result_day_close",
]

DAILY_DB_COLUMNS = [
    "run_id", "futures_id", "group_date", "result_date", "bucket_id", "bucket_label",
    "base_day_close", "night_open", "night_high", "night_low", "night_close",
    "result_day_open", "result_day_high", "result_day_low", "result_day_close",
    "night_return_points", "night_return_pct", "next_open_return_points",
    "next_open_return_pct", "next_close_return_points", "next_close_return_pct",
    "next_intraday_return_points", "next_intraday_return_pct", "next_range_points",
    "next_range_prev_close_pct", "next_range_open_pct",
    "night_close_to_next_open_points", "night_close_to_next_open_pct",
    "max_up_from_open_points", "max_up_from_open_pct", "max_down_from_open_points",
    "max_down_from_open_pct", "close_location_pct", "next_day_direction",
    "direction_continued", "direction_reversed", "filled_base_day_close", "data_quality",
]

SUMMARY_DB_COLUMNS = [
    "run_id", "bucket_id", "bucket_label", "metric_name", "sample_count",
    "mean_value", "median_value", "std_value", "min_value", "q10_value",
    "q25_value", "q75_value", "q90_value", "max_value", "positive_count",
    "positive_rate_pct",
]

YEARLY_DB_COLUMNS = [
    "run_id", "result_year", "bucket_id", "bucket_label", "metric_name",
    "sample_count", "mean_value", "median_value", "std_value", "min_value",
    "q10_value", "q25_value", "q75_value", "q90_value", "max_value",
    "positive_count", "positive_rate_pct",
]


@dataclass(frozen=True)
class AnalysisConfig:
    futures_id: str
    start_date: date
    end_date: date
    bucket_edges_pct: tuple[float, ...]
    output_dir: Path
    write_database: bool
    export_csv: bool


def _parse_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} 必須是 YYYY-MM-DD：{value}") from exc


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> AnalysisConfig:
    config_path = Path(path)
    if not config_path.is_absolute():
        config_path = PROJECT_ROOT / config_path
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    start_date = _parse_date(str(raw["start_date"]), "start_date")
    end_date = _parse_date(str(raw["end_date"]), "end_date")
    if start_date > end_date:
        raise ValueError("start_date 不可晚於 end_date")

    edges = tuple(float(value) for value in raw.get("bucket_edges_pct", []))
    if len(edges) != 9 or tuple(sorted(edges)) != edges or len(set(edges)) != len(edges):
        raise ValueError("bucket_edges_pct 必須提供 9 個由小到大且不重複的邊界")

    output_dir = Path(raw.get("output_dir", "../output/analysis/night_bucket"))
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir
    return AnalysisConfig(
        futures_id=str(raw.get("futures_id", "TX")),
        start_date=start_date,
        end_date=end_date,
        bucket_edges_pct=edges,
        output_dir=output_dir,
        write_database=bool(raw.get("write_database", True)),
        export_csv=bool(raw.get("export_csv", True)),
    )


def load_source(config: AnalysisConfig) -> pd.DataFrame:
    from common.db import sql_to_df

    return sql_to_df(
        SOURCE_SQL,
        params=(config.futures_id, config.start_date, config.end_date),
        parse_dates=["group_date", "result_date"],
    )


def _format_edge(value: float) -> str:
    return f"{value:g}%"


def build_bucket_labels(bucket_edges_pct: Iterable[float]) -> dict[int, str]:
    edges = [float(value) for value in bucket_edges_pct]
    labels = {1: f"01 小於 {_format_edge(edges[0])}"}
    for index, (lower, upper) in enumerate(zip(edges, edges[1:]), start=2):
        labels[index] = (
            f"{index:02d} [{_format_edge(lower)}, {_format_edge(upper)})"
        )
    labels[10] = f"10 大於等於 {_format_edge(edges[-1])}"
    return labels


def build_daily_features(
    source: pd.DataFrame,
    bucket_edges_pct: Iterable[float],
) -> tuple[pd.DataFrame, dict[str, int]]:
    frame = source.copy()
    source_rows = len(frame)
    for column in REQUIRED_COLUMNS:
        if column not in frame.columns:
            raise ValueError(f"來源資料缺少欄位：{column}")

    numeric_columns = [
        "base_day_close", "night_open", "night_high", "night_low", "night_close",
        "result_day_open", "result_day_high", "result_day_low", "result_day_close",
    ]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    invalid_reasons = pd.DataFrame(index=frame.index)
    invalid_reasons["missing_night_close"] = frame["night_close"].isna()
    invalid_reasons["missing_next_day"] = frame["result_date"].isna()
    invalid_reasons["missing_next_day_ohlc"] = frame[
        ["result_day_open", "result_day_high", "result_day_low", "result_day_close"]
    ].isna().any(axis=1)
    invalid_reasons["bad_base_close"] = frame["base_day_close"].isna() | (frame["base_day_close"] <= 0)
    invalid_reasons["bad_next_open"] = frame["result_day_open"].notna() & (frame["result_day_open"] <= 0)

    valid_mask = ~invalid_reasons.any(axis=1)
    exclusion_summary = {
        column: int(mask.sum()) for column, mask in invalid_reasons.items() if int(mask.sum()) > 0
    }
    frame = frame.loc[valid_mask].copy()
    if frame.empty:
        raise ValueError("所選期間沒有完整的夜盤 D -> 隔日日盤 T 配對")

    base = frame["base_day_close"]
    day_open = frame["result_day_open"]
    day_high = frame["result_day_high"]
    day_low = frame["result_day_low"]
    day_close = frame["result_day_close"]
    night_close = frame["night_close"]

    frame["night_return_points"] = night_close - base
    frame["night_return_pct"] = frame["night_return_points"] / base * 100.0
    frame["next_open_return_points"] = day_open - base
    frame["next_open_return_pct"] = frame["next_open_return_points"] / base * 100.0
    frame["next_close_return_points"] = day_close - base
    frame["next_close_return_pct"] = frame["next_close_return_points"] / base * 100.0
    frame["next_intraday_return_points"] = day_close - day_open
    frame["next_intraday_return_pct"] = frame["next_intraday_return_points"] / day_open * 100.0
    frame["next_range_points"] = day_high - day_low
    frame["next_range_prev_close_pct"] = frame["next_range_points"] / base * 100.0
    frame["next_range_open_pct"] = frame["next_range_points"] / day_open * 100.0
    frame["night_close_to_next_open_points"] = day_open - night_close
    frame["night_close_to_next_open_pct"] = frame["night_close_to_next_open_points"] / night_close * 100.0
    frame["max_up_from_open_points"] = day_high - day_open
    frame["max_up_from_open_pct"] = frame["max_up_from_open_points"] / day_open * 100.0
    frame["max_down_from_open_points"] = day_low - day_open
    frame["max_down_from_open_pct"] = frame["max_down_from_open_points"] / day_open * 100.0

    range_points = frame["next_range_points"]
    frame["close_location_pct"] = np.where(
        range_points > 0,
        (day_close - day_low) / range_points * 100.0,
        50.0,
    )
    frame["next_day_direction"] = np.sign(frame["next_close_return_points"]).astype("int8")
    night_direction = np.sign(frame["night_return_points"])
    comparable = (night_direction != 0) & (frame["next_day_direction"] != 0)
    frame["direction_continued"] = np.where(
        comparable, night_direction == frame["next_day_direction"], pd.NA
    )
    frame["direction_reversed"] = np.where(
        comparable, night_direction != frame["next_day_direction"], pd.NA
    )
    frame["filled_base_day_close"] = (day_low <= base) & (day_high >= base)

    raw_edges = [float(value) for value in bucket_edges_pct]
    bucket_labels = build_bucket_labels(raw_edges)
    edges = [-math.inf, *raw_edges, math.inf]
    frame["bucket_id"] = pd.cut(
        frame["night_return_pct"], bins=edges, labels=range(1, 11), right=False
    ).astype("int8")
    frame["bucket_label"] = frame["bucket_id"].map(bucket_labels)
    frame["data_quality"] = "valid"

    diagnostics = {
        "source_rows": source_rows,
        "valid_rows": len(frame),
        "excluded_rows": source_rows - len(frame),
        **exclusion_summary,
    }
    return frame.reset_index(drop=True), diagnostics


def _metric_rows(grouped: Any, *, year_column: bool) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for keys, group in grouped:
        if year_column:
            result_year, bucket_id = keys
        else:
            result_year, bucket_id = None, keys
        bucket_id = int(bucket_id)
        for metric_name in METRICS:
            values = pd.to_numeric(group[metric_name], errors="coerce").dropna()
            sample_count = int(len(values))
            positive_count = int((values > 0).sum())
            row = {
                "bucket_id": bucket_id,
                "bucket_label": str(group["bucket_label"].iloc[0]),
                "metric_name": metric_name,
                "sample_count": sample_count,
                "mean_value": values.mean() if sample_count else np.nan,
                "median_value": values.median() if sample_count else np.nan,
                "std_value": values.std(ddof=1) if sample_count > 1 else np.nan,
                "min_value": values.min() if sample_count else np.nan,
                "q10_value": values.quantile(0.10) if sample_count else np.nan,
                "q25_value": values.quantile(0.25) if sample_count else np.nan,
                "q75_value": values.quantile(0.75) if sample_count else np.nan,
                "q90_value": values.quantile(0.90) if sample_count else np.nan,
                "max_value": values.max() if sample_count else np.nan,
                "positive_count": positive_count,
                "positive_rate_pct": positive_count / sample_count * 100.0 if sample_count else np.nan,
            }
            if year_column:
                row["result_year"] = int(result_year)
            rows.append(row)
    return rows


def build_summaries(daily: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary = pd.DataFrame(_metric_rows(daily.groupby("bucket_id", sort=True), year_column=False))
    with_year = daily.assign(result_year=daily["result_date"].dt.year.astype(int))
    yearly = pd.DataFrame(
        _metric_rows(with_year.groupby(["result_year", "bucket_id"], sort=True), year_column=True)
    )
    return summary, yearly


def _db_value(value: Any) -> Any:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, np.generic):
        value = value.item()
    try:
        return None if pd.isna(value) else value
    except (TypeError, ValueError):
        return value


def _records(frame: pd.DataFrame, columns: list[str]) -> list[tuple[Any, ...]]:
    return [tuple(_db_value(row[column]) for column in columns) for _, row in frame.iterrows()]


def _insert_rows(cursor: Any, table: str, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(columns))
    cursor.executemany(
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
        rows,
    )


def save_to_database(
    run_id: uuid.UUID,
    config: AnalysisConfig,
    daily: pd.DataFrame,
    summary: pd.DataFrame,
    yearly: pd.DataFrame,
    diagnostics: dict[str, int],
) -> None:
    from common.db import get_connection

    exclusion_summary = {
        key: value for key, value in diagnostics.items()
        if key not in {"source_rows", "valid_rows", "excluded_rows"}
    }
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT to_regclass('research.analysis_run')")
            if cursor.fetchone()[0] is None:
                raise RuntimeError(
                    "尚未建立 research 資料表；請先執行 "
                    "db_migrate/20260913_create_research_night_bucket_schema.sql"
                )
            cursor.execute(
                """
                INSERT INTO research.analysis_run (
                    run_id, analysis_name, futures_id, start_date, end_date,
                    bucket_edges, metric_version, source_relation, status,
                    source_rows, valid_rows, excluded_rows, exclusion_summary
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, 'running', %s, %s, %s, %s::jsonb)
                """,
                (
                    run_id, ANALYSIS_NAME, config.futures_id, config.start_date, config.end_date,
                    json.dumps(config.bucket_edges_pct), METRIC_VERSION, SOURCE_RELATION,
                    diagnostics["source_rows"], diagnostics["valid_rows"], diagnostics["excluded_rows"],
                    json.dumps(exclusion_summary, ensure_ascii=False),
                ),
            )

            daily_db = daily.copy()
            daily_db.insert(0, "futures_id", config.futures_id)
            daily_db.insert(0, "run_id", run_id)
            summary_db = summary.copy()
            summary_db.insert(0, "run_id", run_id)
            yearly_db = yearly.copy()
            yearly_db.insert(0, "run_id", run_id)

            _insert_rows(
                cursor, "research.futures_night_bucket_daily", DAILY_DB_COLUMNS,
                _records(daily_db, DAILY_DB_COLUMNS),
            )
            _insert_rows(
                cursor, "research.futures_night_bucket_summary", SUMMARY_DB_COLUMNS,
                _records(summary_db, SUMMARY_DB_COLUMNS),
            )
            _insert_rows(
                cursor, "research.futures_night_bucket_yearly", YEARLY_DB_COLUMNS,
                _records(yearly_db, YEARLY_DB_COLUMNS),
            )
            cursor.execute(
                """
                UPDATE research.analysis_run
                SET status = 'completed', finished_at = CURRENT_TIMESTAMP
                WHERE run_id = %s
                """,
                (run_id,),
            )
        conn.commit()


def export_csv_files(
    run_id: uuid.UUID,
    config: AnalysisConfig,
    daily: pd.DataFrame,
    summary: pd.DataFrame,
    yearly: pd.DataFrame,
    diagnostics: dict[str, int],
) -> list[Path]:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{config.start_date:%Y%m%d}_{config.end_date:%Y%m%d}"
    outputs = [
        config.output_dir / f"futures_night_bucket_daily_{suffix}.csv",
        config.output_dir / f"futures_night_bucket_summary_{suffix}.csv",
        config.output_dir / f"futures_night_bucket_yearly_{suffix}.csv",
    ]
    daily.to_csv(outputs[0], index=False, encoding="utf-8-sig")
    summary.assign(metric_label=summary["metric_name"].map(METRICS)).to_csv(
        outputs[1], index=False, encoding="utf-8-sig"
    )
    yearly.assign(metric_label=yearly["metric_name"].map(METRICS)).to_csv(
        outputs[2], index=False, encoding="utf-8-sig"
    )
    manifest_path = config.output_dir / f"futures_night_bucket_manifest_{suffix}.json"
    manifest_path.write_text(
        json.dumps({
            "run_id": str(run_id),
            "analysis_name": ANALYSIS_NAME,
            "metric_version": METRIC_VERSION,
            "futures_id": config.futures_id,
            "start_date": config.start_date.isoformat(),
            "end_date": config.end_date.isoformat(),
            "bucket_edges_pct": config.bucket_edges_pct,
            "metrics": METRICS,
            "diagnostics": diagnostics,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    outputs.append(manifest_path)
    return outputs


def run_analysis(config: AnalysisConfig) -> dict[str, Any]:
    run_id = uuid.uuid4()
    source = load_source(config)
    daily, diagnostics = build_daily_features(source, config.bucket_edges_pct)
    summary, yearly = build_summaries(daily)
    outputs: list[Path] = []
    if config.write_database:
        save_to_database(run_id, config, daily, summary, yearly, diagnostics)
    if config.export_csv:
        outputs = export_csv_files(run_id, config, daily, summary, yearly, diagnostics)
    return {
        "run_id": str(run_id),
        "diagnostics": diagnostics,
        "outputs": [str(path) for path in outputs],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="台指期夜盤漲跌幅十組分層分析")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="JSON 設定檔")
    parser.add_argument("--start-date", help="覆寫開始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", help="覆寫結束日期 YYYY-MM-DD")
    parser.add_argument("--no-db", action="store_true", help="不寫入 research Schema")
    parser.add_argument("--no-csv", action="store_true", help="不輸出 CSV")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base = load_config(args.config)
    config = AnalysisConfig(
        futures_id=base.futures_id,
        start_date=_parse_date(args.start_date, "start_date") if args.start_date else base.start_date,
        end_date=_parse_date(args.end_date, "end_date") if args.end_date else base.end_date,
        bucket_edges_pct=base.bucket_edges_pct,
        output_dir=base.output_dir,
        write_database=base.write_database and not args.no_db,
        export_csv=base.export_csv and not args.no_csv,
    )
    if config.start_date > config.end_date:
        raise ValueError("開始日期不可晚於結束日期")
    result = run_analysis(config)
    print(f"分析完成：run_id={result['run_id']}")
    print(
        f"來源 {result['diagnostics']['source_rows']} 筆；"
        f"有效 {result['diagnostics']['valid_rows']} 筆；"
        f"排除 {result['diagnostics']['excluded_rows']} 筆"
    )
    for path in result["outputs"]:
        print(f"CSV：{path}")


if __name__ == "__main__":
    main()
