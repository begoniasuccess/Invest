from __future__ import annotations

import argparse
import json
import math
import uuid
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu, spearmanr

from backtest.futures_raw_signal_strength_scan import (
    SOURCE_VIEW,
    Settings,
    _bh_adjust,
    _db_value,
    _hedges_g,
    load_source,
    prepare_detail,
)


ANALYSIS_NAME = "futures_volatility_strength_baseline"
METRIC_VERSION = "1.0.0"
SCORE_RULES = {
    "布林通道位置": {"operator": "<=", "threshold": 20.0, "condition": "布林低位"},
    "散戶多空比": {"operator": ">=", "threshold": 80.0, "condition": "散戶比偏高"},
    "MA10斜率": {"operator": "<=", "threshold": 20.0, "condition": "MA10斜率偏低"},
    "主力淨多空_OI比": {"operator": "<=", "threshold": 20.0, "condition": "主力淨多空OI偏低"},
}
LEVEL_ORDER = ("低波動", "中波動", "高波動")
OUTCOME_COLUMNS = [
    "隔日開收報酬_pct", "隔日上行MFE_pct", "隔日下行MFE_pct",
    "隔日日內振幅_pct", "隔日收盤位置_pct",
    "隔日開盤跳空_日收_pct", "隔日開盤跳空_夜收_pct",
]


def _level_from_score(score: object) -> object:
    if pd.isna(score):
        return pd.NA
    value = int(score)
    if value <= 1:
        return LEVEL_ORDER[0]
    if value == 2:
        return LEVEL_ORDER[1]
    return LEVEL_ORDER[2]


def build_daily_score(detail: pd.DataFrame) -> pd.DataFrame:
    """將四個代表性訊號組成完整樣本的0至4分波動強度。"""
    required = list(SCORE_RULES)
    selected = detail[detail["指標代碼"].isin(required)].copy()
    percentile = selected.pivot(index="訊號日", columns="指標代碼", values="歷史分位_pct")
    raw = selected.pivot(index="訊號日", columns="指標代碼", values="原始值")
    missing = [name for name in required if name not in percentile.columns]
    if missing:
        raise ValueError(f"缺少波動分數指標：{', '.join(missing)}")

    common = detail.drop_duplicates("訊號日").set_index("訊號日")
    daily = pd.DataFrame(index=percentile.index)
    daily["隔日日期"] = common["隔日日期"]
    condition_columns: list[str] = []
    for signal, rule in SCORE_RULES.items():
        daily[f"{signal}_原始值"] = raw[signal]
        daily[f"{signal}_歷史分位_pct"] = percentile[signal]
        available = percentile[signal].notna()
        met = percentile[signal].le(rule["threshold"]) if rule["operator"] == "<=" else percentile[signal].ge(rule["threshold"])
        condition = pd.Series(pd.NA, index=daily.index, dtype="Int64")
        condition.loc[available] = met.loc[available].astype(int)
        column = f"條件_{rule['condition']}"
        daily[column] = condition
        condition_columns.append(column)

    daily["有效條件數"] = daily[condition_columns].notna().sum(axis=1)
    complete = daily["有效條件數"].eq(len(condition_columns))
    score = daily[condition_columns].fillna(0).sum(axis=1).astype("Int64")
    daily["波動分數"] = score.where(complete, pd.NA)
    daily["波動等級"] = daily["波動分數"].map(_level_from_score)
    for column in OUTCOME_COLUMNS:
        daily[column] = common[column]
    daily = daily.reset_index().rename(columns={"訊號日": "訊號日"})
    daily["訊號日"] = pd.to_datetime(daily["訊號日"])
    daily["隔日日期"] = pd.to_datetime(daily["隔日日期"])
    return daily.sort_values("訊號日").reset_index(drop=True)


def _summary_row(group: pd.DataFrame) -> dict[str, object]:
    close = pd.to_numeric(group["隔日開收報酬_pct"], errors="coerce")
    up = pd.to_numeric(group["隔日上行MFE_pct"], errors="coerce")
    down = pd.to_numeric(group["隔日下行MFE_pct"], errors="coerce")
    intraday_range = pd.to_numeric(group["隔日日內振幅_pct"], errors="coerce")
    row: dict[str, object] = {
        "樣本數": int(intraday_range.notna().sum()),
        "隔日日內振幅平均_pct": intraday_range.mean(),
        "隔日日內振幅中位數_pct": intraday_range.median(),
        "隔日日內振幅標準差_pct": intraday_range.std(ddof=1),
        "隔日上行MFE平均_pct": up.mean(),
        "隔日下行MFE平均_pct": down.mean(),
        "隔日開收平均_pct": close.mean(),
        "隔日上漲機率_pct": (close > 0).mean() * 100.0,
        "隔日收盤位置平均_pct": pd.to_numeric(group["隔日收盤位置_pct"], errors="coerce").mean(),
    }
    for threshold in (1.0, 1.5, 2.0):
        row[f"振幅至少{threshold:.1f}pct_機率"] = (intraday_range >= threshold).mean() * 100.0
    for threshold in (0.5, 0.75, 1.0):
        row[f"上行至少{threshold:.2f}pct_機率"] = (up >= threshold).mean() * 100.0
        row[f"下行至少{threshold:.2f}pct_機率"] = (down >= threshold).mean() * 100.0
    return row


def _period_parts(daily: pd.DataFrame, split_date: date) -> list[tuple[str, pd.DataFrame]]:
    signal_date = pd.to_datetime(daily["訊號日"]).dt.date
    return [
        ("全期", daily),
        ("探索期", daily[signal_date < split_date]),
        ("驗證期", daily[signal_date >= split_date]),
    ]


def build_summary(daily: pd.DataFrame, split_date: date) -> pd.DataFrame:
    complete = daily.dropna(subset=["波動分數", "隔日日內振幅_pct"]).copy()
    rows: list[dict[str, object]] = []
    for period, period_data in _period_parts(complete, split_date):
        for score, group in period_data.groupby("波動分數", sort=True):
            rows.append({"期間": period, "分組方式": "分數", "分組": str(int(score)), **_summary_row(group)})
        for level in LEVEL_ORDER:
            group = period_data[period_data["波動等級"] == level]
            if not group.empty:
                rows.append({"期間": period, "分組方式": "等級", "分組": level, **_summary_row(group)})
    return pd.DataFrame(rows)


def build_yearly_summary(daily: pd.DataFrame, minimum_size: int) -> pd.DataFrame:
    complete = daily.dropna(subset=["波動分數", "隔日日內振幅_pct"]).copy()
    complete["年"] = pd.to_datetime(complete["隔日日期"]).dt.year
    rows: list[dict[str, object]] = []
    for (year, level), group in complete.groupby(["年", "波動等級"], sort=True):
        if len(group) >= minimum_size:
            rows.append({"年": int(year), "波動等級": level, **_summary_row(group)})
    return pd.DataFrame(rows)


def build_tests(daily: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    complete = daily.dropna(subset=["波動分數", "隔日日內振幅_pct"]).copy()
    rows: list[dict[str, object]] = []
    for period, group in _period_parts(complete, settings.split_date):
        if len(group) < settings.minimum_group_size * 2:
            continue
        rho, p_value = spearmanr(group["波動分數"], group["隔日日內振幅_pct"])
        rows.append({
            "期間": period, "檢定類型": "Spearman分數趨勢", "樣本數": len(group),
            "低波動樣本數": math.nan, "高波動樣本數": math.nan,
            "統計值": rho, "效果值": rho, "效果方向": "正向" if rho > 0 else "負向",
            "效果量": abs(rho), "p值": p_value,
            "低波動平均_pct": math.nan, "高波動平均_pct": math.nan,
        })
        low = group.loc[group["波動等級"] == "低波動", "隔日日內振幅_pct"].dropna()
        high = group.loc[group["波動等級"] == "高波動", "隔日日內振幅_pct"].dropna()
        if min(len(low), len(high)) >= settings.minimum_group_size:
            statistic, p_value = mannwhitneyu(high, low, alternative="two-sided")
            difference = high.mean() - low.mean()
            rows.append({
                "期間": period, "檢定類型": "高波動對低波動", "樣本數": len(low) + len(high),
                "低波動樣本數": len(low), "高波動樣本數": len(high),
                "統計值": statistic, "效果值": difference,
                "效果方向": "高波動較高" if difference > 0 else "低波動較高",
                "效果量": abs(_hedges_g(high, low)), "p值": p_value,
                "低波動平均_pct": low.mean(), "高波動平均_pct": high.mean(),
            })
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result["FDR_q值"] = _bh_adjust(result["p值"])
    result["FDR顯著"] = result["FDR_q值"] < settings.alpha
    return result


SUMMARY_RENAME = {
    "樣本數": "sample_count", "隔日日內振幅平均_pct": "range_mean_pct",
    "隔日日內振幅中位數_pct": "range_median_pct", "隔日日內振幅標準差_pct": "range_std_pct",
    "隔日上行MFE平均_pct": "up_mfe_mean_pct", "隔日下行MFE平均_pct": "down_mfe_mean_pct",
    "隔日開收平均_pct": "open_close_mean_pct", "隔日上漲機率_pct": "up_probability_pct",
    "隔日收盤位置平均_pct": "close_location_mean_pct",
    "振幅至少1.0pct_機率": "range_ge_100_probability_pct",
    "振幅至少1.5pct_機率": "range_ge_150_probability_pct",
    "振幅至少2.0pct_機率": "range_ge_200_probability_pct",
    "上行至少0.50pct_機率": "up_ge_050_probability_pct",
    "下行至少0.50pct_機率": "down_ge_050_probability_pct",
    "上行至少0.75pct_機率": "up_ge_075_probability_pct",
    "下行至少0.75pct_機率": "down_ge_075_probability_pct",
    "上行至少1.00pct_機率": "up_ge_100_probability_pct",
    "下行至少1.00pct_機率": "down_ge_100_probability_pct",
}
SUMMARY_METRIC_COLUMNS = list(SUMMARY_RENAME.values())


def _records(frame: pd.DataFrame, columns: list[str]) -> list[tuple[Any, ...]]:
    return [tuple(_db_value(value) for value in row) for row in frame[columns].itertuples(index=False, name=None)]


def _insert_rows(cursor: Any, table: str, columns: list[str], frame: pd.DataFrame) -> None:
    rows = _records(frame, columns)
    if rows:
        cursor.executemany(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
            rows,
        )


def build_database_frames(
    run_id: uuid.UUID,
    daily: pd.DataFrame,
    summary: pd.DataFrame,
    yearly: pd.DataFrame,
    tests: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    daily_db = daily.rename(columns={
        "訊號日": "signal_date", "隔日日期": "result_date",
        "布林通道位置_原始值": "bollinger_position_raw",
        "布林通道位置_歷史分位_pct": "bollinger_position_percentile_pct",
        "散戶多空比_原始值": "retail_ratio_raw",
        "散戶多空比_歷史分位_pct": "retail_ratio_percentile_pct",
        "MA10斜率_原始值": "ma10_slope_raw", "MA10斜率_歷史分位_pct": "ma10_slope_percentile_pct",
        "主力淨多空_OI比_原始值": "main_net_oi_raw",
        "主力淨多空_OI比_歷史分位_pct": "main_net_oi_percentile_pct",
        "條件_布林低位": "bollinger_low_flag", "條件_散戶比偏高": "retail_high_flag",
        "條件_MA10斜率偏低": "ma10_low_flag", "條件_主力淨多空OI偏低": "main_net_oi_low_flag",
        "有效條件數": "available_condition_count", "波動分數": "volatility_score",
        "波動等級": "volatility_level", "隔日開收報酬_pct": "next_open_close_return_pct",
        "隔日上行MFE_pct": "next_up_mfe_pct", "隔日下行MFE_pct": "next_down_mfe_pct",
        "隔日日內振幅_pct": "next_intraday_range_pct", "隔日收盤位置_pct": "next_close_location_pct",
        "隔日開盤跳空_日收_pct": "next_open_gap_day_close_pct",
        "隔日開盤跳空_夜收_pct": "next_open_gap_night_close_pct",
    }).copy()
    daily_db.insert(0, "run_id", run_id)

    summary_db = summary.rename(columns={"期間": "period_name", "分組方式": "group_type", "分組": "group_label", **SUMMARY_RENAME}).copy()
    summary_db.insert(0, "run_id", run_id)
    yearly_db = yearly.rename(columns={"年": "result_year", "波動等級": "volatility_level", **SUMMARY_RENAME}).copy()
    yearly_db.insert(0, "run_id", run_id)
    test_db = tests.rename(columns={
        "期間": "period_name", "檢定類型": "test_type", "樣本數": "sample_count",
        "低波動樣本數": "low_count", "高波動樣本數": "high_count", "統計值": "statistic",
        "效果值": "effect_value", "效果方向": "effect_direction", "效果量": "effect_size",
        "p值": "p_value", "低波動平均_pct": "low_mean_pct", "高波動平均_pct": "high_mean_pct",
        "FDR_q值": "fdr_q_value", "FDR顯著": "fdr_significant",
    }).copy()
    test_db.insert(0, "run_id", run_id)
    return daily_db, summary_db, yearly_db, test_db


def save_to_database(
    run_id: uuid.UUID,
    daily: pd.DataFrame,
    summary: pd.DataFrame,
    yearly: pd.DataFrame,
    tests: pd.DataFrame,
    settings: Settings,
    source_rows: int,
) -> None:
    from common.db import get_connection

    daily_db, summary_db, yearly_db, test_db = build_database_frames(run_id, daily, summary, yearly, tests)
    daily_columns = ["run_id"] + [column for column in daily_db.columns if column != "run_id"]
    summary_columns = ["run_id", "period_name", "group_type", "group_label", *SUMMARY_METRIC_COLUMNS]
    yearly_columns = ["run_id", "result_year", "volatility_level", *SUMMARY_METRIC_COLUMNS]
    test_columns = [
        "run_id", "period_name", "test_type", "sample_count", "low_count", "high_count",
        "statistic", "effect_value", "effect_direction", "effect_size", "p_value",
        "low_mean_pct", "high_mean_pct", "fdr_q_value", "fdr_significant",
    ]
    tables = (
        "research.futures_volatility_strength_daily",
        "research.futures_volatility_strength_summary",
        "research.futures_volatility_strength_yearly",
        "research.futures_volatility_strength_test",
    )
    complete_count = int(daily["波動分數"].notna().sum())
    parameters = {
        "score_rules": SCORE_RULES,
        "level_rule": {"低波動": "0-1", "中波動": "2", "高波動": "3-4"},
        "split_date": settings.split_date.isoformat(),
        "history_window": settings.history_window,
        "minimum_history": settings.minimum_history,
        "minimum_group_size": settings.minimum_group_size,
        "minimum_year_size": settings.minimum_year_size,
        "alpha": settings.alpha,
    }
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for table in tables:
                cursor.execute("SELECT to_regclass(%s)", (table,))
                if cursor.fetchone()[0] is None:
                    raise RuntimeError(
                        "尚未建立波動強度研究表；請先執行 "
                        "db_migrate/20261020_add_futures_volatility_strength.sql"
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
                    run_id, ANALYSIS_NAME, settings.futures_id, settings.start,
                    settings.end or pd.to_datetime(daily["訊號日"]).max().date(),
                    json.dumps(parameters, ensure_ascii=False), METRIC_VERSION, SOURCE_VIEW,
                    source_rows, complete_count, max(source_rows - complete_count, 0),
                    json.dumps({"daily_rows": len(daily), "complete_score_rows": complete_count}, ensure_ascii=False),
                ),
            )
            _insert_rows(cursor, tables[0], daily_columns, daily_db)
            _insert_rows(cursor, tables[1], summary_columns, summary_db)
            _insert_rows(cursor, tables[2], yearly_columns, yearly_db)
            _insert_rows(cursor, tables[3], test_columns, test_db)
            cursor.execute(
                "UPDATE research.analysis_run SET status='completed', finished_at=CURRENT_TIMESTAMP WHERE run_id=%s",
                (run_id,),
            )
        conn.commit()


def export_results(
    run_id: uuid.UUID,
    daily: pd.DataFrame,
    summary: pd.DataFrame,
    yearly: pd.DataFrame,
    tests: pd.DataFrame,
    settings: Settings,
    output_dir: Path,
) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{settings.start:%Y%m%d}_{settings.end:%Y%m%d}" if settings.end else f"{settings.start:%Y%m%d}_latest"
    paths = (
        output_dir / f"volatility_strength_daily_{suffix}.csv",
        output_dir / f"volatility_strength_summary_{suffix}.csv",
        output_dir / f"volatility_strength_yearly_{suffix}.csv",
        output_dir / f"volatility_strength_tests_{suffix}.csv",
        output_dir / f"volatility_strength_manifest_{suffix}.json",
    )
    for frame, path in zip((daily, summary, yearly, tests), paths[:4]):
        frame.to_csv(path, index=False, encoding="utf-8-sig")
    paths[4].write_text(json.dumps({
        "run_id": str(run_id), "analysis_name": ANALYSIS_NAME, "metric_version": METRIC_VERSION,
        "settings": {**asdict(settings), "start": settings.start.isoformat(),
                     "end": settings.end.isoformat() if settings.end else None,
                     "split_date": settings.split_date.isoformat()},
        "source_view": SOURCE_VIEW, "score_rules": SCORE_RULES,
        "level_rule": {"低波動": "0-1分", "中波動": "2分", "高波動": "3-4分"},
        "limitations": [
            "此分數是在第一階段結果後選出的探索性基準，不是完全未觀察樣本外模型",
            "分數只預測波動空間，不預測多空方向",
            "MFE來自日K，不能判定停利與停損的盤中先後順序",
        ],
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="隔日台指期日盤波動強度0至4分基準回測")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--split-date", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--history-window", type=int, default=252)
    parser.add_argument("--minimum-history", type=int, default=60)
    parser.add_argument("--minimum-group-size", type=int, default=30)
    parser.add_argument("--minimum-year-size", type=int, default=10)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--winsor-tail", type=float, default=0.005)
    parser.add_argument("--output-dir", type=Path, default=Path("backtest_output/futures_volatility_strength"))
    parser.add_argument("--save-db", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(
        start=args.start, end=args.end, futures_id=args.futures_id, split_date=args.split_date,
        history_window=args.history_window, minimum_history=args.minimum_history,
        minimum_group_size=args.minimum_group_size, minimum_year_size=args.minimum_year_size,
        alpha=args.alpha, winsor_tail=args.winsor_tail,
    )
    source = load_source(settings)
    detail, _ = prepare_detail(source, settings)
    daily = build_daily_score(detail)
    summary = build_summary(daily, settings.split_date)
    yearly = build_yearly_summary(daily, settings.minimum_year_size)
    tests = build_tests(daily, settings)
    run_id = uuid.uuid4()
    paths = export_results(run_id, daily, summary, yearly, tests, settings, args.output_dir)
    if args.save_db:
        save_to_database(run_id, daily, summary, yearly, tests, settings, len(source))
    complete = daily["波動分數"].notna()
    print(f"完整波動分數樣本：{int(complete.sum()):,} / {len(daily):,} 日")
    print(f"研究 run_id：{run_id}")
    print(f"資料庫保存：{'完成' if args.save_db else '未啟用（加 --save-db 可保存）'}")
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
