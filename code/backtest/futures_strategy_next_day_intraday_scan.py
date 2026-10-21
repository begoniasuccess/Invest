from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import binomtest, mannwhitneyu, ttest_1samp, ttest_ind, wilcoxon

from backtest.futures_strategy_next_day_field_scan import (
    SOURCE_VIEW,
    Settings,
    _bh_adjust,
    _hedges_g,
    _winsorize_pair,
    detect_strategy_columns,
    load_source,
)


OUTCOME_LABELS = {
    "gap_from_day_close_pct": "隔日開盤相對前日日收",
    "gap_from_night_close_pct": "隔日開盤相對前日夜收",
    "close_from_day_close_pct": "隔日日收相對前日日收",
    "close_from_night_close_pct": "隔日日收相對前日夜收",
    "open_to_close_pct": "隔日日盤開到收",
    "open_to_high_pct": "隔日日開到最高",
    "open_to_low_pct": "隔日日開到最低",
    "night_to_high_pct": "前日夜收到隔日最高",
    "night_to_low_pct": "前日夜收到隔日最低",
    "intraday_range_pct": "隔日日盤振幅",
    "high_to_close_pullback_pct": "隔日最高到收盤回落",
    "low_to_close_rebound_pct": "隔日最低到收盤反彈",
    "close_location_pct": "隔日收盤區間位置",
}
ENTRY_LABELS = {
    "day_open": "隔日日盤開盤",
}


def _safe_pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.astype(float).div(denominator.astype(float).replace(0, np.nan)) * 100.0


def prepare_intraday_data(source: pd.DataFrame) -> pd.DataFrame:
    work = source.sort_values("日期").reset_index(drop=True).copy()
    strategies = detect_strategy_columns(work)
    required = ["日盤收", "夜盤收", "日盤開", "日盤高", "日盤低"]
    for column in required:
        work[column] = pd.to_numeric(work[column], errors="coerce")

    output = work[["日期", *strategies]].copy()
    output["隔日日期"] = work["日期"].shift(-1)
    output["signal_day_close"] = work["日盤收"]
    output["signal_night_close"] = work["夜盤收"]
    output["target_open"] = work["日盤開"].shift(-1)
    output["target_high"] = work["日盤高"].shift(-1)
    output["target_low"] = work["日盤低"].shift(-1)
    output["target_close"] = work["日盤收"].shift(-1)

    day_close = output["signal_day_close"]
    night_close = output["signal_night_close"]
    day_open = output["target_open"]
    day_high = output["target_high"]
    day_low = output["target_low"]
    day_close_next = output["target_close"]

    output["gap_from_day_close_pct"] = _safe_pct(day_open - day_close, day_close)
    output["gap_from_night_close_pct"] = _safe_pct(day_open - night_close, night_close)
    output["close_from_day_close_pct"] = _safe_pct(day_close_next - day_close, day_close)
    output["close_from_night_close_pct"] = _safe_pct(day_close_next - night_close, night_close)
    output["open_to_close_pct"] = _safe_pct(day_close_next - day_open, day_open)
    output["open_to_high_pct"] = _safe_pct(day_high - day_open, day_open)
    output["open_to_low_pct"] = _safe_pct(day_low - day_open, day_open)
    output["night_to_high_pct"] = _safe_pct(day_high - night_close, night_close)
    output["night_to_low_pct"] = _safe_pct(day_low - night_close, night_close)
    output["intraday_range_pct"] = _safe_pct(day_high - day_low, day_open)
    output["high_to_close_pullback_pct"] = _safe_pct(day_high - day_close_next, day_open)
    output["low_to_close_rebound_pct"] = _safe_pct(day_close_next - day_low, day_open)
    output["close_location_pct"] = (day_close_next - day_low).div(
        (day_high - day_low).replace(0, np.nan)
    ) * 100.0
    return output


def _period_group_scan(frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for strategy in detect_strategy_columns(frame):
        direction = frame[strategy].astype("string").str.strip()
        for outcome, label in OUTCOME_LABELS.items():
            values = pd.to_numeric(frame[outcome], errors="coerce")
            long_group = values[direction == "多"].dropna()
            short_group = values[direction == "空"].dropna()
            if min(len(long_group), len(short_group)) < settings.minimum_group_size:
                continue
            long_group, short_group = _winsorize_pair(
                long_group, short_group, settings.winsor_tail
            )
            u_stat, p_value = mannwhitneyu(long_group, short_group, alternative="two-sided")
            _, welch_p = ttest_ind(long_group, short_group, equal_var=False)
            rows.append({
                "策略欄位": strategy, "當沖結果": label, "結果代碼": outcome,
                "多樣本數": len(long_group), "空樣本數": len(short_group),
                "多平均_pct": long_group.mean(), "空平均_pct": short_group.mean(),
                "多減空_pct": long_group.mean() - short_group.mean(),
                "多中位數_pct": long_group.median(), "空中位數_pct": short_group.median(),
                "Hedges_g": _hedges_g(long_group, short_group),
                "Mann_Whitney_U": u_stat, "p值": p_value, "Welch_p值": welch_p,
            })
    result = pd.DataFrame(rows)
    if len(result):
        result["FDR_q值"] = _bh_adjust(result["p值"])
        result["FDR顯著"] = result["FDR_q值"] < settings.alpha
    return result


def scan_group_differences(frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    full = _period_group_scan(frame, settings)
    if full.empty:
        return full
    discovery = _period_group_scan(
        frame[pd.to_datetime(frame["日期"]).dt.date < settings.split_date], settings
    )
    confirmation = _period_group_scan(
        frame[pd.to_datetime(frame["日期"]).dt.date >= settings.split_date], settings
    )
    key = ["策略欄位", "結果代碼"]
    discovery = discovery[key + ["FDR_q值", "多減空_pct"]].rename(columns={
        "FDR_q值": "探索期_q值", "多減空_pct": "探索期_多減空_pct",
    })
    confirmation = confirmation[key + ["p值", "FDR_q值", "多減空_pct"]].rename(columns={
        "p值": "驗證期_p值", "FDR_q值": "驗證期_q值",
        "多減空_pct": "驗證期_多減空_pct",
    })
    result = full.merge(discovery, on=key, how="left").merge(confirmation, on=key, how="left")
    result["前後期同方向"] = (
        np.sign(result["探索期_多減空_pct"]) == np.sign(result["驗證期_多減空_pct"])
    )
    result["重複驗證通過"] = (
        result["FDR顯著"] & result["前後期同方向"] &
        (result["探索期_q值"] < 0.10) & (result["驗證期_p值"] < settings.alpha)
    )
    return result.sort_values(
        ["重複驗證通過", "FDR顯著", "FDR_q值", "Hedges_g"],
        ascending=[False, False, True, False],
    ).reset_index(drop=True)


def _directional_metrics(frame: pd.DataFrame, strategy: str, entry: str) -> pd.DataFrame:
    direction = frame[strategy].astype("string").str.strip()
    sign = direction.map({"多": 1.0, "空": -1.0})
    if entry != "day_open":
        raise ValueError("方向化績效只允許使用隔日日盤開盤作為可執行進場基準")
    entry_price = frame["target_open"]
    close_return = _safe_pct(frame["target_close"] - entry_price, entry_price) * sign
    long_mfe = _safe_pct(frame["target_high"] - entry_price, entry_price)
    short_mfe = _safe_pct(entry_price - frame["target_low"], entry_price)
    long_mae = _safe_pct(entry_price - frame["target_low"], entry_price)
    short_mae = _safe_pct(frame["target_high"] - entry_price, entry_price)
    return pd.DataFrame({
        "日期": frame["日期"], "隔日日期": frame["隔日日期"], "訊號": direction,
        "方向化收盤報酬_pct": close_return,
        "最大有利空間_pct": np.where(direction == "多", long_mfe, short_mfe),
        "最大不利空間_pct": np.where(direction == "多", long_mae, short_mae),
    }).dropna(subset=["訊號"])


def _one_sample_result(values: pd.Series, threshold: float = 0.0) -> tuple[float, float, float]:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if len(clean) < 10:
        return math.nan, math.nan, math.nan
    centered = clean - threshold
    _, t_p = ttest_1samp(clean, popmean=threshold)
    try:
        _, w_p = wilcoxon(centered, alternative="greater")
    except ValueError:
        w_p = 1.0
    return float(clean.mean()), float(t_p), float(w_p)


def _directional_period(frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for strategy in detect_strategy_columns(frame):
        for entry, entry_label in ENTRY_LABELS.items():
            metrics = _directional_metrics(frame, strategy, entry)
            valid = metrics.dropna(subset=["方向化收盤報酬_pct"])
            if len(valid) < settings.minimum_group_size * 2:
                continue
            mean_return, t_p, positive_p = _one_sample_result(valid["方向化收盤報酬_pct"])
            wins = int((valid["方向化收盤報酬_pct"] > 0).sum())
            win_rate = wins / len(valid) * 100.0
            win_p = binomtest(wins, len(valid), 0.5, alternative="greater").pvalue
            rows.append({
                "策略欄位": strategy, "假設進場基準": entry_label, "進場代碼": entry,
                "樣本數": len(valid), "方向化平均收盤報酬_pct": mean_return,
                "方向化中位收盤報酬_pct": valid["方向化收盤報酬_pct"].median(),
                "方向勝率_pct": win_rate, "平均最大有利空間_pct": valid["最大有利空間_pct"].mean(),
                "平均最大不利空間_pct": valid["最大不利空間_pct"].mean(),
                "報酬Welch單樣本_p值": t_p, "報酬Wilcoxon單尾_p值": positive_p,
                "勝率高於50_p值": win_p,
            })
    result = pd.DataFrame(rows)
    if len(result):
        result["報酬FDR_q值"] = _bh_adjust(result["報酬Wilcoxon單尾_p值"])
        result["勝率FDR_q值"] = _bh_adjust(result["勝率高於50_p值"])
    return result


def scan_directional_performance(frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    full = _directional_period(frame, settings)
    if full.empty:
        return full
    discovery = _directional_period(
        frame[pd.to_datetime(frame["日期"]).dt.date < settings.split_date], settings
    )
    confirmation = _directional_period(
        frame[pd.to_datetime(frame["日期"]).dt.date >= settings.split_date], settings
    )
    key = ["策略欄位", "進場代碼"]
    discovery = discovery[key + ["方向化平均收盤報酬_pct", "報酬FDR_q值", "勝率FDR_q值"]].rename(columns={
        "方向化平均收盤報酬_pct": "探索期_平均報酬_pct",
        "報酬FDR_q值": "探索期_報酬q值", "勝率FDR_q值": "探索期_勝率q值",
    })
    confirmation = confirmation[key + ["方向化平均收盤報酬_pct", "報酬Wilcoxon單尾_p值", "勝率高於50_p值"]].rename(columns={
        "方向化平均收盤報酬_pct": "驗證期_平均報酬_pct",
        "報酬Wilcoxon單尾_p值": "驗證期_報酬p值", "勝率高於50_p值": "驗證期_勝率p值",
    })
    result = full.merge(discovery, on=key, how="left").merge(confirmation, on=key, how="left")
    result["前後期皆正報酬"] = (
        (result["探索期_平均報酬_pct"] > 0) & (result["驗證期_平均報酬_pct"] > 0)
    )
    result["方向策略候選"] = (
        result["前後期皆正報酬"] & (result["報酬FDR_q值"] < settings.alpha) &
        (result["探索期_報酬q值"] < 0.10) & (result["驗證期_報酬p值"] < settings.alpha)
    )
    return result.sort_values(
        ["方向策略候選", "報酬FDR_q值", "方向化平均收盤報酬_pct"],
        ascending=[False, True, False],
    ).reset_index(drop=True)


def export_results(group_results: pd.DataFrame, directional: pd.DataFrame,
                   settings: Settings, output_dir: Path) -> tuple[Path, Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    end_label = settings.end.strftime("%Y%m%d") if settings.end else "latest"
    suffix = f"{settings.start:%Y%m%d}_{end_label}"
    group_path = output_dir / f"intraday_group_differences_{suffix}.csv"
    group_short_path = output_dir / f"intraday_group_shortlist_{suffix}.csv"
    direction_path = output_dir / f"intraday_directional_performance_{suffix}.csv"
    manifest_path = output_dir / f"intraday_scan_manifest_{suffix}.json"
    group_results.to_csv(group_path, index=False, encoding="utf-8-sig")
    group_results[group_results["重複驗證通過"]].to_csv(
        group_short_path, index=False, encoding="utf-8-sig"
    )
    directional.to_csv(direction_path, index=False, encoding="utf-8-sig")
    manifest = {
        "settings": {**asdict(settings), "start": settings.start.isoformat(),
                     "end": settings.end.isoformat() if settings.end else None,
                     "split_date": settings.split_date.isoformat()},
        "source_view": SOURCE_VIEW,
        "objective": "尋找隔日台指期日盤當沖可利用的策略訊號",
        "included_outcomes": list(OUTCOME_LABELS.values()),
        "excluded": "隔日均線、法人、選權、籌碼、成交量等不能直接代表當沖損益的欄位",
        "directional_assumption": "多訊號做多、空訊號做空；未扣交易成本與滑價",
        "mfe_mae_warning": "最大有利/不利空間只表示盤中曾到達，不代表可實現損益",
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return group_path, group_short_path, direction_path, manifest_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="策略多空對隔日台指期日盤當沖結果掃描")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--split-date", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--minimum-group-size", type=int, default=100)
    parser.add_argument("--output-dir", type=Path,
                        default=Path("backtest_output/futures_strategy_next_day_intraday_scan"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(
        start=args.start, end=args.end, futures_id=args.futures_id,
        minimum_group_size=args.minimum_group_size, split_date=args.split_date,
    )
    source = load_source(settings)
    prepared = prepare_intraday_data(source)
    if settings.end:
        prepared = prepared[pd.to_datetime(prepared["日期"]).dt.date <= settings.end].copy()
    group_results = scan_group_differences(prepared, settings)
    directional = scan_directional_performance(prepared, settings)
    paths = export_results(group_results, directional, settings, args.output_dir)
    print(f"策略欄位：{len(detect_strategy_columns(prepared))} 個")
    print(f"當沖結果：{len(OUTCOME_LABELS)} 項")
    print(f"多空差異重複驗證通過：{int(group_results['重複驗證通過'].sum())} 組")
    print(f"方向策略候選：{int(directional['方向策略候選'].sum())} 組")
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
