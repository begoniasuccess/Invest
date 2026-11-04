from __future__ import annotations

import argparse
import json
import math
import uuid
from dataclasses import asdict
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu, spearmanr

from backtest.futures_raw_signal_strength_scan import (
    BUCKET_LABELS,
    Settings,
    _clip,
    _numeric,
    _pct,
    _rolling_percentile,
    _safe_ratio,
    _strength_bucket,
    build_signal_features,
    load_source,
)
from backtest.futures_strategy_next_day_field_scan import _bh_adjust, _hedges_g


ANALYSIS_NAME = "futures_direction_signal_strength_scan"
METRIC_VERSION = "1.1.0"
DAY_DIRECTION_OUTCOMES = {
    "隔日開收報酬_pct": "隔日日盤收盤相對開盤報酬；正值偏多",
    "隔日方向效率_pct": "(收盤-開盤)/(最高-最低)*100；正值偏多，並降低振幅大小干擾",
    "隔日收盤位置_pct": "收盤位於日內區間的位置；越高越偏多",
    "隔日多空空間差_pct": "上行MFE-下行MFE；正值表示多方盤中空間較大",
    "隔日收漲": "收盤高於開盤為1，否則為0",
    "隔日多方空間較大": "上行MFE大於下行MFE為1，否則為0",
}
NIGHT_DIRECTION_OUTCOMES = {
    "夜盤收相對日盤收_pct": "(夜盤收-日盤收)/日盤收；包含夜盤開盤跳空，正值偏多",
    "夜盤開收報酬_pct": "夜盤收盤相對夜盤開盤報酬；正值偏多",
    "夜盤方向效率_pct": "(夜盤收-夜盤開)/(夜盤高-夜盤低)*100；正值偏多",
    "夜盤收盤位置_pct": "夜盤收盤位於夜盤高低區間的位置；越高越偏多",
    "夜盤多空空間差_pct": "夜盤上行MFE-下行MFE；正值表示多方空間較大",
    "夜盤收高於日盤收": "夜盤收盤高於日盤收盤為1，否則為0",
    "夜盤收漲": "夜盤收盤高於夜盤開盤為1，否則為0",
    "夜盤多方空間較大": "夜盤上行MFE大於下行MFE為1，否則為0",
}
DIRECTION_OUTCOMES = {**DAY_DIRECTION_OUTCOMES, **NIGHT_DIRECTION_OUTCOMES}
RANKING_OUTCOMES_BY_SESSION = {
    "當日夜盤": (
        "夜盤收相對日盤收_pct", "夜盤開收報酬_pct", "夜盤方向效率_pct",
        "夜盤收盤位置_pct", "夜盤多空空間差_pct",
    ),
    "隔日日盤": (
        "隔日開收報酬_pct", "隔日方向效率_pct",
        "隔日收盤位置_pct", "隔日多空空間差_pct",
    ),
}
PRIMARY_OUTCOMES = {"當日夜盤": "夜盤收相對日盤收_pct", "隔日日盤": "隔日方向效率_pct"}


def _binary(valid: pd.Series, condition: pd.Series) -> pd.Series:
    return pd.Series(np.where(valid, condition.astype(float), np.nan), index=valid.index, dtype=float)


def prepare_direction_detail(
    source: pd.DataFrame,
    settings: Settings,
) -> tuple[pd.DataFrame, dict[str, dict[str, object]]]:
    ordered = source.sort_values("日期").reset_index(drop=True).copy()
    signals, metadata = build_signal_features(ordered)
    day_open = _numeric(ordered, "日盤開").shift(-1)
    day_high = _numeric(ordered, "日盤高").shift(-1)
    day_low = _numeric(ordered, "日盤低").shift(-1)
    day_close = _numeric(ordered, "日盤收").shift(-1)
    signal_day_close = _numeric(ordered, "日盤收")
    night_open = _numeric(ordered, "夜盤開")
    night_high = _numeric(ordered, "夜盤高")
    night_low = _numeric(ordered, "夜盤低")
    night_close = _numeric(ordered, "夜盤收")
    result_date = pd.to_datetime(ordered["日期"]).shift(-1)
    up_mfe = _pct(day_high - day_open, day_open)
    down_mfe = _pct(day_open - day_low, day_open)
    valid_close = day_open.notna() & day_close.notna()
    valid_mfe = up_mfe.notna() & down_mfe.notna()
    night_up_mfe = _pct(night_high - night_open, night_open)
    night_down_mfe = _pct(night_open - night_low, night_open)
    valid_night_close = night_open.notna() & night_close.notna()
    valid_night_mfe = night_up_mfe.notna() & night_down_mfe.notna()
    valid_day_to_night = signal_day_close.notna() & night_close.notna()
    outcomes = pd.DataFrame({
        "隔日日期": result_date,
        "隔日開收報酬_pct": _pct(day_close - day_open, day_open),
        "隔日方向效率_pct": _safe_ratio(day_close - day_open, day_high - day_low) * 100.0,
        "隔日收盤位置_pct": _safe_ratio(day_close - day_low, day_high - day_low) * 100.0,
        "隔日多空空間差_pct": up_mfe - down_mfe,
        "隔日收漲": _binary(valid_close, day_close > day_open),
        "隔日多方空間較大": _binary(valid_mfe, up_mfe > down_mfe),
        "夜盤收相對日盤收_pct": _pct(night_close - signal_day_close, signal_day_close),
        "夜盤開收報酬_pct": _pct(night_close - night_open, night_open),
        "夜盤方向效率_pct": _safe_ratio(night_close - night_open, night_high - night_low) * 100.0,
        "夜盤收盤位置_pct": _safe_ratio(night_close - night_low, night_high - night_low) * 100.0,
        "夜盤多空空間差_pct": night_up_mfe - night_down_mfe,
        "夜盤收高於日盤收": _binary(valid_day_to_night, night_close > signal_day_close),
        "夜盤收漲": _binary(valid_night_close, night_close > night_open),
        "夜盤多方空間較大": _binary(valid_night_mfe, night_up_mfe > night_down_mfe),
        # 保留兩側空間供人工覆核，但不把兩者各自變大判定為方向有效。
        "隔日上行MFE_pct": up_mfe,
        "隔日下行MFE_pct": down_mfe,
        "夜盤上行MFE_pct": night_up_mfe,
        "夜盤下行MFE_pct": night_down_mfe,
    })

    parts: list[pd.DataFrame] = []
    for signal, meta in metadata.items():
        raw = pd.to_numeric(signals[signal], errors="coerce")
        percentile = _rolling_percentile(raw, settings.history_window, settings.minimum_history)
        parts.append(pd.concat([
            pd.DataFrame({
                "訊號日": pd.to_datetime(ordered["日期"]),
                "指標類別": meta["類別"],
                "指標代碼": signal,
                "公式": meta["公式"],
                "原始值": raw,
                "歷史分位_pct": percentile,
                "強度層": _strength_bucket(percentile),
            }),
            outcomes,
        ], axis=1))
    detail = pd.concat(parts, ignore_index=True)
    signal_dates = pd.to_datetime(detail["訊號日"]).dt.date
    valid = (signal_dates >= settings.start) & detail["隔日日期"].notna()
    if settings.end:
        valid &= signal_dates <= settings.end
    detail = detail[valid].copy()
    return detail.reset_index(drop=True), metadata


def build_direction_bucket_summary(detail: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    grouped = detail.dropna(subset=["強度層"]).groupby(
        ["指標類別", "指標代碼", "強度層"], observed=True, sort=False
    )
    for (category, signal, bucket), group in grouped:
        close_return = pd.to_numeric(group["隔日開收報酬_pct"], errors="coerce")
        rows.append({
            "指標類別": category,
            "指標代碼": signal,
            "強度層": str(bucket),
            "樣本數": int(close_return.notna().sum()),
            "原始值平均": group["原始值"].mean(),
            "歷史分位平均_pct": group["歷史分位_pct"].mean(),
            "隔日開收平均_pct": close_return.mean(),
            "隔日開收中位數_pct": close_return.median(),
            "隔日上漲機率_pct": group["隔日收漲"].mean() * 100.0,
            "隔日方向效率平均_pct": group["隔日方向效率_pct"].mean(),
            "隔日收盤位置平均_pct": group["隔日收盤位置_pct"].mean(),
            "隔日多空空間差平均_pct": group["隔日多空空間差_pct"].mean(),
            "隔日多方空間較大機率_pct": group["隔日多方空間較大"].mean() * 100.0,
            "隔日上行MFE平均_pct": group["隔日上行MFE_pct"].mean(),
            "隔日下行MFE平均_pct": group["隔日下行MFE_pct"].mean(),
            "夜盤收相對日盤收平均_pct": group["夜盤收相對日盤收_pct"].mean(),
            "夜盤收高於日盤收機率_pct": group["夜盤收高於日盤收"].mean() * 100.0,
            "夜盤開收平均_pct": group["夜盤開收報酬_pct"].mean(),
            "夜盤收漲機率_pct": group["夜盤收漲"].mean() * 100.0,
            "夜盤方向效率平均_pct": group["夜盤方向效率_pct"].mean(),
            "夜盤收盤位置平均_pct": group["夜盤收盤位置_pct"].mean(),
            "夜盤多空空間差平均_pct": group["夜盤多空空間差_pct"].mean(),
            "夜盤多方空間較大機率_pct": group["夜盤多方空間較大"].mean() * 100.0,
            "夜盤上行MFE平均_pct": group["夜盤上行MFE_pct"].mean(),
            "夜盤下行MFE平均_pct": group["夜盤下行MFE_pct"].mean(),
        })
    return pd.DataFrame(rows)


def _strength_label(effect: float) -> str:
    absolute = abs(effect)
    if absolute >= 0.20:
        return "強"
    if absolute >= 0.10:
        return "中"
    if absolute >= 0.05:
        return "弱"
    return "很弱"


def scan_direction_period(detail: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for signal, signal_data in detail.groupby("指標代碼", sort=False):
        category = signal_data["指標類別"].iloc[0]
        for outcome in DIRECTION_OUTCOMES:
            data = signal_data[["歷史分位_pct", "強度層", outcome]].dropna()
            if len(data) < settings.minimum_group_size * 2:
                continue
            outcome_values = _clip(data[outcome], settings.winsor_tail)
            percentile = data.loc[outcome_values.index, "歷史分位_pct"]
            if percentile.nunique() >= 2 and outcome_values.nunique() >= 2:
                rho, p_value = spearmanr(percentile, outcome_values)
                if np.isfinite(rho) and np.isfinite(p_value):
                    rows.append({
                        "指標類別": category, "指標代碼": signal, "結果指標": outcome,
                        "檢定類型": "Spearman強度趨勢", "樣本數": len(outcome_values),
                        "最低層樣本數": math.nan, "最高層樣本數": math.nan,
                        "統計值": rho, "效果值": rho,
                        "方向判定": "指標越高越偏多" if rho > 0 else "指標越高越偏空",
                        "方向強度": _strength_label(rho), "效果量": abs(rho), "p值": p_value,
                    })

            low = _clip(data.loc[data["強度層"] == BUCKET_LABELS[0], outcome], settings.winsor_tail)
            high = _clip(data.loc[data["強度層"] == BUCKET_LABELS[-1], outcome], settings.winsor_tail)
            if min(len(low), len(high)) >= settings.minimum_group_size:
                statistic, p_value = mannwhitneyu(high, low, alternative="two-sided")
                difference = high.mean() - low.mean()
                rows.append({
                    "指標類別": category, "指標代碼": signal, "結果指標": outcome,
                    "檢定類型": "Q5最高20%對Q1最低20%", "樣本數": len(low) + len(high),
                    "最低層樣本數": len(low), "最高層樣本數": len(high),
                    "最低層平均": low.mean(), "最高層平均": high.mean(),
                    "統計值": statistic, "效果值": difference,
                    "方向判定": "指標越高越偏多" if difference > 0 else "指標越高越偏空",
                    "方向強度": _strength_label(_hedges_g(high, low)),
                    "效果量": abs(_hedges_g(high, low)), "p值": p_value,
                })
    result = pd.DataFrame(rows)
    if not result.empty:
        result["FDR_q值"] = _bh_adjust(result["p值"])
        result["FDR顯著"] = result["FDR_q值"] < settings.alpha
    return result


def add_direction_validation(
    full: pd.DataFrame,
    detail: pd.DataFrame,
    settings: Settings,
) -> pd.DataFrame:
    if full.empty:
        return full
    signal_dates = pd.to_datetime(detail["訊號日"]).dt.date
    discovery = scan_direction_period(detail[signal_dates < settings.split_date], settings)
    validation = scan_direction_period(detail[signal_dates >= settings.split_date], settings)
    keys = ["指標代碼", "結果指標", "檢定類型"]
    keep = keys + ["效果值", "p值", "FDR_q值"]
    discovery = discovery[keep].rename(columns={
        "效果值": "探索期_效果值", "p值": "探索期_p值", "FDR_q值": "探索期_q值",
    }) if not discovery.empty else pd.DataFrame(columns=keep).rename(columns={
        "效果值": "探索期_效果值", "p值": "探索期_p值", "FDR_q值": "探索期_q值",
    })
    validation = validation[keep].rename(columns={
        "效果值": "驗證期_效果值", "p值": "驗證期_p值", "FDR_q值": "驗證期_q值",
    }) if not validation.empty else pd.DataFrame(columns=keep).rename(columns={
        "效果值": "驗證期_效果值", "p值": "驗證期_p值", "FDR_q值": "驗證期_q值",
    })
    result = full.merge(discovery, on=keys, how="left").merge(validation, on=keys, how="left")
    result["前後期同方向"] = (
        np.sign(result["探索期_效果值"]) == np.sign(result["驗證期_效果值"])
    )

    stability: list[tuple[int, int, float]] = []
    for row in result.itertuples(index=False):
        signal_history = detail[detail["指標代碼"] == row.指標代碼].copy()
        year_column = "訊號日" if row.結果指標 in NIGHT_DIRECTION_OUTCOMES else "隔日日期"
        signal_history["_year"] = pd.to_datetime(signal_history[year_column]).dt.year
        expected_sign = int(np.sign(row.效果值))
        signs: list[int] = []
        for _, year_group in signal_history.groupby("_year"):
            values = year_group[["歷史分位_pct", "強度層", row.結果指標]].dropna()
            if row.檢定類型 == "Spearman強度趨勢":
                if len(values) < settings.minimum_year_size:
                    continue
                effect = spearmanr(values["歷史分位_pct"], values[row.結果指標]).statistic
            else:
                low = values.loc[values["強度層"] == BUCKET_LABELS[0], row.結果指標]
                high = values.loc[values["強度層"] == BUCKET_LABELS[-1], row.結果指標]
                if min(len(low), len(high)) < max(5, settings.minimum_year_size // 5):
                    continue
                effect = high.mean() - low.mean()
            if np.isfinite(effect) and effect != 0:
                signs.append(int(np.sign(effect)))
        matches = sum(sign == expected_sign for sign in signs)
        stability.append((len(signs), matches, matches / len(signs) if signs else math.nan))
    result[["可比較年度數", "同方向年度數", "年度方向一致率"]] = pd.DataFrame(
        stability, index=result.index
    )
    result["重複驗證通過"] = (
        result["檢定類型"].eq("Spearman強度趨勢")
        & result["FDR顯著"]
        & result["前後期同方向"]
        & (result["探索期_q值"] < 0.10)
        & (result["驗證期_p值"] < settings.alpha)
        & (result["年度方向一致率"] >= 0.60)
    )
    return result.sort_values(
        ["重複驗證通過", "FDR顯著", "FDR_q值", "效果量"],
        ascending=[False, False, True, False], na_position="last",
    ).reset_index(drop=True)


def build_direction_ranking(tests: pd.DataFrame) -> pd.DataFrame:
    if tests.empty:
        return pd.DataFrame()
    spearman = tests[tests["檢定類型"].eq("Spearman強度趨勢")]
    rows: list[dict[str, object]] = []
    for (category, signal), group in spearman.groupby(["指標類別", "指標代碼"], sort=False):
        for session, outcomes in RANKING_OUTCOMES_BY_SESSION.items():
            ranked = group[group["結果指標"].isin(outcomes)]
            stable = ranked[ranked["重複驗證通過"].fillna(False)]
            positive = int((stable["效果值"] > 0).sum())
            negative = int((stable["效果值"] < 0).sum())
            if stable.empty:
                direction = "沒有穩定方向"
                grade = "未驗證"
                strongest_pool = ranked
            else:
                strongest_pool = stable
                if positive > 0 and negative == 0:
                    direction = "指標越高越偏多"
                elif negative > 0 and positive == 0:
                    direction = "指標越高越偏空"
                elif positive > negative:
                    direction = "偏多，但不同結果有衝突"
                elif negative > positive:
                    direction = "偏空，但不同結果有衝突"
                else:
                    direction = "方向證據衝突"
            if strongest_pool.empty:
                strongest_outcome = None
                strongest_effect = math.nan
            else:
                strongest = strongest_pool.loc[strongest_pool["效果值"].abs().idxmax()]
                strongest_outcome = strongest["結果指標"]
                strongest_effect = strongest["效果值"]
            if not stable.empty:
                grade = _strength_label(float(strongest_effect))

            def value(outcome: str, column: str) -> object:
                match = group[group["結果指標"].eq(outcome)]
                return match.iloc[0][column] if not match.empty else math.nan

            primary = PRIMARY_OUTCOMES[session]
            q_comparison = tests[
                tests["指標代碼"].eq(signal)
                & tests["結果指標"].eq(primary)
                & tests["檢定類型"].eq("Q5最高20%對Q1最低20%")
            ]
            intraday_return = "夜盤開收報酬_pct" if session == "當日夜盤" else "隔日開收報酬_pct"
            efficiency = "夜盤方向效率_pct" if session == "當日夜盤" else "隔日方向效率_pct"
            close_location = "夜盤收盤位置_pct" if session == "當日夜盤" else "隔日收盤位置_pct"
            space_gap = "夜盤多空空間差_pct" if session == "當日夜盤" else "隔日多空空間差_pct"
            rows.append({
                "指標類別": category,
                "指標代碼": signal,
                "預測時段": session,
                "主要結果指標": primary,
                "綜合方向判定": direction,
                "方向強度等級": grade,
                "穩定方向證據數": len(stable),
                "穩定偏多證據數": positive,
                "穩定偏空證據數": negative,
                "最強結果指標": strongest_outcome,
                "最強Spearman_rho": strongest_effect,
                "最強絕對rho": abs(strongest_effect) if np.isfinite(strongest_effect) else math.nan,
                "主要結果_rho": value(primary, "效果值"),
                "主要結果_FDR_q值": value(primary, "FDR_q值"),
                "主要結果_重複驗證": value(primary, "重複驗證通過"),
                "夜盤收相對日盤收_rho": value("夜盤收相對日盤收_pct", "效果值"),
                "盤內開收報酬_rho": value(intraday_return, "效果值"),
                "方向效率_rho": value(efficiency, "效果值"),
                "收盤位置_rho": value(close_location, "效果值"),
                "多空空間差_rho": value(space_gap, "效果值"),
                "主要結果_Q5減Q1": q_comparison.iloc[0]["效果值"] if not q_comparison.empty else math.nan,
                "主要結果_Q5Q1_FDR_q值": q_comparison.iloc[0]["FDR_q值"] if not q_comparison.empty else math.nan,
            })
    return pd.DataFrame(rows).sort_values(
        ["穩定方向證據數", "最強絕對rho"],
        ascending=[False, False], na_position="last",
    ).reset_index(drop=True)


def build_direction_yearly(detail: pd.DataFrame, minimum_size: int) -> pd.DataFrame:
    work = detail.copy()
    work["年"] = pd.to_datetime(work["訊號日"]).dt.year
    parts: list[pd.DataFrame] = []
    for year, group in work.groupby("年"):
        summary = build_direction_bucket_summary(group)
        if not summary.empty:
            summary.insert(0, "年", int(year))
            parts.append(summary[summary["樣本數"] >= minimum_size])
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def export_direction_results(
    detail: pd.DataFrame,
    buckets: pd.DataFrame,
    tests: pd.DataFrame,
    ranking: pd.DataFrame,
    yearly: pd.DataFrame,
    settings: Settings,
    metadata: dict[str, dict[str, object]],
    output_dir: Path,
    run_id: uuid.UUID,
) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    end_label = settings.end.strftime("%Y%m%d") if settings.end else "latest"
    suffix = f"{settings.start:%Y%m%d}_{end_label}"
    paths = (
        output_dir / f"direction_signal_detail_{suffix}.csv",
        output_dir / f"direction_signal_buckets_{suffix}.csv",
        output_dir / f"direction_signal_tests_{suffix}.csv",
        output_dir / f"direction_signal_ranking_{suffix}.csv",
        output_dir / f"direction_signal_yearly_{suffix}.csv",
        output_dir / f"direction_signal_manifest_{suffix}.json",
    )
    for frame, path in zip((detail, buckets, tests, ranking, yearly), paths[:5]):
        frame.to_csv(path, index=False, encoding="utf-8-sig")
    paths[5].write_text(json.dumps({
        "run_id": str(run_id),
        "analysis_name": ANALYSIS_NAME,
        "metric_version": METRIC_VERSION,
        "settings": {**asdict(settings), "start": settings.start.isoformat(),
                     "end": settings.end.isoformat() if settings.end else None,
                     "split_date": settings.split_date.isoformat()},
        "objective": "逐一檢驗原始連續指標越強時，當日夜盤及隔日日盤偏多或偏空的傾向是否同步增強",
        "signals": metadata,
        "direction_outcomes": DIRECTION_OUTCOMES,
        "primary_outcomes": PRIMARY_OUTCOMES,
        "warnings": [
            "所有方向結果均統一為數值越高越偏多；負相關表示指標越高越偏空",
            "振幅大小不作為方向有效的判定依據",
            "日K只能比較上下可達空間，無法判斷盤中高低點先後",
            "通過統計檢定仍不等於扣除成本後可以交易",
        ],
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="原始指標強度與當日夜盤／隔日日盤多空方向傾向掃描")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--split-date", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--history-window", type=int, default=252)
    parser.add_argument("--minimum-history", type=int, default=60)
    parser.add_argument("--minimum-group-size", type=int, default=30)
    parser.add_argument("--minimum-year-size", type=int, default=20)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--winsor-tail", type=float, default=0.005)
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path("backtest_output/futures_direction_signal_strength_scan"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(
        start=args.start, end=args.end, futures_id=args.futures_id,
        split_date=args.split_date, history_window=args.history_window,
        minimum_history=args.minimum_history, minimum_group_size=args.minimum_group_size,
        minimum_year_size=args.minimum_year_size, alpha=args.alpha,
        winsor_tail=args.winsor_tail,
    )
    source = load_source(settings)
    detail, metadata = prepare_direction_detail(source, settings)
    buckets = build_direction_bucket_summary(detail)
    tests = add_direction_validation(scan_direction_period(detail, settings), detail, settings)
    ranking = build_direction_ranking(tests)
    yearly = build_direction_yearly(detail, settings.minimum_year_size)
    run_id = uuid.uuid4()
    paths = export_direction_results(
        detail, buckets, tests, ranking, yearly, settings, metadata,
        args.output_dir, run_id,
    )
    print(f"方向候選指標：{len(metadata)} 個")
    print(f"具有穩定方向證據（指標×時段）：{int((ranking['穩定方向證據數'] > 0).sum()) if len(ranking) else 0} 組")
    print(f"研究 run_id：{run_id}")
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
