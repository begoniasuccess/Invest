from __future__ import annotations

import argparse
import itertools
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import fisher_exact

from analysis.futures_daily_amplitude_feature_scan import (
    Settings as FeatureSettings,
    load_source,
    load_targets,
    prepare_detail,
)
from backtest.futures_strategy_next_day_field_scan import _bh_adjust


ANALYSIS_NAME = "futures_daily_amplitude_violent_combo_scan"
METRIC_VERSION = "1.0.0"
TARGET = "劇烈以上"
CORE_FEATURES = {
    "近5日日盤振幅_前30日比": "近期振幅",
    "夜盤振幅_前30日比": "夜盤振幅",
    "夜成交量_前20日比": "夜盤成交量",
}
THRESHOLDS = {
    "Q4以上": 60.0,
    "Q5": 80.0,
}


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    split_date: date = date(2023, 1, 1)
    history_window: int = 252
    minimum_history: int = 60
    minimum_rule_sample: int = 20
    minimum_year_rule_sample: int = 8
    alpha: float = 0.05

    def validate(self) -> None:
        if self.end and self.end < self.start:
            raise ValueError("end 不可早於 start")
        if self.split_date <= self.start:
            raise ValueError("split_date 必須晚於 start")
        if self.history_window < 60:
            raise ValueError("history_window 至少為60")
        if not 20 <= self.minimum_history <= self.history_window:
            raise ValueError("minimum_history 必須介於20與history_window之間")
        if self.minimum_rule_sample < 10:
            raise ValueError("minimum_rule_sample 至少為10")
        if self.minimum_year_rule_sample < 3:
            raise ValueError("minimum_year_rule_sample 至少為3")


@dataclass(frozen=True)
class Rule:
    rule_id: str
    threshold_label: str
    threshold_pct: float
    features: tuple[str, ...]

    @property
    def name(self) -> str:
        names = [CORE_FEATURES[feature] for feature in self.features]
        return "＋".join(names) + f"皆為{self.threshold_label}"


def build_rules() -> list[Rule]:
    rules: list[Rule] = []
    feature_names = tuple(CORE_FEATURES)
    counter = 1
    for threshold_label, threshold_pct in THRESHOLDS.items():
        for size in range(1, len(feature_names) + 1):
            for features in itertools.combinations(feature_names, size):
                rules.append(Rule(
                    rule_id=f"R{counter:02d}",
                    threshold_label=threshold_label,
                    threshold_pct=threshold_pct,
                    features=features,
                ))
                counter += 1
    return rules


def prepare_model_frame(detail: pd.DataFrame) -> pd.DataFrame:
    base_columns = ["訊號日", "結果日", "range_ratio", TARGET]
    selected = detail[detail["指標代碼"].isin(CORE_FEATURES)].copy()
    if selected.empty:
        raise ValueError("找不到三項核心振幅特徵")

    first_feature = next(iter(CORE_FEATURES))
    base = (
        selected[selected["指標代碼"] == first_feature][base_columns]
        .drop_duplicates(subset=["結果日"])
        .copy()
    )
    for feature in CORE_FEATURES:
        part = (
            selected[selected["指標代碼"] == feature]
            [["結果日", "原始值", "歷史分位_pct", "強度層"]]
            .drop_duplicates(subset=["結果日"])
            .rename(columns={
                "原始值": f"{feature}_原始值",
                "歷史分位_pct": f"{feature}_歷史分位_pct",
                "強度層": f"{feature}_強度層",
            })
        )
        base = base.merge(part, on="結果日", how="left", validate="one_to_one")
    base["訊號日"] = pd.to_datetime(base["訊號日"])
    base["結果日"] = pd.to_datetime(base["結果日"])
    base[TARGET] = pd.to_numeric(base[TARGET], errors="coerce")
    return base.sort_values("結果日").reset_index(drop=True)


def _wilson_interval(events: int, count: int, z: float = 1.959963984540054) -> tuple[float, float]:
    if count <= 0:
        return math.nan, math.nan
    rate = events / count
    denominator = 1.0 + z * z / count
    centre = (rate + z * z / (2.0 * count)) / denominator
    margin = z * math.sqrt(rate * (1.0 - rate) / count + z * z / (4.0 * count * count)) / denominator
    return max(0.0, centre - margin), min(1.0, centre + margin)


def _rule_masks(data: pd.DataFrame, rule: Rule) -> tuple[pd.Series, pd.Series]:
    percentile_columns = [f"{feature}_歷史分位_pct" for feature in rule.features]
    eligible = data[percentile_columns].notna().all(axis=1) & data[TARGET].notna()
    matched = eligible & data[percentile_columns].ge(rule.threshold_pct).all(axis=1)
    return eligible, matched


def _period_stats(data: pd.DataFrame, rule: Rule) -> dict[str, Any]:
    eligible, matched = _rule_masks(data, rule)
    universe = data[eligible]
    hit = data[matched]
    miss = data[eligible & ~matched]
    total_count = len(universe)
    total_events = int(universe[TARGET].sum()) if total_count else 0
    match_count = len(hit)
    match_events = int(hit[TARGET].sum()) if match_count else 0
    baseline = total_events / total_count if total_count else math.nan
    rate = match_events / match_count if match_count else math.nan
    miss_rate = miss[TARGET].mean() if len(miss) else math.nan
    low_ci, high_ci = _wilson_interval(match_events, match_count)
    p_value = math.nan
    odds_ratio = math.nan
    if match_count and len(miss) and universe[TARGET].nunique() == 2:
        table = [
            [match_events, match_count - match_events],
            [int(miss[TARGET].sum()), int(len(miss) - miss[TARGET].sum())],
        ]
        odds_ratio, p_value = fisher_exact(table, alternative="two-sided")
    return {
        "母體數": total_count,
        "母體事件數": total_events,
        "母體事件率_pct": baseline * 100 if np.isfinite(baseline) else math.nan,
        "符合數": match_count,
        "事件數": match_events,
        "事件率_pct": rate * 100 if np.isfinite(rate) else math.nan,
        "事件率95CI下限_pct": low_ci * 100 if np.isfinite(low_ci) else math.nan,
        "事件率95CI上限_pct": high_ci * 100 if np.isfinite(high_ci) else math.nan,
        "未符合事件率_pct": miss_rate * 100 if np.isfinite(miss_rate) else math.nan,
        "Lift": rate / baseline if np.isfinite(rate) and baseline > 0 else math.nan,
        "相對母體增減_百分點": (rate - baseline) * 100 if np.isfinite(rate) and np.isfinite(baseline) else math.nan,
        "覆蓋率_pct": match_count / total_count * 100 if total_count else math.nan,
        "符合對未符合勝算比": odds_ratio,
        "Fisher_p值": p_value,
        "符合日平均振幅倍數": hit["range_ratio"].mean() if match_count else math.nan,
    }


def build_yearly(frame: pd.DataFrame, rules: list[Rule], settings: Settings) -> pd.DataFrame:
    work = frame.copy()
    work["年"] = work["結果日"].dt.year
    rows: list[dict[str, Any]] = []
    for rule in rules:
        for year, group in work.groupby("年", sort=True):
            stats = _period_stats(group, rule)
            rows.append({
                "規則代碼": rule.rule_id,
                "規則名稱": rule.name,
                "門檻": rule.threshold_label,
                "條件數": len(rule.features),
                "年": int(year),
                **stats,
                "年度樣本足夠": stats["符合數"] >= settings.minimum_year_rule_sample,
            })
    return pd.DataFrame(rows)


def build_summary(
    frame: pd.DataFrame,
    rules: list[Rule],
    yearly: pd.DataFrame,
    settings: Settings,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    result_dates = frame["結果日"].dt.date
    periods = {
        "全期": frame,
        "探索期": frame[result_dates < settings.split_date],
        "驗證期": frame[result_dates >= settings.split_date],
    }
    for rule in rules:
        row: dict[str, Any] = {
            "規則代碼": rule.rule_id,
            "規則名稱": rule.name,
            "門檻": rule.threshold_label,
            "歷史分位門檻_pct": rule.threshold_pct,
            "條件數": len(rule.features),
            "使用指標": "｜".join(rule.features),
        }
        for period, group in periods.items():
            stats = _period_stats(group, rule)
            row.update({f"{period}_{key}": value for key, value in stats.items()})
        rows.append(row)
    summary = pd.DataFrame(rows)
    summary["全期_Fisher_q值"] = _bh_adjust(summary["全期_Fisher_p值"])

    annual_info: list[tuple[int, int, float]] = []
    for rule in rules:
        group = yearly[(yearly["規則代碼"] == rule.rule_id) & yearly["年度樣本足夠"]]
        comparable = group.dropna(subset=["Lift"])
        positive = int(comparable["Lift"].gt(1.0).sum())
        annual_info.append((
            len(comparable),
            positive,
            positive / len(comparable) if len(comparable) else math.nan,
        ))
    summary[["可比較年度數", "Lift大於1年度數", "年度正向率"]] = pd.DataFrame(annual_info, index=summary.index)
    summary["探索驗證皆高於母體"] = (
        summary["探索期_Lift"].gt(1.0) & summary["驗證期_Lift"].gt(1.0)
    )
    summary["簡化候選"] = (
        summary["全期_Fisher_q值"].lt(settings.alpha)
        & summary["探索驗證皆高於母體"]
        & summary["驗證期_Fisher_p值"].lt(0.10)
        & summary["驗證期_符合數"].ge(settings.minimum_rule_sample)
        & summary["年度正向率"].ge(0.60)
    )
    return summary.sort_values(
        ["簡化候選", "驗證期_事件率_pct", "驗證期_符合數"],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True)


def build_rule_detail(frame: pd.DataFrame, rules: list[Rule]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    base_columns = ["訊號日", "結果日", "range_ratio", TARGET]
    for rule in rules:
        eligible, matched = _rule_masks(frame, rule)
        part = frame[base_columns].copy()
        part["規則代碼"] = rule.rule_id
        part["規則名稱"] = rule.name
        part["門檻"] = rule.threshold_label
        part["條件數"] = len(rule.features)
        part["使用指標"] = "｜".join(rule.features)
        part["可判定"] = eligible
        part["符合條件"] = matched
        parts.append(part)
    return pd.concat(parts, ignore_index=True)


def export_results(
    frame: pd.DataFrame,
    summary: pd.DataFrame,
    yearly: pd.DataFrame,
    rule_detail: pd.DataFrame,
    rules: list[Rule],
    settings: Settings,
    output_dir: Path,
) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{settings.start:%Y%m%d}_{settings.end:%Y%m%d}" if settings.end else f"{settings.start:%Y%m%d}_latest"
    paths = (
        output_dir / f"violent_amplitude_combo_summary_{suffix}.csv",
        output_dir / f"violent_amplitude_combo_yearly_{suffix}.csv",
        output_dir / f"violent_amplitude_combo_detail_{suffix}.csv",
        output_dir / f"violent_amplitude_combo_manifest_{suffix}.json",
    )
    summary.to_csv(paths[0], index=False, encoding="utf-8-sig")
    yearly.to_csv(paths[1], index=False, encoding="utf-8-sig")
    rule_detail.to_csv(paths[2], index=False, encoding="utf-8-sig")
    paths[3].write_text(json.dumps({
        "analysis_name": ANALYSIS_NAME,
        "metric_version": METRIC_VERSION,
        "run_id": str(uuid.uuid4()),
        "settings": {
            **asdict(settings),
            "start": settings.start.isoformat(),
            "end": settings.end.isoformat() if settings.end else None,
            "split_date": settings.split_date.isoformat(),
        },
        "target": {TARGET: "振幅標籤為劇烈振幅或極端振幅"},
        "features": CORE_FEATURES,
        "thresholds": THRESHOLDS,
        "rules": [{
            "rule_id": rule.rule_id,
            "name": rule.name,
            "threshold": rule.threshold_label,
            "threshold_pct": rule.threshold_pct,
            "features": list(rule.features),
        } for rule in rules],
        "candidate_rule": (
            "全期Fisher檢定BH-FDR q<0.05、探索與驗證Lift皆>1、"
            "驗證期Fisher p<0.10、驗證期符合數達門檻、年度正向率>=60%"
        ),
        "warnings": [
            "本程式只研究隔日日盤是否劇烈以上，不區分多空或單邊型態",
            "這是條件機率分析，不是含進出場、成本與損益的交易回測",
            "三項特徵可能高度相關，組合Lift必須搭配樣本數與驗證期一起看",
        ],
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="隔日日盤劇烈振幅：三項核心前置特徵組合統計")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--split-date", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--history-window", type=int, default=252)
    parser.add_argument("--minimum-history", type=int, default=60)
    parser.add_argument("--minimum-rule-sample", type=int, default=20)
    parser.add_argument("--minimum-year-rule-sample", type=int, default=8)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("../output/analysis/futures_daily_amplitude_violent_combo_scan"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(
        start=args.start,
        end=args.end,
        futures_id=args.futures_id,
        split_date=args.split_date,
        history_window=args.history_window,
        minimum_history=args.minimum_history,
        minimum_rule_sample=args.minimum_rule_sample,
        minimum_year_rule_sample=args.minimum_year_rule_sample,
        alpha=args.alpha,
    )
    settings.validate()
    feature_settings = FeatureSettings(
        start=settings.start,
        end=settings.end,
        futures_id=settings.futures_id,
        split_date=settings.split_date,
        history_window=settings.history_window,
        minimum_history=settings.minimum_history,
    )
    source = load_source(feature_settings)
    targets = load_targets(feature_settings)
    feature_detail, _ = prepare_detail(source, targets, feature_settings)
    frame = prepare_model_frame(feature_detail)
    rules = build_rules()
    yearly = build_yearly(frame, rules, settings)
    summary = build_summary(frame, rules, yearly, settings)
    rule_detail = build_rule_detail(frame, rules)
    paths = export_results(frame, summary, yearly, rule_detail, rules, settings, args.output_dir)

    baseline = frame[TARGET].mean() * 100
    print(f"有效研究日：{len(frame):,} 日")
    print(f"劇烈以上：{int(frame[TARGET].sum()):,} 日（{baseline:.2f}%）")
    print(f"組合規則：{len(rules)} 組")
    print(f"簡化候選：{int(summary['簡化候選'].sum())} 組")
    display_columns = ["規則代碼", "規則名稱", "驗證期_符合數", "驗證期_事件率_pct", "驗證期_Lift", "簡化候選"]
    print("\n驗證期事件率最高：")
    print(summary[display_columns].head(10).to_string(index=False))
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
