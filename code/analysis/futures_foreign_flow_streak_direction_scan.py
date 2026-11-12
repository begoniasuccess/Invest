from __future__ import annotations

import argparse
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import binomtest, fisher_exact

from analysis.futures_daily_amplitude_foreign_flow_scan import (
    FOREIGN_COLUMN,
    STOCK_SOURCE_TABLE,
    STOCK_SYMBOL,
    TURNOVER_COLUMN,
    Settings,
    _wilson_interval,
    build_foreign_feature,
    load_source_with_foreign,
)
from analysis.futures_foreign_flow_direction_scan import prepare_direction_frame
from backtest.futures_strategy_next_day_field_scan import _bh_adjust


ANALYSIS_NAME = "futures_foreign_flow_streak_direction_scan"
METRIC_VERSION = "1.0.0"
STREAK_WINDOWS = (2, 3, 5)


@dataclass(frozen=True)
class StreakRule:
    rule_id: str
    name: str
    condition_id: str
    condition_name: str
    foreign_side: str
    streak_days: int
    require_extreme: bool
    target_column: str
    target_value: str
    target_name: str


def _condition_specs(settings: Settings) -> list[tuple[str, str, str, int, bool]]:
    specs: list[tuple[str, str, str, int, bool]] = []
    for days in STREAK_WINDOWS:
        specs.extend([
            (f"SELL_{days}", f"連續{days}日賣超", "sell", days, False),
            (f"BUY_{days}", f"連續{days}日買超", "buy", days, False),
        ])
    specs.extend([
        (
            "SELL_3_EXTREME",
            f"連續3日賣超＋{settings.foreign_days}日累計大賣Q1",
            "sell", 3, True,
        ),
        (
            "BUY_3_EXTREME",
            f"連續3日買超＋{settings.foreign_days}日累計大買Q5",
            "buy", 3, True,
        ),
    ])
    return specs


def build_rules(settings: Settings) -> list[StreakRule]:
    rules: list[StreakRule] = []
    rule_no = 1
    targets = [
        ("隔日開收方向", "下跌", "上漲", "隔日日盤開收到收盤"),
        ("隔日日盤走勢", "跌勢盤", "漲勢盤", "隔日日盤極值先後走勢"),
    ]
    for condition_id, condition_name, side, days, require_extreme in _condition_specs(settings):
        for target_column, sell_target, buy_target, target_label in targets:
            target_value = sell_target if side == "sell" else buy_target
            rules.append(StreakRule(
                rule_id=f"S{rule_no:02d}",
                name=f"{condition_name}→{target_value}",
                condition_id=condition_id,
                condition_name=condition_name,
                foreign_side=side,
                streak_days=days,
                require_extreme=require_extreme,
                target_column=target_column,
                target_value=target_value,
                target_name=f"{target_label}{target_value}",
            ))
            rule_no += 1
    return rules


def add_streak_features(
    source: pd.DataFrame,
    direction_frame: pd.DataFrame,
    settings: Settings,
) -> pd.DataFrame:
    ordered = source.sort_values("日期").reset_index(drop=True).copy()
    foreign = pd.to_numeric(ordered[FOREIGN_COLUMN], errors="coerce")
    features = pd.DataFrame({"訊號日": pd.to_datetime(ordered["日期"])})
    for days in STREAK_WINDOWS:
        buy_days = foreign.gt(0).rolling(days, min_periods=days).sum()
        sell_days = foreign.lt(0).rolling(days, min_periods=days).sum()
        features[f"連續{days}日買超"] = buy_days.eq(days)
        features[f"連續{days}日賣超"] = sell_days.eq(days)
    streak_columns = [column for column in features.columns if column != "訊號日"]
    base = direction_frame.drop(
        columns=[column for column in streak_columns if column in direction_frame.columns]
    )
    frame = base.merge(features, on="訊號日", how="left", validate="one_to_one")
    frame["連續3日大買"] = (
        frame["連續3日買超"]
        & frame["外資歷史分位_pct"].ge(settings.upper_percentile)
    )
    frame["連續3日大賣"] = (
        frame["連續3日賣超"]
        & frame["外資歷史分位_pct"].le(settings.lower_percentile)
    )
    return frame


def _condition_mask(frame: pd.DataFrame, rule: StreakRule) -> pd.Series:
    if rule.require_extreme:
        column = "連續3日大賣" if rule.foreign_side == "sell" else "連續3日大買"
    else:
        direction = "賣超" if rule.foreign_side == "sell" else "買超"
        column = f"連續{rule.streak_days}日{direction}"
    return frame[column].fillna(False).astype(bool)


def _valid_target(frame: pd.DataFrame, rule: StreakRule) -> pd.Series:
    valid_values = (
        {"上漲", "下跌"}
        if rule.target_column == "隔日開收方向"
        else {"漲勢盤", "跌勢盤"}
    )
    return frame[rule.target_column].isin(valid_values)


def _rate(events: int, count: int) -> float:
    return events / count if count else math.nan


def _period_stats(frame: pd.DataFrame, rule: StreakRule) -> dict[str, Any]:
    eligible = _valid_target(frame, rule)
    matched = eligible & _condition_mask(frame, rule)
    event = frame[rule.target_column].eq(rule.target_value)
    rest = eligible & ~matched

    universe_count = int(eligible.sum())
    universe_events = int((eligible & event).sum())
    match_count = int(matched.sum())
    match_events = int((matched & event).sum())
    rest_count = int(rest.sum())
    rest_events = int((rest & event).sum())
    universe_rate = _rate(universe_events, universe_count)
    match_rate = _rate(match_events, match_count)
    rest_rate = _rate(rest_events, rest_count)
    low_ci, high_ci = _wilson_interval(match_events, match_count)

    odds_ratio = math.nan
    p_value = math.nan
    if match_count and rest_count:
        odds_ratio, p_value = fisher_exact([
            [match_events, match_count - match_events],
            [rest_events, rest_count - rest_events],
        ], alternative="two-sided")

    returns = pd.to_numeric(frame.loc[matched, "隔日開收報酬_pct"], errors="coerce")
    return {
        "母體數": universe_count,
        "母體命中數": universe_events,
        "母體命中率_pct": universe_rate * 100 if np.isfinite(universe_rate) else math.nan,
        "符合數": match_count,
        "命中數": match_events,
        "命中率_pct": match_rate * 100 if np.isfinite(match_rate) else math.nan,
        "命中率95CI下限_pct": low_ci * 100 if np.isfinite(low_ci) else math.nan,
        "命中率95CI上限_pct": high_ci * 100 if np.isfinite(high_ci) else math.nan,
        "其餘日數": rest_count,
        "其餘日命中率_pct": rest_rate * 100 if np.isfinite(rest_rate) else math.nan,
        "相對其餘日Lift": match_rate / rest_rate if np.isfinite(match_rate) and rest_rate > 0 else math.nan,
        "相對其餘日增減_百分點": (
            (match_rate - rest_rate) * 100
            if np.isfinite(match_rate) and np.isfinite(rest_rate) else math.nan
        ),
        "符合對其餘日勝算比": odds_ratio,
        "Fisher_p值": p_value,
        "符合日平均開收報酬_pct": returns.mean(),
        "符合日中位開收報酬_pct": returns.median(),
    }


def build_yearly(
    frame: pd.DataFrame,
    rules: list[StreakRule],
    settings: Settings,
) -> pd.DataFrame:
    work = frame.copy()
    work["年"] = work["結果日"].dt.year
    rows: list[dict[str, Any]] = []
    for rule in rules:
        for year, group in work.groupby("年", sort=True):
            stats = _period_stats(group, rule)
            rows.append({
                "規則代碼": rule.rule_id,
                "規則名稱": rule.name,
                "條件代碼": rule.condition_id,
                "條件名稱": rule.condition_name,
                "方向定義": "開收方向" if rule.target_column == "隔日開收方向" else "極值先後走勢",
                "年": int(year),
                **stats,
                "年度樣本足夠": stats["符合數"] >= settings.minimum_year_rule_sample,
            })
    return pd.DataFrame(rows)


def build_summary(
    frame: pd.DataFrame,
    rules: list[StreakRule],
    yearly: pd.DataFrame,
    settings: Settings,
) -> pd.DataFrame:
    dates = frame["結果日"].dt.date
    periods = {
        "全期": frame,
        "探索期": frame[dates < settings.split_date],
        "驗證期": frame[dates >= settings.split_date],
    }
    rows: list[dict[str, Any]] = []
    for rule in rules:
        row: dict[str, Any] = {
            "規則代碼": rule.rule_id,
            "規則名稱": rule.name,
            "條件代碼": rule.condition_id,
            "條件名稱": rule.condition_name,
            "外資方向": "賣超" if rule.foreign_side == "sell" else "買超",
            "連續日數": rule.streak_days,
            "要求累計極端": rule.require_extreme,
            "方向定義": "開收方向" if rule.target_column == "隔日開收方向" else "極值先後走勢",
            "預測目標": rule.target_name,
        }
        for period, group in periods.items():
            row.update({f"{period}_{key}": value for key, value in _period_stats(group, rule).items()})
        rows.append(row)
    summary = pd.DataFrame(rows)
    summary["全期_Fisher_q值"] = _bh_adjust(summary["全期_Fisher_p值"])

    annual_info: list[tuple[int, int, float]] = []
    for rule in rules:
        group = yearly[(yearly["規則代碼"] == rule.rule_id) & yearly["年度樣本足夠"]]
        comparable = group.dropna(subset=["相對其餘日Lift"])
        positive = int(comparable["相對其餘日Lift"].gt(1.0).sum())
        annual_info.append((
            len(comparable), positive,
            positive / len(comparable) if len(comparable) else math.nan,
        ))
    summary[["可比較年度數", "優於其餘日年度數", "年度正向率"]] = pd.DataFrame(
        annual_info, index=summary.index
    )
    summary["探索驗證皆正向"] = (
        summary["探索期_相對其餘日Lift"].gt(1.0)
        & summary["驗證期_相對其餘日Lift"].gt(1.0)
    )
    summary["連續外資方向候選"] = (
        summary["全期_Fisher_q值"].lt(settings.alpha)
        & summary["探索驗證皆正向"]
        & summary["驗證期_Fisher_p值"].lt(0.10)
        & summary["驗證期_符合數"].ge(settings.minimum_rule_sample)
        & summary["年度正向率"].ge(0.60)
    )
    return summary.sort_values(
        ["連續外資方向候選", "驗證期_相對其餘日Lift", "驗證期_符合數"],
        ascending=[False, False, False], na_position="last",
    ).reset_index(drop=True)


def _pair_condition_mask(
    frame: pd.DataFrame,
    side: str,
    days: int,
    require_extreme: bool,
) -> pd.Series:
    if require_extreme:
        return frame["連續3日大賣" if side == "sell" else "連續3日大買"].fillna(False)
    return frame[f"連續{days}日{'賣超' if side == 'sell' else '買超'}"].fillna(False)


def _classifier_stats(
    frame: pd.DataFrame,
    target_column: str,
    up_value: str,
    down_value: str,
    days: int,
    require_extreme: bool,
) -> dict[str, Any]:
    valid = frame[target_column].isin([up_value, down_value])
    sell = valid & _pair_condition_mask(frame, "sell", days, require_extreme)
    buy = valid & _pair_condition_mask(frame, "buy", days, require_extreme)
    signaled = sell | buy
    correct = (sell & frame[target_column].eq(down_value)) | (buy & frame[target_column].eq(up_value))
    signal_count = int(signaled.sum())
    correct_count = int(correct.sum())
    accuracy = _rate(correct_count, signal_count)
    low_ci, high_ci = _wilson_interval(correct_count, signal_count)
    majority = frame.loc[valid, target_column].value_counts(normalize=True).max() if valid.any() else math.nan
    p_value = binomtest(correct_count, signal_count, 0.5).pvalue if signal_count else math.nan
    return {
        "極端訊號數": signal_count,
        "預測正確數": correct_count,
        "正確率_pct": accuracy * 100 if np.isfinite(accuracy) else math.nan,
        "正確率95CI下限_pct": low_ci * 100 if np.isfinite(low_ci) else math.nan,
        "正確率95CI上限_pct": high_ci * 100 if np.isfinite(high_ci) else math.nan,
        "多數類基準_pct": majority * 100 if np.isfinite(majority) else math.nan,
        "相對多數類增減_百分點": (accuracy - majority) * 100 if np.isfinite(accuracy) and np.isfinite(majority) else math.nan,
        "二項檢定_p值_vs50": p_value,
    }


def build_classifier_summary(frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    dates = frame["結果日"].dt.date
    periods = {
        "全期": frame,
        "探索期": frame[dates < settings.split_date],
        "驗證期": frame[dates >= settings.split_date],
    }
    conditions = [
        (f"連續{days}日同向", days, False) for days in STREAK_WINDOWS
    ] + [("連續3日同向＋3日累計Q1/Q5", 3, True)]
    targets = [
        ("開收方向", "隔日開收方向", "上漲", "下跌"),
        ("極值先後走勢", "隔日日盤走勢", "漲勢盤", "跌勢盤"),
    ]
    rows: list[dict[str, Any]] = []
    for condition_name, days, require_extreme in conditions:
        for definition, column, up_value, down_value in targets:
            for period, group in periods.items():
                rows.append({
                    "條件名稱": condition_name,
                    "連續日數": days,
                    "要求累計極端": require_extreme,
                    "方向定義": definition,
                    "期間": period,
                    **_classifier_stats(
                        group, column, up_value, down_value, days, require_extreme
                    ),
                })
    return pd.DataFrame(rows)


def build_detail(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "訊號日", "結果日", FOREIGN_COLUMN, "外資3日累計買賣超_億",
        "外資3日累計買賣超率_pct", "外資歷史分位_pct", "外資強度層",
        "連續2日買超", "連續2日賣超", "連續3日買超", "連續3日賣超",
        "連續5日買超", "連續5日賣超", "連續3日大買", "連續3日大賣",
        "隔日日盤開", "隔日日盤收", "隔日開收報酬_pct",
        "隔日開收方向", "隔日日盤走勢",
    ]
    return frame[[column for column in columns if column in frame.columns]].copy()


def export_results(
    summary: pd.DataFrame,
    yearly: pd.DataFrame,
    classifier: pd.DataFrame,
    detail: pd.DataFrame,
    rules: list[StreakRule],
    settings: Settings,
    output_dir: Path,
) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{settings.start:%Y%m%d}_{settings.end:%Y%m%d}" if settings.end else f"{settings.start:%Y%m%d}_latest"
    paths = (
        output_dir / f"foreign_flow_streak_direction_summary_{suffix}.csv",
        output_dir / f"foreign_flow_streak_direction_yearly_{suffix}.csv",
        output_dir / f"foreign_flow_streak_direction_classifier_{suffix}.csv",
        output_dir / f"foreign_flow_streak_direction_detail_{suffix}.csv",
        output_dir / f"foreign_flow_streak_direction_manifest_{suffix}.json",
    )
    summary.to_csv(paths[0], index=False, encoding="utf-8-sig")
    yearly.to_csv(paths[1], index=False, encoding="utf-8-sig")
    classifier.to_csv(paths[2], index=False, encoding="utf-8-sig")
    detail.to_csv(paths[3], index=False, encoding="utf-8-sig")
    paths[4].write_text(json.dumps({
        "analysis_name": ANALYSIS_NAME,
        "metric_version": METRIC_VERSION,
        "run_id": str(uuid.uuid4()),
        "settings": {
            **asdict(settings),
            "start": settings.start.isoformat(),
            "end": settings.end.isoformat() if settings.end else None,
            "split_date": settings.split_date.isoformat(),
        },
        "foreign_source_table": STOCK_SOURCE_TABLE,
        "foreign_source_symbol": STOCK_SYMBOL,
        "streak_definitions": {
            "continuous": "最後N個交易日每天外資買賣超皆同號",
            "continuous_large": (
                "連續3日每天同向，且近3日外資買賣超合計／近3日市場成交金額合計"
                "位於此前252筆歷史的Q1大賣或Q5大買"
            ),
        },
        "rules": [asdict(rule) for rule in rules],
        "warnings": [
            "連續買賣超只代表每日正負號相同；連續大買大賣另要求3日累計幅度進入Q5或Q1",
            "相鄰訊號高度重疊，p值必須搭配探索驗證及逐年一致性解讀",
            "本分析只檢驗隔日日盤方向，不代表交易損益",
            "外資資料使用訊號日收盤後已知資訊，不使用結果日資料",
        ],
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="外資連續買賣超與隔日日盤方向統計")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--split-date", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--history-window", type=int, default=252)
    parser.add_argument("--minimum-history", type=int, default=60)
    parser.add_argument("--foreign-days", type=int, default=3)
    parser.add_argument("--lower-percentile", type=float, default=20.0)
    parser.add_argument("--upper-percentile", type=float, default=80.0)
    parser.add_argument("--minimum-rule-sample", type=int, default=20)
    parser.add_argument("--minimum-year-rule-sample", type=int, default=5)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path("../output/analysis/futures_foreign_flow_streak_direction_scan"),
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
        foreign_days=args.foreign_days,
        lower_percentile=args.lower_percentile,
        upper_percentile=args.upper_percentile,
        minimum_rule_sample=args.minimum_rule_sample,
        minimum_year_rule_sample=args.minimum_year_rule_sample,
        alpha=args.alpha,
    )
    settings.validate()
    source = load_source_with_foreign(settings)
    foreign_feature = build_foreign_feature(source, settings)
    direction_frame = prepare_direction_frame(source, foreign_feature, settings)
    frame = add_streak_features(source, direction_frame, settings)
    rules = build_rules(settings)
    yearly = build_yearly(frame, rules, settings)
    summary = build_summary(frame, rules, yearly, settings)
    classifier = build_classifier_summary(frame, settings)
    detail = build_detail(frame)
    paths = export_results(summary, yearly, classifier, detail, rules, settings, args.output_dir)

    print(f"有效研究日：{len(frame):,} 日")
    print(f"連續條件方向規則：{len(rules)} 組")
    print(f"方向候選：{int(summary['連續外資方向候選'].sum())} 組")
    display = [
        "規則代碼", "規則名稱", "驗證期_符合數", "驗證期_命中率_pct",
        "驗證期_其餘日命中率_pct", "驗證期_相對其餘日Lift",
        "驗證期_Fisher_p值", "連續外資方向候選",
    ]
    print("\n驗證期結果：")
    print(summary[display].to_string(index=False))
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
