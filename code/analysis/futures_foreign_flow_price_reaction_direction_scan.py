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
    Settings,
    _wilson_interval,
    build_foreign_feature,
    load_source_with_foreign,
)
from analysis.futures_foreign_flow_direction_scan import prepare_direction_frame
from analysis.futures_foreign_flow_streak_direction_scan import add_streak_features
from backtest.futures_strategy_next_day_field_scan import _bh_adjust


ANALYSIS_NAME = "futures_foreign_flow_price_reaction_direction_scan"
METRIC_VERSION = "1.0.0"
REACTION_WINDOWS = (1, 3)


@dataclass(frozen=True)
class ReactionRule:
    rule_id: str
    name: str
    foreign_side: str
    reaction_window: int
    reaction_type: str
    target_column: str
    target_value: str
    target_name: str


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce").astype(float)


def add_price_reaction_features(
    source: pd.DataFrame,
    streak_frame: pd.DataFrame,
) -> pd.DataFrame:
    """Add signal-day price reactions without using any result-day value."""
    ordered = source.sort_values("日期").reset_index(drop=True).copy()
    day_open = _numeric(ordered, "日盤開")
    day_close = _numeric(ordered, "日盤收")
    first_open_3d = day_open.shift(2)
    features = pd.DataFrame({
        "訊號日": pd.to_datetime(ordered["日期"]),
        "當日日盤開收反應_pct": (
            (day_close - day_open).div(day_open.replace(0, np.nan)) * 100.0
        ),
        "近3日日盤價格反應_pct": (
            (day_close - first_open_3d).div(first_open_3d.replace(0, np.nan)) * 100.0
        ),
    })
    return streak_frame.merge(features, on="訊號日", how="left", validate="one_to_one")


def _reaction_column(window: int) -> str:
    if window == 1:
        return "當日日盤開收反應_pct"
    if window == 3:
        return "近3日日盤價格反應_pct"
    raise ValueError(f"不支援的價格反應期間：{window}")


def build_rules() -> list[ReactionRule]:
    rules: list[ReactionRule] = []
    rule_no = 1
    target_specs = [
        ("隔日開收方向", "上漲", "下跌", "隔日日盤開收到收盤"),
        ("隔日日盤走勢", "漲勢盤", "跌勢盤", "隔日日盤極值先後走勢"),
    ]
    for window in REACTION_WINDOWS:
        for side, side_name in (("sell", "連續大賣"), ("buy", "連續大買")):
            for reaction_type, reaction_name in (("confirm", "價格同向確認"), ("absorb", "價格背離吸收")):
                for target_column, up_value, down_value, target_label in target_specs:
                    if reaction_type == "confirm":
                        target_value = down_value if side == "sell" else up_value
                    else:
                        target_value = up_value if side == "sell" else down_value
                    rules.append(ReactionRule(
                        rule_id=f"R{rule_no:02d}",
                        name=f"{side_name}＋{window}日{reaction_name}→{target_value}",
                        foreign_side=side,
                        reaction_window=window,
                        reaction_type=reaction_type,
                        target_column=target_column,
                        target_value=target_value,
                        target_name=f"{target_label}{target_value}",
                    ))
                    rule_no += 1
    return rules


def _valid_target(frame: pd.DataFrame, rule: ReactionRule) -> pd.Series:
    values = (
        {"上漲", "下跌"}
        if rule.target_column == "隔日開收方向"
        else {"漲勢盤", "跌勢盤"}
    )
    return frame[rule.target_column].isin(values)


def _foreign_mask(frame: pd.DataFrame, side: str) -> pd.Series:
    column = "連續3日大賣" if side == "sell" else "連續3日大買"
    return frame[column].fillna(False).astype(bool)


def _reaction_mask(
    frame: pd.DataFrame,
    side: str,
    window: int,
    reaction_type: str,
) -> pd.Series:
    reaction = pd.to_numeric(frame[_reaction_column(window)], errors="coerce")
    if reaction_type == "confirm":
        return reaction.lt(0) if side == "sell" else reaction.gt(0)
    if reaction_type == "absorb":
        return reaction.ge(0) if side == "sell" else reaction.le(0)
    raise ValueError(f"不支援的價格反應類型：{reaction_type}")


def _rate(events: int, count: int) -> float:
    return events / count if count else math.nan


def _period_stats(frame: pd.DataFrame, rule: ReactionRule) -> dict[str, Any]:
    reaction = pd.to_numeric(
        frame[_reaction_column(rule.reaction_window)], errors="coerce"
    )
    reference = (
        _valid_target(frame, rule)
        & _foreign_mask(frame, rule.foreign_side)
        & reaction.notna()
    )
    matched = reference & _reaction_mask(
        frame, rule.foreign_side, rule.reaction_window, rule.reaction_type
    )
    rest = reference & ~matched
    event = frame[rule.target_column].eq(rule.target_value)

    reference_count = int(reference.sum())
    reference_events = int((reference & event).sum())
    match_count = int(matched.sum())
    match_events = int((matched & event).sum())
    rest_count = int(rest.sum())
    rest_events = int((rest & event).sum())
    reference_rate = _rate(reference_events, reference_count)
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
        "同側外資參考組數": reference_count,
        "同側外資參考組命中數": reference_events,
        "同側外資參考組命中率_pct": (
            reference_rate * 100 if np.isfinite(reference_rate) else math.nan
        ),
        "符合數": match_count,
        "命中數": match_events,
        "命中率_pct": match_rate * 100 if np.isfinite(match_rate) else math.nan,
        "命中率95CI下限_pct": low_ci * 100 if np.isfinite(low_ci) else math.nan,
        "命中率95CI上限_pct": high_ci * 100 if np.isfinite(high_ci) else math.nan,
        "相反價格反應數": rest_count,
        "相反價格反應命中率_pct": rest_rate * 100 if np.isfinite(rest_rate) else math.nan,
        "相對相反價格反應Lift": (
            match_rate / rest_rate
            if np.isfinite(match_rate) and rest_rate > 0 else math.nan
        ),
        "相對相反價格反應增減_百分點": (
            (match_rate - rest_rate) * 100
            if np.isfinite(match_rate) and np.isfinite(rest_rate) else math.nan
        ),
        "符合對相反價格反應勝算比": odds_ratio,
        "Fisher_p值": p_value,
        "符合日平均開收報酬_pct": returns.mean(),
        "符合日中位開收報酬_pct": returns.median(),
    }


def build_yearly(
    frame: pd.DataFrame,
    rules: list[ReactionRule],
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
                "外資方向": "大賣" if rule.foreign_side == "sell" else "大買",
                "價格反應期間": rule.reaction_window,
                "價格反應類型": "同向確認" if rule.reaction_type == "confirm" else "背離吸收",
                "方向定義": "開收方向" if rule.target_column == "隔日開收方向" else "極值先後走勢",
                "年": int(year),
                **stats,
                "年度樣本足夠": stats["符合數"] >= settings.minimum_year_rule_sample,
            })
    return pd.DataFrame(rows)


def build_summary(
    frame: pd.DataFrame,
    rules: list[ReactionRule],
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
            "外資方向": "大賣" if rule.foreign_side == "sell" else "大買",
            "價格反應期間": rule.reaction_window,
            "價格反應類型": "同向確認" if rule.reaction_type == "confirm" else "背離吸收",
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
        group = yearly[
            (yearly["規則代碼"] == rule.rule_id) & yearly["年度樣本足夠"]
        ].dropna(subset=["相對相反價格反應Lift"])
        positive = int(group["相對相反價格反應Lift"].gt(1.0).sum())
        annual_info.append((
            len(group), positive,
            positive / len(group) if len(group) else math.nan,
        ))
    summary[["可比較年度數", "優於相反反應年度數", "年度正向率"]] = pd.DataFrame(
        annual_info, index=summary.index
    )
    summary["探索驗證皆正向"] = (
        summary["探索期_相對相反價格反應Lift"].gt(1.0)
        & summary["驗證期_相對相反價格反應Lift"].gt(1.0)
    )
    summary["價格反應方向候選"] = (
        summary["全期_Fisher_q值"].lt(settings.alpha)
        & summary["探索驗證皆正向"]
        & summary["驗證期_Fisher_p值"].lt(0.10)
        & summary["驗證期_符合數"].ge(settings.minimum_rule_sample)
        & summary["年度正向率"].ge(0.60)
    )
    return summary.sort_values(
        ["價格反應方向候選", "驗證期_相對相反價格反應Lift", "驗證期_符合數"],
        ascending=[False, False, False], na_position="last",
    ).reset_index(drop=True)


def _classifier_stats(
    frame: pd.DataFrame,
    reaction_window: int,
    reaction_type: str,
    target_column: str,
    up_value: str,
    down_value: str,
) -> dict[str, Any]:
    valid = frame[target_column].isin([up_value, down_value])
    sell = (
        valid
        & _foreign_mask(frame, "sell")
        & _reaction_mask(frame, "sell", reaction_window, reaction_type)
    )
    buy = (
        valid
        & _foreign_mask(frame, "buy")
        & _reaction_mask(frame, "buy", reaction_window, reaction_type)
    )
    signaled = sell | buy
    if reaction_type == "confirm":
        correct = (
            (sell & frame[target_column].eq(down_value))
            | (buy & frame[target_column].eq(up_value))
        )
    else:
        correct = (
            (sell & frame[target_column].eq(up_value))
            | (buy & frame[target_column].eq(down_value))
        )
    signal_count = int(signaled.sum())
    correct_count = int(correct.sum())
    accuracy = _rate(correct_count, signal_count)
    low_ci, high_ci = _wilson_interval(correct_count, signal_count)
    majority = frame.loc[valid, target_column].value_counts(normalize=True).max() if valid.any() else math.nan
    p_value = binomtest(correct_count, signal_count, 0.5).pvalue if signal_count else math.nan
    return {
        "訊號數": signal_count,
        "預測正確數": correct_count,
        "正確率_pct": accuracy * 100 if np.isfinite(accuracy) else math.nan,
        "正確率95CI下限_pct": low_ci * 100 if np.isfinite(low_ci) else math.nan,
        "正確率95CI上限_pct": high_ci * 100 if np.isfinite(high_ci) else math.nan,
        "多數類基準_pct": majority * 100 if np.isfinite(majority) else math.nan,
        "相對多數類增減_百分點": (
            (accuracy - majority) * 100
            if np.isfinite(accuracy) and np.isfinite(majority) else math.nan
        ),
        "二項檢定_p值_vs50": p_value,
    }


def build_classifier_summary(frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    dates = frame["結果日"].dt.date
    periods = {
        "全期": frame,
        "探索期": frame[dates < settings.split_date],
        "驗證期": frame[dates >= settings.split_date],
    }
    targets = [
        ("開收方向", "隔日開收方向", "上漲", "下跌"),
        ("極值先後走勢", "隔日日盤走勢", "漲勢盤", "跌勢盤"),
    ]
    rows: list[dict[str, Any]] = []
    for window in REACTION_WINDOWS:
        for reaction_type, reaction_name in (("confirm", "同向確認"), ("absorb", "背離吸收")):
            for definition, column, up_value, down_value in targets:
                for period, group in periods.items():
                    rows.append({
                        "價格反應期間": window,
                        "價格反應類型": reaction_name,
                        "預測邏輯": "順外資方向" if reaction_type == "confirm" else "反外資方向",
                        "方向定義": definition,
                        "期間": period,
                        **_classifier_stats(
                            group, window, reaction_type, column, up_value, down_value
                        ),
                    })
    return pd.DataFrame(rows)


def build_detail(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "訊號日", "結果日", FOREIGN_COLUMN,
        "外資3日累計買賣超_億", "外資3日累計買賣超率_pct",
        "外資歷史分位_pct", "外資強度層",
        "連續3日大買", "連續3日大賣",
        "當日日盤開收反應_pct", "近3日日盤價格反應_pct",
        "隔日日盤開", "隔日日盤收", "隔日開收報酬_pct",
        "隔日開收方向", "隔日日盤走勢",
    ]
    return frame[[column for column in columns if column in frame.columns]].copy()


def export_results(
    summary: pd.DataFrame,
    yearly: pd.DataFrame,
    classifier: pd.DataFrame,
    detail: pd.DataFrame,
    rules: list[ReactionRule],
    settings: Settings,
    output_dir: Path,
) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = (
        f"{settings.start:%Y%m%d}_{settings.end:%Y%m%d}"
        if settings.end else f"{settings.start:%Y%m%d}_latest"
    )
    paths = (
        output_dir / f"foreign_flow_price_reaction_direction_summary_{suffix}.csv",
        output_dir / f"foreign_flow_price_reaction_direction_yearly_{suffix}.csv",
        output_dir / f"foreign_flow_price_reaction_direction_classifier_{suffix}.csv",
        output_dir / f"foreign_flow_price_reaction_direction_detail_{suffix}.csv",
        output_dir / f"foreign_flow_price_reaction_direction_manifest_{suffix}.json",
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
        "foreign_condition": (
            "連續3個交易日每天同向，且3日累計外資買賣超率位於"
            "此前252筆歷史的Q1大賣或Q5大買"
        ),
        "price_reactions": {
            "1日": "訊號日日盤收盤相對訊號日日盤開盤",
            "3日": "訊號日日盤收盤相對最近3個交易日首日的日盤開盤",
            "同向確認": "大買且價格上漲，或大賣且價格下跌",
            "背離吸收": "大買但價格未上漲，或大賣但價格未下跌",
        },
        "comparison": "每一規則只與同側連續極端外資、但價格反應相反的日期比較",
        "rules": [asdict(rule) for rule in rules],
        "warnings": [
            "所有價格反應只使用訊號日以前及當日資料，不使用結果日資料",
            "相鄰連續外資訊號高度重疊，p值須搭配探索驗證與逐年一致性",
            "本分析只檢驗隔日日盤方向，不代表扣除成本後的交易損益",
            "1日與3日價格反應屬於預先指定的敏感度比較，需一併考慮多重檢定",
        ],
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="外資連續大買大賣與價格反應的隔日方向統計")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--split-date", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--history-window", type=int, default=252)
    parser.add_argument("--minimum-history", type=int, default=60)
    parser.add_argument("--foreign-days", type=int, choices=[3], default=3)
    parser.add_argument("--lower-percentile", type=float, default=20.0)
    parser.add_argument("--upper-percentile", type=float, default=80.0)
    parser.add_argument("--minimum-rule-sample", type=int, default=20)
    parser.add_argument("--minimum-year-rule-sample", type=int, default=5)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path("../output/analysis/futures_foreign_flow_price_reaction_direction_scan"),
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
    direction = prepare_direction_frame(source, foreign_feature, settings)
    streak = add_streak_features(source, direction, settings)
    frame = add_price_reaction_features(source, streak)
    rules = build_rules()
    yearly = build_yearly(frame, rules, settings)
    summary = build_summary(frame, rules, yearly, settings)
    classifier = build_classifier_summary(frame, settings)
    detail = build_detail(frame)
    paths = export_results(summary, yearly, classifier, detail, rules, settings, args.output_dir)

    print(f"有效研究日：{len(frame):,} 日")
    print(f"價格反應方向規則：{len(rules)} 組")
    print(f"方向候選：{int(summary['價格反應方向候選'].sum())} 組")
    display = [
        "規則代碼", "規則名稱", "驗證期_符合數", "驗證期_命中率_pct",
        "驗證期_相反價格反應命中率_pct", "驗證期_相對相反價格反應Lift",
        "驗證期_Fisher_p值", "價格反應方向候選",
    ]
    print("\n驗證期結果：")
    print(summary[display].to_string(index=False))
    print("\n整體分類器：")
    print(classifier[classifier["期間"].eq("驗證期")].to_string(index=False))
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
