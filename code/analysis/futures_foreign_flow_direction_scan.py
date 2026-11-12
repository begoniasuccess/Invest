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
from backtest.futures_raw_signal_strength_scan import BUCKET_LABELS
from backtest.futures_strategy_next_day_field_scan import _bh_adjust


ANALYSIS_NAME = "futures_foreign_flow_direction_scan"
METRIC_VERSION = "1.0.0"


@dataclass(frozen=True)
class DirectionRule:
    rule_id: str
    name: str
    foreign_side: str
    target_column: str
    target_value: str
    target_name: str


def build_rules(settings: Settings) -> list[DirectionRule]:
    days = settings.foreign_days
    return [
        DirectionRule(
            "D01", f"外資{days}日累計大賣Q1→隔日日盤下跌", "sell",
            "隔日開收方向", "下跌", "隔日日盤開收到收盤下跌",
        ),
        DirectionRule(
            "D02", f"外資{days}日累計大買Q5→隔日日盤上漲", "buy",
            "隔日開收方向", "上漲", "隔日日盤開收到收盤上漲",
        ),
        DirectionRule(
            "D03", f"外資{days}日累計大賣Q1→隔日跌勢盤", "sell",
            "隔日日盤走勢", "跌勢盤", "隔日日盤為跌勢盤",
        ),
        DirectionRule(
            "D04", f"外資{days}日累計大買Q5→隔日漲勢盤", "buy",
            "隔日日盤走勢", "漲勢盤", "隔日日盤為漲勢盤",
        ),
    ]


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce").astype(float)


def prepare_direction_frame(
    source: pd.DataFrame,
    foreign_feature: pd.DataFrame,
    settings: Settings,
) -> pd.DataFrame:
    ordered = source.sort_values("日期").reset_index(drop=True).copy()
    day_open = _numeric(ordered, "日盤開").shift(-1)
    day_close = _numeric(ordered, "日盤收").shift(-1)
    result_date = pd.to_datetime(ordered["日期"]).shift(-1)
    return_pct = (day_close - day_open).div(day_open.replace(0, np.nan)) * 100.0

    close_direction = pd.Series(pd.NA, index=ordered.index, dtype="object")
    close_direction.loc[return_pct.gt(0)] = "上漲"
    close_direction.loc[return_pct.lt(0)] = "下跌"
    close_direction.loc[return_pct.eq(0)] = "平盤"

    trend = ordered["日盤走勢"].astype("string").str.strip().shift(-1)
    trend = trend.where(trend.isin(["漲勢盤", "跌勢盤"]))

    frame = pd.DataFrame({
        "訊號日": pd.to_datetime(ordered["日期"]),
        "結果日": result_date,
        "隔日日盤開": day_open,
        "隔日日盤收": day_close,
        "隔日開收報酬_pct": return_pct,
        "隔日開收方向": close_direction,
        "隔日日盤走勢": trend,
    })
    frame = frame.merge(foreign_feature, on="訊號日", how="left", validate="one_to_one")
    start_ts = pd.Timestamp(settings.start)
    end_ts = pd.Timestamp(settings.end or date.today())
    frame = frame[frame["結果日"].between(start_ts, end_ts)].copy()
    return frame.sort_values("結果日").reset_index(drop=True)


def _rate(events: int, count: int) -> float:
    return events / count if count else math.nan


def _rule_masks(
    frame: pd.DataFrame,
    rule: DirectionRule,
    settings: Settings,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    valid_values = (
        {"上漲", "下跌"}
        if rule.target_column == "隔日開收方向"
        else {"漲勢盤", "跌勢盤"}
    )
    eligible = frame["外資歷史分位_pct"].notna() & frame[rule.target_column].isin(valid_values)
    matched = eligible & (
        frame["外資歷史分位_pct"].le(settings.lower_percentile)
        if rule.foreign_side == "sell"
        else frame["外資歷史分位_pct"].ge(settings.upper_percentile)
    )
    event = frame[rule.target_column].eq(rule.target_value)
    return eligible, matched, event


def _period_stats(frame: pd.DataFrame, rule: DirectionRule, settings: Settings) -> dict[str, Any]:
    eligible, matched, event = _rule_masks(frame, rule, settings)
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
        odds_ratio, p_value = fisher_exact(
            [
                [match_events, match_count - match_events],
                [rest_events, rest_count - rest_events],
            ],
            alternative="two-sided",
        )

    matched_return = pd.to_numeric(
        frame.loc[matched, "隔日開收報酬_pct"], errors="coerce"
    )
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
        "其餘日命中數": rest_events,
        "其餘日命中率_pct": rest_rate * 100 if np.isfinite(rest_rate) else math.nan,
        "相對母體Lift": match_rate / universe_rate if np.isfinite(match_rate) and universe_rate > 0 else math.nan,
        "相對其餘日Lift": match_rate / rest_rate if np.isfinite(match_rate) and rest_rate > 0 else math.nan,
        "相對其餘日增減_百分點": (
            (match_rate - rest_rate) * 100
            if np.isfinite(match_rate) and np.isfinite(rest_rate) else math.nan
        ),
        "符合對其餘日勝算比": odds_ratio,
        "Fisher_p值": p_value,
        "符合日平均開收報酬_pct": matched_return.mean(),
        "符合日中位開收報酬_pct": matched_return.median(),
    }


def build_yearly(
    frame: pd.DataFrame,
    rules: list[DirectionRule],
    settings: Settings,
) -> pd.DataFrame:
    work = frame.copy()
    work["年"] = work["結果日"].dt.year
    rows: list[dict[str, Any]] = []
    for rule in rules:
        for year, group in work.groupby("年", sort=True):
            stats = _period_stats(group, rule, settings)
            rows.append({
                "規則代碼": rule.rule_id,
                "規則名稱": rule.name,
                "目標": rule.target_name,
                "年": int(year),
                **stats,
                "年度樣本足夠": stats["符合數"] >= settings.minimum_year_rule_sample,
            })
    return pd.DataFrame(rows)


def build_summary(
    frame: pd.DataFrame,
    rules: list[DirectionRule],
    yearly: pd.DataFrame,
    settings: Settings,
) -> pd.DataFrame:
    result_dates = frame["結果日"].dt.date
    periods = {
        "全期": frame,
        "探索期": frame[result_dates < settings.split_date],
        "驗證期": frame[result_dates >= settings.split_date],
    }
    rows: list[dict[str, Any]] = []
    for rule in rules:
        row: dict[str, Any] = {
            "規則代碼": rule.rule_id,
            "規則名稱": rule.name,
            "外資方向": "大賣" if rule.foreign_side == "sell" else "大買",
            "方向定義": "開收方向" if rule.target_column == "隔日開收方向" else "極值先後走勢",
            "預測目標": rule.target_name,
        }
        for period, group in periods.items():
            stats = _period_stats(group, rule, settings)
            row.update({f"{period}_{key}": value for key, value in stats.items()})
        rows.append(row)

    summary = pd.DataFrame(rows)
    summary["全期_Fisher_q值"] = _bh_adjust(summary["全期_Fisher_p值"])
    annual_info: list[tuple[int, int, float]] = []
    for rule in rules:
        group = yearly[(yearly["規則代碼"] == rule.rule_id) & yearly["年度樣本足夠"]]
        comparable = group.dropna(subset=["相對其餘日Lift"])
        positive = int(comparable["相對其餘日Lift"].gt(1.0).sum())
        annual_info.append((
            len(comparable),
            positive,
            positive / len(comparable) if len(comparable) else math.nan,
        ))
    summary[["可比較年度數", "優於其餘日年度數", "年度正向率"]] = pd.DataFrame(
        annual_info, index=summary.index
    )
    summary["探索驗證皆正向"] = (
        summary["探索期_相對其餘日Lift"].gt(1.0)
        & summary["驗證期_相對其餘日Lift"].gt(1.0)
    )
    summary["方向候選"] = (
        summary["全期_Fisher_q值"].lt(settings.alpha)
        & summary["探索驗證皆正向"]
        & summary["驗證期_Fisher_p值"].lt(0.10)
        & summary["驗證期_符合數"].ge(settings.minimum_rule_sample)
        & summary["年度正向率"].ge(0.60)
    )
    return summary.sort_values(
        ["方向候選", "驗證期_相對其餘日Lift", "驗證期_符合數"],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True)


def build_bucket_stats(frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    result_dates = frame["結果日"].dt.date
    periods = {
        "全期": frame,
        "探索期": frame[result_dates < settings.split_date],
        "驗證期": frame[result_dates >= settings.split_date],
    }
    rows: list[dict[str, Any]] = []
    for period, data in periods.items():
        for bucket_no, bucket in enumerate(BUCKET_LABELS, start=1):
            group = data[data["外資強度層"].astype("string").eq(bucket)]
            close = group[group["隔日開收方向"].isin(["上漲", "下跌"])]
            trend = group[group["隔日日盤走勢"].isin(["漲勢盤", "跌勢盤"])]
            rows.append({
                "期間": period,
                "外資分位層序": bucket_no,
                "外資強度層": bucket,
                "開收方向樣本數": len(close),
                "隔日上漲數": int(close["隔日開收方向"].eq("上漲").sum()),
                "隔日上漲率_pct": close["隔日開收方向"].eq("上漲").mean() * 100 if len(close) else math.nan,
                "隔日下跌率_pct": close["隔日開收方向"].eq("下跌").mean() * 100 if len(close) else math.nan,
                "隔日平均開收報酬_pct": close["隔日開收報酬_pct"].mean(),
                "隔日中位開收報酬_pct": close["隔日開收報酬_pct"].median(),
                "走勢樣本數": len(trend),
                "隔日漲勢盤率_pct": trend["隔日日盤走勢"].eq("漲勢盤").mean() * 100 if len(trend) else math.nan,
                "隔日跌勢盤率_pct": trend["隔日日盤走勢"].eq("跌勢盤").mean() * 100 if len(trend) else math.nan,
            })
    return pd.DataFrame(rows)


def _classifier_stats(
    frame: pd.DataFrame,
    target_column: str,
    up_value: str,
    down_value: str,
    settings: Settings,
) -> dict[str, Any]:
    valid = frame[target_column].isin([up_value, down_value]) & frame["外資歷史分位_pct"].notna()
    sell = valid & frame["外資歷史分位_pct"].le(settings.lower_percentile)
    buy = valid & frame["外資歷史分位_pct"].ge(settings.upper_percentile)
    signaled = sell | buy
    correct = (sell & frame[target_column].eq(down_value)) | (buy & frame[target_column].eq(up_value))
    signal_count = int(signaled.sum())
    correct_count = int(correct.sum())
    accuracy = _rate(correct_count, signal_count)
    low_ci, high_ci = _wilson_interval(correct_count, signal_count)
    sell_count = int(sell.sum())
    buy_count = int(buy.sum())
    sell_hit = _rate(int((sell & frame[target_column].eq(down_value)).sum()), sell_count)
    buy_hit = _rate(int((buy & frame[target_column].eq(up_value)).sum()), buy_count)
    eligible_values = frame.loc[valid, target_column]
    majority = eligible_values.value_counts(normalize=True).max() if len(eligible_values) else math.nan
    p_value = binomtest(correct_count, signal_count, 0.5).pvalue if signal_count else math.nan
    return {
        "可判定日數": int(valid.sum()),
        "極端訊號數": signal_count,
        "預測正確數": correct_count,
        "正確率_pct": accuracy * 100 if np.isfinite(accuracy) else math.nan,
        "正確率95CI下限_pct": low_ci * 100 if np.isfinite(low_ci) else math.nan,
        "正確率95CI上限_pct": high_ci * 100 if np.isfinite(high_ci) else math.nan,
        "多數類基準_pct": majority * 100 if np.isfinite(majority) else math.nan,
        "相對多數類增減_百分點": (accuracy - majority) * 100 if np.isfinite(accuracy) and np.isfinite(majority) else math.nan,
        "大賣訊號數": sell_count,
        "大賣預測向下命中率_pct": sell_hit * 100 if np.isfinite(sell_hit) else math.nan,
        "大買訊號數": buy_count,
        "大買預測向上命中率_pct": buy_hit * 100 if np.isfinite(buy_hit) else math.nan,
        "平衡正確率_pct": (sell_hit + buy_hit) / 2 * 100 if np.isfinite(sell_hit) and np.isfinite(buy_hit) else math.nan,
        "二項檢定_p值_vs50": p_value,
    }


def build_classifier_summary(frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    result_dates = frame["結果日"].dt.date
    periods = {
        "全期": frame,
        "探索期": frame[result_dates < settings.split_date],
        "驗證期": frame[result_dates >= settings.split_date],
    }
    definitions = [
        ("開收方向", "隔日開收方向", "上漲", "下跌"),
        ("極值先後走勢", "隔日日盤走勢", "漲勢盤", "跌勢盤"),
    ]
    rows: list[dict[str, Any]] = []
    for definition, column, up_value, down_value in definitions:
        for period, group in periods.items():
            rows.append({
                "方向定義": definition,
                "期間": period,
                **_classifier_stats(group, column, up_value, down_value, settings),
            })
    return pd.DataFrame(rows)


def build_detail(frame: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    detail = frame.copy()
    detail["外資大賣Q1"] = detail["外資歷史分位_pct"].le(settings.lower_percentile)
    detail["外資大買Q5"] = detail["外資歷史分位_pct"].ge(settings.upper_percentile)
    detail["外資極端訊號_開收預測"] = np.select(
        [detail["外資大賣Q1"], detail["外資大買Q5"]],
        ["預測下跌", "預測上漲"],
        default="無訊號",
    )
    detail["外資極端訊號_開收正確"] = pd.Series(pd.NA, index=detail.index, dtype="boolean")
    signaled = detail["外資大賣Q1"] | detail["外資大買Q5"]
    valid = detail["隔日開收方向"].isin(["上漲", "下跌"])
    detail.loc[signaled & valid, "外資極端訊號_開收正確"] = (
        (detail["外資大賣Q1"] & detail["隔日開收方向"].eq("下跌"))
        | (detail["外資大買Q5"] & detail["隔日開收方向"].eq("上漲"))
    )
    return detail


def export_results(
    frame: pd.DataFrame,
    summary: pd.DataFrame,
    yearly: pd.DataFrame,
    buckets: pd.DataFrame,
    classifier: pd.DataFrame,
    detail: pd.DataFrame,
    rules: list[DirectionRule],
    settings: Settings,
    output_dir: Path,
) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{settings.start:%Y%m%d}_{settings.end:%Y%m%d}" if settings.end else f"{settings.start:%Y%m%d}_latest"
    paths = (
        output_dir / f"foreign_flow_direction_summary_{suffix}.csv",
        output_dir / f"foreign_flow_direction_yearly_{suffix}.csv",
        output_dir / f"foreign_flow_direction_buckets_{suffix}.csv",
        output_dir / f"foreign_flow_direction_classifier_{suffix}.csv",
        output_dir / f"foreign_flow_direction_detail_{suffix}.csv",
        output_dir / f"foreign_flow_direction_manifest_{suffix}.json",
    )
    summary.to_csv(paths[0], index=False, encoding="utf-8-sig")
    yearly.to_csv(paths[1], index=False, encoding="utf-8-sig")
    buckets.to_csv(paths[2], index=False, encoding="utf-8-sig")
    classifier.to_csv(paths[3], index=False, encoding="utf-8-sig")
    detail.to_csv(paths[4], index=False, encoding="utf-8-sig")
    paths[5].write_text(json.dumps({
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
        "source_columns": {
            "foreign_net_buy_sell": FOREIGN_COLUMN,
            "market_turnover": TURNOVER_COLUMN,
        },
        "foreign_feature_formula": (
            f"近{settings.foreign_days}日外資買賣超合計／"
            f"近{settings.foreign_days}日市場成交金額合計×100"
        ),
        "direction_definitions": {
            "open_close": "隔日日盤收盤相對隔日日盤開盤；正值上漲、負值下跌",
            "extreme_order": "隔日日盤最高點晚於最低點為漲勢盤，反之為跌勢盤",
        },
        "rules": [asdict(rule) for rule in rules],
        "candidate_rule": (
            "全期Fisher檢定BH-FDR q<0.05、探索與驗證相對其餘日Lift皆>1、"
            "驗證期Fisher p<0.10、驗證期符合數達門檻、年度正向率>=60%"
        ),
        "warnings": [
            "本程式研究隔日日盤方向，不代表任何進出場策略或交易損益",
            "外資資料只用訊號日收盤後已知資訊預測下一交易日，不使用結果日資料",
            "近3日累計訊號會讓相鄰日期重疊，Fisher與二項檢定p值應搭配探索驗證及逐年一致性解讀",
            "外資買賣與行情可能受共同因素影響，結果代表關聯而非因果",
            "平盤不納入開收二元方向檢定，無合法日盤走勢標籤者不納入走勢檢定",
        ],
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="外資近3日累計買賣超與隔日日盤方向統計")
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
        "--output-dir",
        type=Path,
        default=Path("../output/analysis/futures_foreign_flow_direction_scan"),
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
    frame = prepare_direction_frame(source, foreign_feature, settings)
    rules = build_rules(settings)
    yearly = build_yearly(frame, rules, settings)
    summary = build_summary(frame, rules, yearly, settings)
    buckets = build_bucket_stats(frame, settings)
    classifier = build_classifier_summary(frame, settings)
    detail = build_detail(frame, settings)
    paths = export_results(
        frame, summary, yearly, buckets, classifier, detail, rules, settings, args.output_dir
    )

    valid_close = frame["隔日開收方向"].isin(["上漲", "下跌"]).sum()
    valid_trend = frame["隔日日盤走勢"].isin(["漲勢盤", "跌勢盤"]).sum()
    print(f"有效開收方向研究日：{valid_close:,} 日")
    print(f"有效漲跌勢盤研究日：{valid_trend:,} 日")
    print(f"方向規則：{len(rules)} 組")
    print(f"方向候選：{int(summary['方向候選'].sum())} 組")
    display = [
        "規則代碼", "規則名稱", "驗證期_符合數", "驗證期_命中率_pct",
        "驗證期_其餘日命中率_pct", "驗證期_相對其餘日Lift",
        "驗證期_Fisher_p值", "方向候選",
    ]
    print("\n驗證期規則結果：")
    print(summary[display].to_string(index=False))
    print("\n極端外資訊號整體方向分類：")
    print(classifier[classifier["期間"].eq("驗證期")].to_string(index=False))
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
