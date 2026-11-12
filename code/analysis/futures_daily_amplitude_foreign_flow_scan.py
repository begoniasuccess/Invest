from __future__ import annotations

import argparse
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import fisher_exact

from analysis.futures_daily_amplitude_feature_scan import (
    EXTRA_SOURCE_COLUMNS,
    SOURCE_VIEW,
    Settings as FeatureSettings,
    load_targets,
    prepare_detail,
)
from backtest.futures_raw_signal_strength_scan import (
    SOURCE_COLUMNS,
    _rolling_percentile,
    _safe_ratio,
    _strength_bucket,
)
from backtest.futures_strategy_next_day_field_scan import _bh_adjust


ANALYSIS_NAME = "futures_daily_amplitude_foreign_flow_scan"
METRIC_VERSION = "1.0.0"
TARGET = "劇烈以上"
STOCK_SOURCE_TABLE = "core.stock_report_daily"
STOCK_SYMBOL = "TAIEX"
FOREIGN_COLUMN = "買超_外資_億"
TURNOVER_COLUMN = "總成交金額_億"
RECENT_RANGE_FEATURE = "近5日日盤振幅_前30日比"
NIGHT_RANGE_FEATURE = "夜盤振幅_前30日比"
BASE_RULE_NAME = "近期振幅＋夜盤振幅皆為Q5"


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    split_date: date = date(2023, 1, 1)
    history_window: int = 252
    minimum_history: int = 60
    foreign_days: int = 3
    lower_percentile: float = 20.0
    upper_percentile: float = 80.0
    minimum_rule_sample: int = 20
    minimum_year_rule_sample: int = 5
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
        if self.foreign_days < 2:
            raise ValueError("foreign_days 至少為2")
        if not 0 < self.lower_percentile < self.upper_percentile < 100:
            raise ValueError("外資分位門檻必須依序介於0與100之間")
        if self.minimum_rule_sample < 10:
            raise ValueError("minimum_rule_sample 至少為10")
        if self.minimum_year_rule_sample < 3:
            raise ValueError("minimum_year_rule_sample 至少為3")


@dataclass(frozen=True)
class Rule:
    rule_id: str
    name: str
    foreign_side: str
    require_base_rule: bool


def build_rules(settings: Settings) -> list[Rule]:
    days = settings.foreign_days
    return [
        Rule("F01", f"外資{days}日累計大賣Q1", "sell", False),
        Rule("F02", f"外資{days}日累計大買Q5", "buy", False),
        Rule("F03", f"主規則＋外資{days}日累計大賣Q1", "sell", True),
        Rule("F04", f"主規則＋外資{days}日累計大買Q5", "buy", True),
    ]


def _build_source_query(settings: Settings) -> tuple[str, tuple[Any, ...]]:
    query_start = settings.start - timedelta(days=max(400, settings.history_window * 2))
    futures_columns = tuple(dict.fromkeys((
        *SOURCE_COLUMNS,
        *EXTRA_SOURCE_COLUMNS,
    )))
    selected = ", ".join(f'f."{column}"' for column in futures_columns)
    sql = f'''SELECT f.futures_id, {selected},
                     stock."{FOREIGN_COLUMN}" AS "{FOREIGN_COLUMN}",
                     stock."{TURNOVER_COLUMN}" AS "{TURNOVER_COLUMN}"
              FROM {SOURCE_VIEW} AS f
              LEFT JOIN {STOCK_SOURCE_TABLE} AS stock
                ON stock."日期" = f."日期"
               AND stock."股票代號" = '{STOCK_SYMBOL}'
              WHERE f.futures_id = ? AND f."日期" >= ?'''
    params: list[Any] = [settings.futures_id, query_start]
    if settings.end:
        sql += ' AND f."日期" <= ?'
        params.append(settings.end)
    sql += ' ORDER BY f."日期"'
    return sql, tuple(params)


def load_source_with_foreign(settings: Settings) -> pd.DataFrame:
    from common import db

    settings.validate()
    sql, params = _build_source_query(settings)
    try:
        return db.sql_to_df(sql, params=params, parse_dates=["日期"])
    except Exception as exc:
        message = str(exc)
        if FOREIGN_COLUMN in message or TURNOVER_COLUMN in message or "does not exist" in message:
            raise RuntimeError(
                f"{STOCK_SOURCE_TABLE} 的 {STOCK_SYMBOL} 資料必須包含"
                f"「{FOREIGN_COLUMN}」與「{TURNOVER_COLUMN}」欄位"
            ) from exc
        raise


def build_foreign_feature(source: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    ordered = source.sort_values("日期").reset_index(drop=True).copy()
    foreign = pd.to_numeric(ordered[FOREIGN_COLUMN], errors="coerce")
    turnover = pd.to_numeric(ordered[TURNOVER_COLUMN], errors="coerce")
    foreign_sum = foreign.rolling(settings.foreign_days, min_periods=settings.foreign_days).sum()
    turnover_sum = turnover.rolling(settings.foreign_days, min_periods=settings.foreign_days).sum()
    ratio = _safe_ratio(foreign_sum, turnover_sum) * 100.0
    percentile = _rolling_percentile(ratio, settings.history_window, settings.minimum_history)

    buy_days = foreign.gt(0).rolling(settings.foreign_days, min_periods=settings.foreign_days).sum()
    sell_days = foreign.lt(0).rolling(settings.foreign_days, min_periods=settings.foreign_days).sum()
    return pd.DataFrame({
        "訊號日": pd.to_datetime(ordered["日期"]),
        "外資單日買賣超_億": foreign,
        f"外資{settings.foreign_days}日累計買賣超_億": foreign_sum,
        f"市場{settings.foreign_days}日累計成交金額_億": turnover_sum,
        f"外資{settings.foreign_days}日累計買賣超率_pct": ratio,
        "外資歷史分位_pct": percentile,
        "外資強度層": _strength_bucket(percentile),
        f"近{settings.foreign_days}日買超天數": buy_days,
        f"近{settings.foreign_days}日賣超天數": sell_days,
        f"連續{settings.foreign_days}日買超": buy_days.eq(settings.foreign_days),
        f"連續{settings.foreign_days}日賣超": sell_days.eq(settings.foreign_days),
    })


def prepare_model_frame(
    feature_detail: pd.DataFrame,
    foreign_feature: pd.DataFrame,
    settings: Settings,
) -> pd.DataFrame:
    base_columns = ["訊號日", "結果日", "range_ratio", TARGET]
    selected = feature_detail[
        feature_detail["指標代碼"].isin([RECENT_RANGE_FEATURE, NIGHT_RANGE_FEATURE])
    ].copy()
    base = (
        selected[selected["指標代碼"] == RECENT_RANGE_FEATURE][base_columns]
        .drop_duplicates(subset=["結果日"])
        .copy()
    )
    for feature in (RECENT_RANGE_FEATURE, NIGHT_RANGE_FEATURE):
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
    base = base.merge(foreign_feature, on="訊號日", how="left", validate="many_to_one")
    base[TARGET] = pd.to_numeric(base[TARGET], errors="coerce")
    base["主規則符合"] = (
        base[f"{RECENT_RANGE_FEATURE}_歷史分位_pct"].ge(80.0)
        & base[f"{NIGHT_RANGE_FEATURE}_歷史分位_pct"].ge(80.0)
    )
    start_ts = pd.Timestamp(settings.start)
    end_ts = pd.Timestamp(settings.end or date.today())
    base = base[base["結果日"].between(start_ts, end_ts)].copy()
    return base.sort_values("結果日").reset_index(drop=True)


def _wilson_interval(events: int, count: int, z: float = 1.959963984540054) -> tuple[float, float]:
    if count <= 0:
        return math.nan, math.nan
    rate = events / count
    denominator = 1.0 + z * z / count
    centre = (rate + z * z / (2.0 * count)) / denominator
    margin = z * math.sqrt(rate * (1.0 - rate) / count + z * z / (4.0 * count * count)) / denominator
    return max(0.0, centre - margin), min(1.0, centre + margin)


def _rule_masks(data: pd.DataFrame, rule: Rule, settings: Settings) -> tuple[pd.Series, pd.Series, pd.Series]:
    eligible = data["外資歷史分位_pct"].notna() & data[TARGET].notna()
    foreign_match = (
        data["外資歷史分位_pct"].le(settings.lower_percentile)
        if rule.foreign_side == "sell"
        else data["外資歷史分位_pct"].ge(settings.upper_percentile)
    )
    reference = eligible & (data["主規則符合"] if rule.require_base_rule else True)
    matched = reference & foreign_match
    return eligible, reference, matched


def _rate(events: int, count: int) -> float:
    return events / count if count else math.nan


def _period_stats(data: pd.DataFrame, rule: Rule, settings: Settings) -> dict[str, Any]:
    eligible, reference, matched = _rule_masks(data, rule, settings)
    universe = data[eligible]
    reference_group = data[reference]
    hit = data[matched]
    reference_miss = data[reference & ~matched]

    universe_count = len(universe)
    universe_events = int(universe[TARGET].sum()) if universe_count else 0
    reference_count = len(reference_group)
    reference_events = int(reference_group[TARGET].sum()) if reference_count else 0
    match_count = len(hit)
    match_events = int(hit[TARGET].sum()) if match_count else 0
    universe_rate = _rate(universe_events, universe_count)
    reference_rate = _rate(reference_events, reference_count)
    match_rate = _rate(match_events, match_count)
    low_ci, high_ci = _wilson_interval(match_events, match_count)

    odds_ratio = math.nan
    p_value = math.nan
    if match_count and len(reference_miss) and reference_group[TARGET].nunique() == 2:
        miss_events = int(reference_miss[TARGET].sum())
        table = [
            [match_events, match_count - match_events],
            [miss_events, len(reference_miss) - miss_events],
        ]
        odds_ratio, p_value = fisher_exact(table, alternative="two-sided")

    return {
        "母體數": universe_count,
        "母體事件數": universe_events,
        "母體事件率_pct": universe_rate * 100 if np.isfinite(universe_rate) else math.nan,
        "參考組": BASE_RULE_NAME if rule.require_base_rule else "全部可判定日",
        "參考組數": reference_count,
        "參考組事件數": reference_events,
        "參考組事件率_pct": reference_rate * 100 if np.isfinite(reference_rate) else math.nan,
        "符合數": match_count,
        "事件數": match_events,
        "事件率_pct": match_rate * 100 if np.isfinite(match_rate) else math.nan,
        "事件率95CI下限_pct": low_ci * 100 if np.isfinite(low_ci) else math.nan,
        "事件率95CI上限_pct": high_ci * 100 if np.isfinite(high_ci) else math.nan,
        "相對母體Lift": match_rate / universe_rate if np.isfinite(match_rate) and universe_rate > 0 else math.nan,
        "相對參考組Lift": match_rate / reference_rate if np.isfinite(match_rate) and reference_rate > 0 else math.nan,
        "相對參考組增減_百分點": (
            (match_rate - reference_rate) * 100
            if np.isfinite(match_rate) and np.isfinite(reference_rate) else math.nan
        ),
        "占母體_pct": match_count / universe_count * 100 if universe_count else math.nan,
        "占參考組_pct": match_count / reference_count * 100 if reference_count else math.nan,
        "符合對參考組其餘勝算比": odds_ratio,
        "增量Fisher_p值": p_value,
        "符合日平均振幅倍數": hit["range_ratio"].mean() if match_count else math.nan,
    }


def build_yearly(frame: pd.DataFrame, rules: list[Rule], settings: Settings) -> pd.DataFrame:
    work = frame.copy()
    work["年"] = work["結果日"].dt.year
    rows: list[dict[str, Any]] = []
    for rule in rules:
        for year, group in work.groupby("年", sort=True):
            stats = _period_stats(group, rule, settings)
            rows.append({
                "規則代碼": rule.rule_id,
                "規則名稱": rule.name,
                "外資方向": "大賣" if rule.foreign_side == "sell" else "大買",
                "加入主規則": rule.require_base_rule,
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
            "加入主規則": rule.require_base_rule,
        }
        for period, group in periods.items():
            stats = _period_stats(group, rule, settings)
            row.update({f"{period}_{key}": value for key, value in stats.items()})
        rows.append(row)
    summary = pd.DataFrame(rows)
    summary["全期_增量Fisher_q值"] = _bh_adjust(summary["全期_增量Fisher_p值"])

    annual_info: list[tuple[int, int, float]] = []
    for rule in rules:
        group = yearly[(yearly["規則代碼"] == rule.rule_id) & yearly["年度樣本足夠"]]
        comparable = group.dropna(subset=["相對參考組Lift"])
        positive = int(comparable["相對參考組Lift"].gt(1.0).sum())
        annual_info.append((
            len(comparable),
            positive,
            positive / len(comparable) if len(comparable) else math.nan,
        ))
    summary[["可比較年度數", "優於參考組年度數", "年度正向率"]] = pd.DataFrame(annual_info, index=summary.index)
    summary["探索驗證皆優於參考組"] = (
        summary["探索期_相對參考組Lift"].gt(1.0)
        & summary["驗證期_相對參考組Lift"].gt(1.0)
    )
    summary["外資增量候選"] = (
        summary["全期_增量Fisher_q值"].lt(settings.alpha)
        & summary["探索驗證皆優於參考組"]
        & summary["驗證期_增量Fisher_p值"].lt(0.10)
        & summary["驗證期_符合數"].ge(settings.minimum_rule_sample)
        & summary["年度正向率"].ge(0.60)
    )
    return summary.sort_values(
        ["外資增量候選", "驗證期_相對參考組Lift", "驗證期_符合數"],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True)


def build_rule_detail(frame: pd.DataFrame, rules: list[Rule], settings: Settings) -> pd.DataFrame:
    foreign_columns = [
        "外資單日買賣超_億",
        f"外資{settings.foreign_days}日累計買賣超_億",
        f"市場{settings.foreign_days}日累計成交金額_億",
        f"外資{settings.foreign_days}日累計買賣超率_pct",
        "外資歷史分位_pct",
        "外資強度層",
        f"近{settings.foreign_days}日買超天數",
        f"近{settings.foreign_days}日賣超天數",
        f"連續{settings.foreign_days}日買超",
        f"連續{settings.foreign_days}日賣超",
    ]
    base_columns = ["訊號日", "結果日", "range_ratio", TARGET, "主規則符合", *foreign_columns]
    parts: list[pd.DataFrame] = []
    for rule in rules:
        eligible, reference, matched = _rule_masks(frame, rule, settings)
        part = frame[base_columns].copy()
        part["規則代碼"] = rule.rule_id
        part["規則名稱"] = rule.name
        part["可判定"] = eligible
        part["屬於參考組"] = reference
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
        output_dir / f"violent_amplitude_foreign_flow_summary_{suffix}.csv",
        output_dir / f"violent_amplitude_foreign_flow_yearly_{suffix}.csv",
        output_dir / f"violent_amplitude_foreign_flow_detail_{suffix}.csv",
        output_dir / f"violent_amplitude_foreign_flow_manifest_{suffix}.json",
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
        "source_view": SOURCE_VIEW,
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
        "base_rule": BASE_RULE_NAME,
        "rules": [asdict(rule) for rule in rules],
        "candidate_rule": (
            "全期增量Fisher檢定BH-FDR q<0.05、探索與驗證相對參考組Lift皆>1、"
            "驗證期增量Fisher p<0.10、驗證期符合數達門檻、年度正向率>=60%"
        ),
        "warnings": [
            "本程式只研究隔日日盤是否劇烈以上，不區分多空與交易損益",
            "F03/F04的參考組是近期振幅與夜盤振幅皆Q5，增量檢定不是與全部交易日比較",
            "外資買賣可能同時受到行情影響，本結果代表關聯而非因果",
            "明細保留連續買賣天數，主規則仍以3日累計買賣超率的歷史分位判定",
        ],
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="外資3日累計買賣超與隔日日盤劇烈振幅統計")
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
        default=Path("../output/analysis/futures_daily_amplitude_foreign_flow_scan"),
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
    feature_settings = FeatureSettings(
        start=settings.start,
        end=settings.end,
        futures_id=settings.futures_id,
        split_date=settings.split_date,
        history_window=settings.history_window,
        minimum_history=settings.minimum_history,
    )
    source = load_source_with_foreign(settings)
    targets = load_targets(feature_settings)
    feature_detail, _ = prepare_detail(source, targets, feature_settings)
    foreign_feature = build_foreign_feature(source, settings)
    frame = prepare_model_frame(feature_detail, foreign_feature, settings)
    rules = build_rules(settings)
    yearly = build_yearly(frame, rules, settings)
    summary = build_summary(frame, rules, yearly, settings)
    rule_detail = build_rule_detail(frame, rules, settings)
    paths = export_results(frame, summary, yearly, rule_detail, rules, settings, args.output_dir)

    baseline = frame[TARGET].mean() * 100
    base_days = frame[frame["主規則符合"] & frame[TARGET].notna()]
    base_rate = base_days[TARGET].mean() * 100 if len(base_days) else math.nan
    print(f"有效研究日：{len(frame):,} 日")
    print(f"劇烈以上母體率：{baseline:.2f}%")
    print(f"主規則：{len(base_days):,} 日，劇烈率 {base_rate:.2f}%")
    print(f"外資規則：{len(rules)} 組")
    print(f"外資增量候選：{int(summary['外資增量候選'].sum())} 組")
    display = [
        "規則代碼", "規則名稱", "驗證期_參考組數", "驗證期_參考組事件率_pct",
        "驗證期_符合數", "驗證期_事件率_pct", "驗證期_相對參考組Lift",
        "驗證期_增量Fisher_p值", "外資增量候選",
    ]
    print("\n驗證期結果：")
    print(summary[display].to_string(index=False))
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
