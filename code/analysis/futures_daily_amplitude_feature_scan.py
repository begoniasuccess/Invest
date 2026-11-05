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
from scipy.stats import chi2_contingency, fisher_exact, spearmanr

from backtest.futures_raw_signal_strength_scan import (
    BUCKET_LABELS,
    SOURCE_COLUMNS,
    _numeric,
    _rolling_percentile,
    _safe_ratio,
    _strength_bucket,
    build_signal_features,
)
from backtest.futures_strategy_next_day_field_scan import _bh_adjust


ANALYSIS_NAME = "futures_daily_amplitude_feature_scan"
METRIC_VERSION = "1.0.0"
SOURCE_VIEW = "report.v_futures_report_daily_corrected_turning"
TARGET_VIEW = "research.v_futures_daily_amplitude_pattern"
EXTRA_SOURCE_COLUMNS = (
    "日成交量", "夜成交量", "日盤走勢", "夜盤走勢",
    "策略_核心_昨價量", "策略_核心_五日價量",
)
TARGETS = {
    "劇烈以上": "振幅標籤為劇烈或極端",
    "劇烈強單邊": "劇烈以上，且型態為強單邊多或強單邊空",
    "劇烈強單邊多": "劇烈以上且型態為強單邊多",
    "劇烈強單邊空": "劇烈以上且型態為強單邊空",
    "劇烈非單邊": "劇烈以上，但不是強單邊多空",
}


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    split_date: date = date(2023, 1, 1)
    history_window: int = 252
    minimum_history: int = 60
    minimum_group_size: int = 20
    minimum_year_size: int = 80
    alpha: float = 0.05

    def validate(self) -> None:
        if self.end and self.end < self.start:
            raise ValueError("end 不可早於 start")
        if self.split_date <= self.start:
            raise ValueError("split_date 必須晚於 start")
        if self.history_window < 60:
            raise ValueError("history_window 至少為 60")
        if not 20 <= self.minimum_history <= self.history_window:
            raise ValueError("minimum_history 必須介於20與history_window之間")
        if self.minimum_group_size < 10 or self.minimum_year_size < 40:
            raise ValueError("樣本門檻過低")


def load_source(settings: Settings) -> pd.DataFrame:
    from common import db

    settings.validate()
    query_start = settings.start - timedelta(days=max(400, settings.history_window * 2))
    columns = tuple(dict.fromkeys((*SOURCE_COLUMNS, *EXTRA_SOURCE_COLUMNS)))
    selected = ", ".join(f'"{column}"' for column in columns)
    sql = f'''SELECT futures_id, {selected}
              FROM {SOURCE_VIEW}
              WHERE futures_id = ? AND "日期" >= ?'''
    params: list[Any] = [settings.futures_id, query_start]
    if settings.end:
        sql += ' AND "日期" <= ?'
        params.append(settings.end)
    sql += ' ORDER BY "日期"'
    return db.sql_to_df(sql, params=tuple(params), parse_dates=["日期"])


def load_targets(settings: Settings) -> pd.DataFrame:
    from common import db

    sql = f'''
        SELECT trading_date, range_ratio, amplitude_label, path_pattern_label,
               excursion_bias_label, session_trend_label
        FROM {TARGET_VIEW}
        WHERE futures_id = ?
          AND valid_for_research
          AND trading_date BETWEEN ? AND ?
        ORDER BY trading_date
    '''
    return db.sql_to_df(
        sql,
        params=(settings.futures_id, settings.start, settings.end or date.today()),
        parse_dates=["trading_date"],
    )


def _pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return _safe_ratio(numerator, denominator) * 100.0


def _rolling_previous_mean(values: pd.Series, window: int) -> pd.Series:
    return pd.to_numeric(values, errors="coerce").shift(1).rolling(window, min_periods=window).mean()


def _direction_code(values: pd.Series) -> pd.Series:
    text = values.astype("string").str.strip()
    result = pd.Series(np.nan, index=values.index, dtype=float)
    result[text.str.contains("多|漲", na=False)] = 1.0
    result[text.str.contains("空|跌", na=False)] = -1.0
    return result


def build_extended_features(source: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
    ordered = source.sort_values("日期").reset_index(drop=True).copy()
    features, metadata = build_signal_features(ordered)
    day_open = _numeric(ordered, "日盤開")
    day_high = _numeric(ordered, "日盤高")
    day_low = _numeric(ordered, "日盤低")
    day_close = _numeric(ordered, "日盤收")
    night_open = _numeric(ordered, "夜盤開")
    night_high = _numeric(ordered, "夜盤高")
    night_low = _numeric(ordered, "夜盤低")
    night_close = _numeric(ordered, "夜盤收")
    day_range = day_high - day_low
    night_range = night_high - night_low
    day_volume = _numeric(ordered, "日成交量")
    night_volume = _numeric(ordered, "夜成交量")
    day_return = _pct(day_close - day_open, day_open)
    night_return = _pct(night_close - night_open, night_open)

    additions: dict[str, tuple[pd.Series, str, str, bool]] = {
        "日盤開收": (day_return, "日夜盤", "(日收-日開)/日開*100", True),
        "日盤振幅": (_pct(day_range, day_open), "日夜盤", "(日高-日低)/日開*100", False),
        "日盤收盤位置": (_safe_ratio(day_close - day_low, day_range) * 100, "日夜盤", "(日收-日低)/(日高-日低)*100", True),
        "夜盤相對日收": (_pct(night_close - day_close, day_close), "日夜盤", "(夜收-日收)/日收*100", True),
        "夜盤開收": (night_return, "日夜盤", "(夜收-夜開)/夜開*100", True),
        "夜盤振幅": (_pct(night_range, night_open), "日夜盤", "(夜高-夜低)/夜開*100", False),
        "夜盤收盤位置": (_safe_ratio(night_close - night_low, night_range) * 100, "日夜盤", "(夜收-夜低)/(夜高-夜低)*100", True),
        "夜日振幅比": (_safe_ratio(night_range, day_range), "振幅狀態", "夜盤振幅/日盤振幅", False),
        "日盤振幅_前30日比": (_safe_ratio(day_range, _rolling_previous_mean(day_range, 30)), "振幅狀態", "日盤振幅/此前30日平均", False),
        "夜盤振幅_前30日比": (_safe_ratio(night_range, _rolling_previous_mean(night_range, 30)), "振幅狀態", "夜盤振幅/此前30日平均", False),
        "近5日日盤振幅_前30日比": (_safe_ratio(day_range.rolling(5, min_periods=5).mean(), _rolling_previous_mean(day_range, 30)), "振幅狀態", "近5日日盤平均振幅/此前30日平均", False),
        "日成交量_前20日比": (_safe_ratio(day_volume, _rolling_previous_mean(day_volume, 20)), "量能", "日成交量/此前20日平均", False),
        "夜成交量_前20日比": (_safe_ratio(night_volume, _rolling_previous_mean(night_volume, 20)), "量能", "夜成交量/此前20日平均", False),
        "日夜方向一致": ((np.sign(day_return) == np.sign(night_return)).where(day_return.notna() & night_return.notna()).astype(float), "日夜盤", "日盤與夜盤開收同方向=1", False),
        "日盤走勢碼": (_direction_code(ordered["日盤走勢"]), "型態", "漲勢盤=1、跌勢盤=-1", True),
        "夜盤走勢碼": (_direction_code(ordered["夜盤走勢"]), "型態", "漲勢盤=1、跌勢盤=-1", True),
        "核心昨價量方向碼": (_direction_code(ordered["策略_核心_昨價量"]), "既有訊號", "多=1、空=-1", True),
        "核心五日價量方向碼": (_direction_code(ordered["策略_核心_五日價量"]), "既有訊號", "多=1、空=-1", True),
    }
    for name, (values, category, formula, natural_direction) in additions.items():
        features[name] = pd.to_numeric(values, errors="coerce")
        metadata[name] = {
            "類別": category,
            "公式": formula,
            "天然方向": natural_direction,
        }
    return features, metadata


def add_target_flags(targets: pd.DataFrame) -> pd.DataFrame:
    result = targets.copy()
    amplitude = result["amplitude_label"].isin(["劇烈振幅", "極端振幅"])
    strong_long = result["path_pattern_label"].eq("強單邊多")
    strong_short = result["path_pattern_label"].eq("強單邊空")
    result["劇烈以上"] = amplitude.astype(int)
    result["劇烈強單邊"] = (amplitude & (strong_long | strong_short)).astype(int)
    result["劇烈強單邊多"] = (amplitude & strong_long).astype(int)
    result["劇烈強單邊空"] = (amplitude & strong_short).astype(int)
    result["劇烈非單邊"] = (amplitude & ~(strong_long | strong_short)).astype(int)
    return result


def prepare_detail(
    source: pd.DataFrame,
    targets: pd.DataFrame,
    settings: Settings,
) -> tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
    ordered = source.sort_values("日期").reset_index(drop=True).copy()
    features, metadata = build_extended_features(ordered)
    target_flags = add_target_flags(targets)
    target_flags["trading_date"] = pd.to_datetime(target_flags["trading_date"])
    target_lookup = target_flags.set_index("trading_date")
    signal_date = pd.to_datetime(ordered["日期"])
    result_date = signal_date.shift(-1)
    common = pd.DataFrame({"訊號日": signal_date, "結果日": result_date})
    common = common.join(target_lookup, on="結果日")
    parts: list[pd.DataFrame] = []
    for name, meta in metadata.items():
        raw = pd.to_numeric(features[name], errors="coerce")
        percentile = _rolling_percentile(raw, settings.history_window, settings.minimum_history)
        part = common.copy()
        part["指標類別"] = meta["類別"]
        part["指標代碼"] = name
        part["公式"] = meta["公式"]
        part["原始值"] = raw
        part["歷史分位_pct"] = percentile
        part["強度層"] = _strength_bucket(percentile)
        parts.append(part)
    detail = pd.concat(parts, ignore_index=True)
    valid = detail["結果日"].notna() & detail["range_ratio"].notna()
    detail = detail[valid].copy()
    return detail.reset_index(drop=True), metadata


def build_target_distribution(targets: pd.DataFrame) -> pd.DataFrame:
    work = add_target_flags(targets)
    work["年"] = pd.to_datetime(work["trading_date"]).dt.year
    rows: list[dict[str, Any]] = []
    periods: list[tuple[str, pd.DataFrame]] = [("全期", work)]
    periods.extend((str(year), group) for year, group in work.groupby("年", sort=True))
    for period, group in periods:
        total = len(group)
        for target in TARGETS:
            count = int(group[target].sum())
            rows.append({
                "期間": period, "分組": "研究目標", "標籤": target,
                "樣本數": count, "母體數": total,
                "占比_pct": count / total * 100 if total else math.nan,
            })
        for group_name, column in (("振幅標籤", "amplitude_label"), ("路徑型態", "path_pattern_label")):
            for label, count in group[column].value_counts(dropna=False).items():
                rows.append({
                    "期間": period, "分組": group_name, "標籤": label,
                    "樣本數": int(count), "母體數": total,
                    "占比_pct": count / total * 100 if total else math.nan,
                })
    return pd.DataFrame(rows)


def build_bucket_summary(detail: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (category, signal), signal_data in detail.groupby(["指標類別", "指標代碼"], sort=False):
        for target in TARGETS:
            usable = signal_data.dropna(subset=["強度層", target])
            baseline = usable[target].mean()
            for bucket in BUCKET_LABELS:
                group = usable[usable["強度層"] == bucket]
                count = len(group)
                events = int(group[target].sum()) if count else 0
                rate = events / count if count else math.nan
                rows.append({
                    "指標類別": category, "指標代碼": signal, "研究目標": target,
                    "強度層": bucket, "樣本數": count, "事件數": events,
                    "事件率_pct": rate * 100 if count else math.nan,
                    "母體事件率_pct": baseline * 100 if np.isfinite(baseline) else math.nan,
                    "Lift": rate / baseline if count and baseline > 0 else math.nan,
                    "相對母體增減_百分點": (rate - baseline) * 100 if count else math.nan,
                    "平均振幅倍數": group["range_ratio"].mean() if count else math.nan,
                })
    return pd.DataFrame(rows)


def _period_stats(data: pd.DataFrame, target: str, minimum_group_size: int) -> dict[str, Any]:
    usable = data[["歷史分位_pct", "強度層", target]].dropna()
    result: dict[str, Any] = {
        "樣本數": len(usable), "事件數": int(usable[target].sum()),
        "母體事件率_pct": usable[target].mean() * 100 if len(usable) else math.nan,
    }
    if len(usable) < minimum_group_size * 2 or usable[target].nunique() < 2:
        return result
    rho, rho_p = spearmanr(usable["歷史分位_pct"], usable[target])
    result.update({"趨勢效果": rho, "趨勢p值": rho_p})
    low = usable[usable["強度層"] == BUCKET_LABELS[0]][target]
    high = usable[usable["強度層"] == BUCKET_LABELS[-1]][target]
    if min(len(low), len(high)) >= minimum_group_size:
        table = [[int(high.sum()), int(len(high) - high.sum())], [int(low.sum()), int(len(low) - low.sum())]]
        odds_ratio, fisher_p = fisher_exact(table, alternative="two-sided")
        q1_rate = low.mean()
        q5_rate = high.mean()
        result.update({
            "Q1樣本數": len(low), "Q5樣本數": len(high),
            "Q1事件率_pct": q1_rate * 100, "Q5事件率_pct": q5_rate * 100,
            "Q5減Q1_百分點": (q5_rate - q1_rate) * 100,
            "Q5對Q1勝算比": odds_ratio, "Q1Q5_p值": fisher_p,
        })
    table5 = pd.crosstab(usable["強度層"], usable[target]).reindex(BUCKET_LABELS, fill_value=0)
    if table5.shape[1] == 2 and (table5.sum(axis=1) >= minimum_group_size).all():
        chi2, p_value, _, _ = chi2_contingency(table5)
        cramers_v = math.sqrt(chi2 / len(usable))
        result.update({"五層差異p值": p_value, "五層效果量": cramers_v})
    return result


def build_ranking(detail: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    dates = pd.to_datetime(detail["結果日"]).dt.date
    discovery_mask = dates < settings.split_date
    for (category, signal), signal_data in detail.groupby(["指標類別", "指標代碼"], sort=False):
        signal_dates = pd.to_datetime(signal_data["結果日"]).dt.date
        parts = {
            "全期": signal_data,
            "探索期": signal_data[signal_dates < settings.split_date],
            "驗證期": signal_data[signal_dates >= settings.split_date],
        }
        for target in TARGETS:
            row: dict[str, Any] = {"指標類別": category, "指標代碼": signal, "研究目標": target}
            for period, group in parts.items():
                stats = _period_stats(group, target, settings.minimum_group_size)
                row.update({f"{period}_{key}": value for key, value in stats.items()})
            rows.append(row)
    ranking = pd.DataFrame(rows)
    if ranking.empty:
        return ranking
    for p_column, q_column in (
        ("全期_趨勢p值", "全期_趨勢q值"),
        ("全期_Q1Q5_p值", "全期_Q1Q5_q值"),
        ("全期_五層差異p值", "全期_五層差異q值"),
        ("探索期_趨勢p值", "探索期_趨勢q值"),
        ("探索期_Q1Q5_p值", "探索期_Q1Q5_q值"),
    ):
        ranking[q_column] = _bh_adjust(ranking[p_column]) if p_column in ranking else math.nan
    ranking["前後期趨勢同方向"] = (
        np.sign(ranking["探索期_趨勢效果"]) == np.sign(ranking["驗證期_趨勢效果"])
    )
    ranking["前後期極端差同方向"] = (
        np.sign(ranking["探索期_Q5減Q1_百分點"]) == np.sign(ranking["驗證期_Q5減Q1_百分點"])
    )
    ranking["全期任一FDR顯著"] = (
        ranking[["全期_趨勢q值", "全期_Q1Q5_q值", "全期_五層差異q值"]]
        .lt(settings.alpha).any(axis=1)
    )
    return ranking


def build_yearly(detail: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    work = detail.copy()
    work["年"] = pd.to_datetime(work["結果日"]).dt.year
    rows: list[dict[str, Any]] = []
    for (category, signal, year), group in work.groupby(["指標類別", "指標代碼", "年"], sort=True):
        for target in TARGETS:
            stats = _period_stats(group, target, max(5, settings.minimum_group_size // 4))
            if stats.get("樣本數", 0) >= settings.minimum_year_size:
                rows.append({
                    "指標類別": category, "指標代碼": signal, "年": int(year),
                    "研究目標": target, **stats,
                })
    return pd.DataFrame(rows)


def add_yearly_consistency(ranking: pd.DataFrame, yearly: pd.DataFrame) -> pd.DataFrame:
    if ranking.empty:
        return ranking
    result = ranking.copy()
    info: list[tuple[int, int, float]] = []
    for row in result.itertuples(index=False):
        expected = np.sign(getattr(row, "全期_趨勢效果", math.nan))
        group = yearly[(yearly["指標代碼"] == row.指標代碼) & (yearly["研究目標"] == row.研究目標)]
        signs = np.sign(pd.to_numeric(group.get("趨勢效果"), errors="coerce").dropna())
        signs = signs[signs != 0]
        matches = int((signs == expected).sum()) if np.isfinite(expected) and expected != 0 else 0
        info.append((len(signs), matches, matches / len(signs) if len(signs) else math.nan))
    result[["可比較年度數", "同方向年度數", "年度趨勢一致率"]] = pd.DataFrame(info, index=result.index)
    trend_pass = (
        result["前後期趨勢同方向"]
        & result["驗證期_趨勢p值"].lt(0.10)
    )
    extreme_pass = (
        result["前後期極端差同方向"]
        & result["驗證期_Q1Q5_p值"].lt(0.10)
    )
    result["第一階段候選"] = (
        result["全期任一FDR顯著"]
        & (trend_pass | extreme_pass)
        & result["年度趨勢一致率"].ge(0.60)
    )
    return result.sort_values(
        ["第一階段候選", "全期任一FDR顯著", "全期_五層差異q值", "全期_趨勢q值"],
        ascending=[False, False, True, True], na_position="last",
    ).reset_index(drop=True)


def export_results(
    detail: pd.DataFrame,
    distribution: pd.DataFrame,
    buckets: pd.DataFrame,
    ranking: pd.DataFrame,
    yearly: pd.DataFrame,
    metadata: dict[str, dict[str, Any]],
    settings: Settings,
    output_dir: Path,
) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{settings.start:%Y%m%d}_{settings.end:%Y%m%d}" if settings.end else f"{settings.start:%Y%m%d}_latest"
    paths = (
        output_dir / f"amplitude_feature_distribution_{suffix}.csv",
        output_dir / f"amplitude_feature_buckets_{suffix}.csv",
        output_dir / f"amplitude_feature_ranking_{suffix}.csv",
        output_dir / f"amplitude_feature_yearly_{suffix}.csv",
        output_dir / f"amplitude_feature_detail_{suffix}.csv",
        output_dir / f"amplitude_feature_manifest_{suffix}.json",
    )
    for frame, path in zip((distribution, buckets, ranking, yearly, detail), paths[:5]):
        frame.to_csv(path, index=False, encoding="utf-8-sig")
    paths[5].write_text(json.dumps({
        "analysis_name": ANALYSIS_NAME,
        "metric_version": METRIC_VERSION,
        "run_id": str(uuid.uuid4()),
        "settings": {**asdict(settings), "start": settings.start.isoformat(),
                     "end": settings.end.isoformat() if settings.end else None,
                     "split_date": settings.split_date.isoformat()},
        "source_view": SOURCE_VIEW,
        "target_view": TARGET_VIEW,
        "targets": TARGETS,
        "features": metadata,
        "rules": {
            "feature_bucket": "各指標只與此前最多252筆歷史比較後切五分位，不使用未來資料",
            "discovery": f"結果日早於{settings.split_date.isoformat()}",
            "validation": f"結果日起於{settings.split_date.isoformat()}",
            "multiple_testing": "同一輸出的全部特徵與目標使用BH-FDR校正",
            "candidate": "全期任一檢定FDR<0.05、探索驗證同方向、驗證p<0.10、年度方向一致率>=60%",
        },
        "warnings": [
            "第一階段只掃描單因子，不代表因果或可直接交易",
            "理想單邊多空事件數較少，必須特別重視年度穩定性",
            "第二階段才測最多兩項特徵的組合，避免樣本快速縮小",
        ],
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="全部交易日劇烈振幅與理想路徑的第一階段單因子分析")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--split-date", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--history-window", type=int, default=252)
    parser.add_argument("--minimum-history", type=int, default=60)
    parser.add_argument("--minimum-group-size", type=int, default=20)
    parser.add_argument("--minimum-year-size", type=int, default=80)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path("../output/analysis/futures_daily_amplitude_feature_scan"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(
        start=args.start, end=args.end, futures_id=args.futures_id,
        split_date=args.split_date, history_window=args.history_window,
        minimum_history=args.minimum_history, minimum_group_size=args.minimum_group_size,
        minimum_year_size=args.minimum_year_size, alpha=args.alpha,
    )
    settings.validate()
    source = load_source(settings)
    targets = load_targets(settings)
    detail, metadata = prepare_detail(source, targets, settings)
    distribution = build_target_distribution(targets)
    buckets = build_bucket_summary(detail)
    ranking = build_ranking(detail, settings)
    yearly = build_yearly(detail, settings)
    ranking = add_yearly_consistency(ranking, yearly)
    paths = export_results(detail, distribution, buckets, ranking, yearly, metadata, settings, args.output_dir)
    print(f"有效目標交易日：{len(targets):,} 日")
    print(f"單因子：{len(metadata)} 項")
    print(f"特徵明細：{len(detail):,} 筆")
    print(f"全期任一FDR顯著：{int(ranking['全期任一FDR顯著'].sum()) if len(ranking) else 0} 組")
    print(f"第一階段候選：{int(ranking['第一階段候選'].sum()) if len(ranking) else 0} 組")
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
