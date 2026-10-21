from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency, mannwhitneyu, ttest_ind


SOURCE_VIEW = "report.v_futures_report_daily_corrected_turning"
GROUPS = ("多", "空")
EXCLUDED_COLUMNS = {
    "id", "futures_id", "日期", "年", "月", "星期", "週次", "天別",
    "建立時間", "夜盤資料來源",
}
RAW_MEASURE_HINTS = (
    "pct", "漲跌", "振幅", "實體", "上影", "下影", "乖離率", "多空比",
    "買賣權比", "收減", "收開", "跳空缺口", "均上升幅度", "位置",
)


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    minimum_group_size: int = 100
    minimum_year_size: int = 20
    split_date: date = date(2023, 1, 1)
    alpha: float = 0.05
    winsor_tail: float = 0.005

    def __post_init__(self) -> None:
        if self.end and self.start > self.end:
            raise ValueError("start 不可晚於 end")
        if not (self.start < self.split_date):
            raise ValueError("split_date 必須晚於 start")
        if self.minimum_group_size < 10 or self.minimum_year_size < 5:
            raise ValueError("樣本門檻過低")
        if not 0 < self.alpha < 1:
            raise ValueError("alpha 必須介於 0 與 1 之間")
        if not 0 <= self.winsor_tail < 0.1:
            raise ValueError("winsor_tail 必須介於 0 與 0.1 之間")


def load_source(settings: Settings) -> pd.DataFrame:
    from common import db

    # 不在 SQL 先切掉 end，保留 end 之後的第一個實際交易日供 shift(-1) 對齊；
    # 建立隔日目標後才以訊號日篩選。
    params = (settings.futures_id, settings.start)
    sql = f'''
        SELECT *
        FROM {SOURCE_VIEW}
        WHERE futures_id = ?
          AND "日期" >= ?
        ORDER BY "日期"
    '''
    return db.sql_to_df(sql, params=params, parse_dates=["日期"])


def detect_strategy_columns(frame: pd.DataFrame) -> list[str]:
    result = []
    for column in frame.columns:
        if not str(column).startswith("策略_"):
            continue
        values = set(frame[column].dropna().astype(str).str.strip().unique())
        if set(GROUPS).issubset(values):
            result.append(column)
    return result


def _numeric_version(series: pd.Series) -> pd.Series | None:
    non_null = series.notna().sum()
    if non_null == 0:
        return None
    converted = pd.to_numeric(series, errors="coerce")
    if converted.notna().sum() / non_null < 0.95 or converted.nunique(dropna=True) < 5:
        return None
    return converted.astype(float)


def build_targets(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, dict[str, str]]]:
    """建立下一交易日目標；絕對量主看變化，尺度化欄位另看隔日原值。"""
    work = frame.sort_values("日期").reset_index(drop=True).copy()
    targets: dict[str, pd.Series] = {}
    metadata: dict[str, dict[str, str]] = {}
    strategy_columns = set(detect_strategy_columns(work))

    for column in work.columns:
        if column in EXCLUDED_COLUMNS or column in strategy_columns:
            continue
        current = work[column]
        future = current.shift(-1)
        numeric = _numeric_version(current)
        if numeric is not None:
            future_numeric = numeric.shift(-1)
            change_name = f"{column}__隔日變化"
            targets[change_name] = future_numeric - numeric
            metadata[change_name] = {
                "來源欄位": str(column), "目標型態": "隔日變化",
                "公式": "下一交易日值－訊號日值", "資料型態": "numeric",
            }
            positive_ratio = float((numeric.dropna() > 0).mean())
            if positive_ratio >= 0.95:
                pct_name = f"{column}__隔日變化率_pct"
                denominator = numeric.replace(0, np.nan)
                targets[pct_name] = (future_numeric - numeric) / denominator * 100.0
                metadata[pct_name] = {
                    "來源欄位": str(column), "目標型態": "隔日變化率",
                    "公式": "(下一交易日值－訊號日值)/訊號日值*100",
                    "資料型態": "numeric",
                }
            if any(hint in str(column) for hint in RAW_MEASURE_HINTS):
                raw_name = f"{column}__隔日原值"
                targets[raw_name] = future_numeric
                metadata[raw_name] = {
                    "來源欄位": str(column), "目標型態": "隔日原值",
                    "公式": "下一交易日值", "資料型態": "numeric",
                }
        else:
            values = current.dropna().astype(str).str.strip()
            if 2 <= values.nunique() <= 30:
                name = f"{column}__隔日類別"
                targets[name] = future.astype("string").str.strip()
                metadata[name] = {
                    "來源欄位": str(column), "目標型態": "隔日類別分布",
                    "公式": "下一交易日類別", "資料型態": "categorical",
                }

    target_frame = pd.DataFrame(targets, index=work.index)
    context = work[["日期", *detect_strategy_columns(work)]].copy()
    context["隔日日期"] = work["日期"].shift(-1)
    return pd.concat([context, target_frame], axis=1), metadata


def _winsorize_pair(a: pd.Series, b: pd.Series, tail: float) -> tuple[pd.Series, pd.Series]:
    combined = pd.concat([a, b]).dropna()
    if combined.empty or tail <= 0:
        return a, b
    low, high = combined.quantile([tail, 1 - tail])
    return a.clip(low, high), b.clip(low, high)


def _hedges_g(a: pd.Series, b: pd.Series) -> float:
    n1, n2 = len(a), len(b)
    if min(n1, n2) < 2:
        return math.nan
    pooled_num = (n1 - 1) * a.var(ddof=1) + (n2 - 1) * b.var(ddof=1)
    pooled_den = n1 + n2 - 2
    if pooled_den <= 0 or pooled_num <= 0:
        return 0.0
    d = (a.mean() - b.mean()) / math.sqrt(pooled_num / pooled_den)
    correction = 1 - 3 / (4 * (n1 + n2) - 9)
    return float(d * correction)


def _cramers_v(table: pd.DataFrame, chi2: float) -> float:
    n = table.to_numpy().sum()
    denominator = min(table.shape[0] - 1, table.shape[1] - 1)
    return math.sqrt((chi2 / n) / denominator) if n and denominator > 0 else math.nan


def _numeric_test(frame: pd.DataFrame, strategy: str, target: str,
                  settings: Settings) -> dict[str, object] | None:
    data = frame[[strategy, target]].dropna().copy()
    data[strategy] = data[strategy].astype(str).str.strip()
    a = pd.to_numeric(data.loc[data[strategy] == "多", target], errors="coerce").dropna()
    b = pd.to_numeric(data.loc[data[strategy] == "空", target], errors="coerce").dropna()
    if min(len(a), len(b)) < settings.minimum_group_size:
        return None
    a, b = _winsorize_pair(a, b, settings.winsor_tail)
    _, welch_p = ttest_ind(a, b, equal_var=False, nan_policy="omit")
    u_stat, primary_p = mannwhitneyu(a, b, alternative="two-sided")
    return {
        "多樣本數": len(a), "空樣本數": len(b),
        "多平均": a.mean(), "空平均": b.mean(), "多減空平均差": a.mean() - b.mean(),
        "多中位數": a.median(), "空中位數": b.median(),
        "效果方向": "多較高" if a.mean() > b.mean() else "空較高",
        "Hedges_g": _hedges_g(a, b), "主要檢定": "Mann-Whitney U",
        "檢定統計值": u_stat, "p值": primary_p, "Welch_p值": welch_p,
    }


def _categorical_test(frame: pd.DataFrame, strategy: str, target: str,
                      settings: Settings) -> dict[str, object] | None:
    data = frame[[strategy, target]].dropna().copy()
    data[strategy] = data[strategy].astype(str).str.strip()
    data[target] = data[target].astype(str).str.strip()
    data = data[data[strategy].isin(GROUPS)]
    table = pd.crosstab(data[strategy], data[target]).reindex(GROUPS, fill_value=0)
    if table.shape[1] < 2 or min(table.sum(axis=1)) < settings.minimum_group_size:
        return None
    # 移除總數為 0 的類別，並合併極稀疏類別以避免卡方近似失真。
    common = table.columns[table.sum(axis=0) >= 10]
    table = table[common]
    if table.shape[1] < 2:
        return None
    chi2, p_value, _, _ = chi2_contingency(table)
    long_top = table.loc["多"].idxmax()
    short_top = table.loc["空"].idxmax()
    return {
        "多樣本數": int(table.loc["多"].sum()), "空樣本數": int(table.loc["空"].sum()),
        "多平均": math.nan, "空平均": math.nan, "多減空平均差": math.nan,
        "多中位數": long_top, "空中位數": short_top,
        "效果方向": f"多常見={long_top}；空常見={short_top}",
        "Hedges_g": math.nan, "主要檢定": "Chi-square",
        "檢定統計值": chi2, "p值": p_value, "Welch_p值": math.nan,
        "Cramers_V": _cramers_v(table, chi2),
    }


def _bh_adjust(series: pd.Series) -> pd.Series:
    result = pd.Series(np.nan, index=series.index, dtype=float)
    clean = pd.to_numeric(series, errors="coerce").dropna().sort_values()
    if clean.empty:
        return result
    adjusted = (clean * len(clean) / np.arange(1, len(clean) + 1))[::-1].cummin()[::-1]
    result.loc[clean.index] = adjusted.clip(upper=1.0)
    return result


def scan_period(frame: pd.DataFrame, metadata: dict[str, dict[str, str]],
                settings: Settings) -> pd.DataFrame:
    strategies = detect_strategy_columns(frame)
    rows: list[dict[str, object]] = []
    for strategy in strategies:
        for target, meta in metadata.items():
            test = (_numeric_test(frame, strategy, target, settings)
                    if meta["資料型態"] == "numeric"
                    else _categorical_test(frame, strategy, target, settings))
            if test is not None:
                rows.append({"策略欄位": strategy, "目標欄位": target, **meta, **test})
    result = pd.DataFrame(rows)
    if not result.empty:
        result["FDR_q值"] = _bh_adjust(result["p值"])
        result["FDR顯著"] = result["FDR_q值"] < settings.alpha
    return result


def _year_stability(frame: pd.DataFrame, strategy: str, target: str,
                    settings: Settings) -> tuple[int, int, float]:
    years = pd.to_datetime(frame["隔日日期"]).dt.year
    signs: list[int] = []
    for _, part in frame.assign(_year=years).groupby("_year"):
        data = part[[strategy, target]].dropna()
        a = pd.to_numeric(data.loc[data[strategy].astype(str).str.strip() == "多", target],
                          errors="coerce").dropna()
        b = pd.to_numeric(data.loc[data[strategy].astype(str).str.strip() == "空", target],
                          errors="coerce").dropna()
        if min(len(a), len(b)) >= settings.minimum_year_size:
            signs.append(1 if a.mean() > b.mean() else -1)
    if not signs:
        return 0, 0, math.nan
    majority = max(signs.count(1), signs.count(-1))
    return len(signs), majority, majority / len(signs)


def add_validation(full: pd.DataFrame, prepared: pd.DataFrame,
                   metadata: dict[str, dict[str, str]], settings: Settings) -> pd.DataFrame:
    if full.empty:
        return full
    discovery_frame = prepared[pd.to_datetime(prepared["日期"]).dt.date < settings.split_date]
    confirm_frame = prepared[pd.to_datetime(prepared["日期"]).dt.date >= settings.split_date]
    discovery = scan_period(discovery_frame, metadata, settings)
    confirmation = scan_period(confirm_frame, metadata, settings)
    key = ["策略欄位", "目標欄位"]
    discovery = discovery[key + ["p值", "FDR_q值", "多減空平均差", "Hedges_g"]].rename(columns={
        "p值": "探索期_p值", "FDR_q值": "探索期_q值",
        "多減空平均差": "探索期_多減空差", "Hedges_g": "探索期_Hedges_g",
    })
    confirmation = confirmation[key + ["p值", "FDR_q值", "多減空平均差", "Hedges_g"]].rename(columns={
        "p值": "驗證期_p值", "FDR_q值": "驗證期_q值",
        "多減空平均差": "驗證期_多減空差", "Hedges_g": "驗證期_Hedges_g",
    })
    output = full.merge(discovery, on=key, how="left").merge(confirmation, on=key, how="left")
    numeric_mask = output["資料型態"] == "numeric"
    same_sign = (np.sign(output["探索期_多減空差"]) ==
                 np.sign(output["驗證期_多減空差"]))
    output["前後期同方向"] = np.where(numeric_mask, same_sign, pd.NA)
    output["重複驗證通過"] = (
        numeric_mask & output["FDR顯著"] & same_sign &
        (output["探索期_q值"] < 0.10) & (output["驗證期_p值"] < settings.alpha)
    )
    year_data = []
    for row in output.itertuples(index=False):
        if row.資料型態 == "numeric":
            year_data.append(_year_stability(prepared, row.策略欄位, row.目標欄位, settings))
        else:
            year_data.append((0, 0, math.nan))
    output[["可比較年度數", "同方向年度數", "年度方向一致率"]] = pd.DataFrame(
        year_data, index=output.index
    )
    output["效果量絕對值"] = np.where(
        output["資料型態"] == "numeric",
        output["Hedges_g"].abs(),
        output.get("Cramers_V", pd.Series(np.nan, index=output.index)).abs(),
    )
    return output.sort_values(
        ["重複驗證通過", "FDR顯著", "FDR_q值", "效果量絕對值"],
        ascending=[False, False, True, False], na_position="last",
    ).reset_index(drop=True)


def build_shortlist(results: pd.DataFrame) -> pd.DataFrame:
    if results.empty:
        return results
    columns = [
        "策略欄位", "來源欄位", "目標型態", "效果方向", "多樣本數", "空樣本數",
        "多平均", "空平均", "多減空平均差", "Hedges_g", "Cramers_V", "p值",
        "FDR_q值", "探索期_q值", "驗證期_p值", "前後期同方向",
        "可比較年度數", "同方向年度數", "年度方向一致率", "重複驗證通過",
    ]
    for column in columns:
        if column not in results:
            results[column] = np.nan
    return results.loc[results["重複驗證通過"].fillna(False), columns].copy()


def export_results(results: pd.DataFrame, shortlist: pd.DataFrame, settings: Settings,
                   output_dir: Path) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    end_label = settings.end.isoformat().replace("-", "") if settings.end else "latest"
    suffix = f"{settings.start:%Y%m%d}_{end_label}"
    all_path = output_dir / f"strategy_next_day_field_scan_all_{suffix}.csv"
    shortlist_path = output_dir / f"strategy_next_day_field_scan_shortlist_{suffix}.csv"
    manifest_path = output_dir / f"strategy_next_day_field_scan_manifest_{suffix}.json"
    results.to_csv(all_path, index=False, encoding="utf-8-sig")
    shortlist.to_csv(shortlist_path, index=False, encoding="utf-8-sig")
    manifest = {
        "settings": {**asdict(settings), "start": settings.start.isoformat(),
                     "end": settings.end.isoformat() if settings.end else None,
                     "split_date": settings.split_date.isoformat()},
        "source_view": SOURCE_VIEW,
        "signal": "所有名稱以策略_開頭，且實際同時含多與空的欄位",
        "alignment": "訊號日對下一個實際存在的交易日",
        "primary_numeric_test": "Mann-Whitney U",
        "secondary_numeric_test": "Welch t",
        "categorical_test": "Chi-square and Cramer's V",
        "multiple_testing": "所有策略×目標假設一起做 Benjamini-Hochberg FDR",
        "repeated_validation": (
            "全期q<alpha、探索期q<0.10、驗證期p<alpha，且多減空方向一致"
        ),
        "warning": "探索性結果不等於可交易規則；shortlist仍需經濟幅度與樣本外回測",
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return all_path, shortlist_path, manifest_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="掃描所有策略多空對隔日欄位的差異")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--split-date", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--minimum-group-size", type=int, default=100)
    parser.add_argument("--minimum-year-size", type=int, default=20)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--winsor-tail", type=float, default=0.005)
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path("backtest_output/futures_strategy_next_day_field_scan"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(
        args.start, args.end, args.futures_id, args.minimum_group_size,
        args.minimum_year_size, args.split_date, args.alpha, args.winsor_tail,
    )
    source = load_source(settings)
    prepared, metadata = build_targets(source)
    if settings.end:
        prepared = prepared[pd.to_datetime(prepared["日期"]).dt.date <= settings.end].copy()
    strategies = detect_strategy_columns(prepared)
    if not strategies:
        raise SystemExit("找不到同時含『多／空』的策略欄位")
    full = scan_period(prepared, metadata, settings)
    results = add_validation(full, prepared, metadata, settings)
    shortlist = build_shortlist(results)
    paths = export_results(results, shortlist, settings, args.output_dir)
    print(f"策略欄位：{len(strategies)} 個")
    print(f"候選目標：{len(metadata)} 個")
    print(f"有效檢定：{len(results)} 組")
    print(f"全期FDR顯著：{int(results['FDR顯著'].sum()) if len(results) else 0} 組")
    print(f"前後期重複驗證：{len(shortlist)} 組")
    if len(shortlist):
        print("\n[重複驗證通過前30名]")
        print(shortlist.head(30).to_string(index=False))
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
