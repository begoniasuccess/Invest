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

ANALYSIS_NAME = "futures_margin_maintenance_scan"
METRIC_VERSION = "1.0.0"
SOURCE_VIEW = "report.v_futures_report_daily_corrected_turning"
TARGET_VIEW = "research.v_futures_daily_amplitude_pattern"
MAINTENANCE_TABLE = "raw_finmind.fm_taiwan_total_exchange_margin_maintenance"
BUCKETS = ("Q1最低20%", "Q2偏低", "Q3中間", "Q4偏高", "Q5最高20%")
FEATURES = {
    "維持率水準": "當日維持率相對此前100筆有效資料的歷史百分位；Q1代表相對低位",
    "單日急降幅度": "前一交易日維持率減當日維持率（百分點）；越大代表下降越急",
    "三日急降幅度": "三個交易日前維持率減當日維持率（百分點）；越大代表累計下降越多",
}
OUTCOMES = {
    "next_range_pct": "次日日盤振幅百分比=(日高-日低)/日開*100",
    "range_ratio": "次日日盤振幅/此前平均振幅（沿用既有標籤 view）",
    "劇烈以上": "次日日盤振幅標籤為劇烈或極端",
    "next_open_close_pct": "次日日盤開收報酬百分比",
    "next_close_return_pct": "訊號日收盤至次日日盤收盤報酬百分比",
    "next_up": "次日日盤收盤高於開盤",
    "low_close_rebound_points": "次日日盤低點至收盤反彈點數",
}


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    split_date: date = date(2023, 1, 1)
    futures_id: str = "TX"
    history_window: int = 100
    minimum_history: int = 80
    low_percentile: float = 20.0
    steep_drop_percentile: float = 90.0
    minimum_group_size: int = 20

    def validate(self) -> None:
        if self.end and self.end < self.start:
            raise ValueError("end 不可早於 start")
        if self.split_date <= self.start:
            raise ValueError("split-date 必須晚於 start")
        if self.history_window < 20:
            raise ValueError("history-window 至少為20")
        if not 20 <= self.minimum_history <= self.history_window:
            raise ValueError("minimum-history 必須介於20與history-window之間")
        if not 0 < self.low_percentile < 50:
            raise ValueError("low-percentile 必須介於0與50之間")
        if not 50 < self.steep_drop_percentile < 100:
            raise ValueError("steep-drop-percentile 必須介於50與100之間")
        if self.minimum_group_size < 5:
            raise ValueError("minimum-group-size 不可小於5")


def load_inputs(settings: Settings) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from common import db

    settings.validate()
    query_start = settings.start - timedelta(days=max(400, settings.history_window * 3))
    query_end = (settings.end or date.today()) + timedelta(days=10)
    source = db.sql_to_df(
        f'''SELECT futures_id, "日期", "日盤開", "日盤高", "日盤低", "日盤收"
            FROM {SOURCE_VIEW}
            WHERE futures_id = ? AND "日期" BETWEEN ? AND ?
            ORDER BY "日期"''',
        params=(settings.futures_id, query_start, query_end),
        parse_dates=["日期"],
    )
    maintenance = db.sql_to_df(
        f'''SELECT date, total_exchange_margin_maintenance AS margin_maintenance
            FROM {MAINTENANCE_TABLE}
            WHERE date BETWEEN ? AND ?
            ORDER BY date''',
        params=(query_start, query_end),
        parse_dates=["date"],
    )
    targets = db.sql_to_df(
        f'''SELECT trading_date, range_ratio, amplitude_label
            FROM {TARGET_VIEW}
            WHERE futures_id = ? AND valid_for_research
              AND trading_date BETWEEN ? AND ?
            ORDER BY trading_date''',
        params=(settings.futures_id, query_start, query_end),
        parse_dates=["trading_date"],
    )
    return source, maintenance, targets


def _historical_percentile(values: pd.Series, window: int) -> pd.Series:
    """Percentile of each value against the preceding window only (no look-ahead)."""
    array = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    output = np.full(len(array), np.nan, dtype=float)
    for i, value in enumerate(array):
        if not np.isfinite(value):
            continue
        history = array[max(0, i - window):i]
        history = history[np.isfinite(history)]
        if not len(history):
            continue
        less = np.count_nonzero(history < value)
        equal = np.count_nonzero(history == value)
        output[i] = (less + equal * 0.5) / len(history) * 100.0
    return pd.Series(output, index=values.index, dtype=float)


def _bucket(percentile: pd.Series) -> pd.Series:
    return pd.cut(
        percentile,
        bins=[-np.inf, 20, 40, 60, 80, np.inf],
        labels=BUCKETS,
        include_lowest=True,
        ordered=True,
    )


def prepare_detail(
    source: pd.DataFrame,
    maintenance: pd.DataFrame,
    targets: pd.DataFrame,
    settings: Settings,
) -> pd.DataFrame:
    src = source.copy()
    src["日期"] = pd.to_datetime(src["日期"], errors="coerce").dt.normalize()
    src = src.dropna(subset=["日期"]).sort_values("日期").drop_duplicates("日期", keep="last").reset_index(drop=True)
    for col in ("日盤開", "日盤高", "日盤低", "日盤收"):
        src[col] = pd.to_numeric(src[col], errors="coerce")

    # Each signal date t predicts the next observed TX day session, not the same-day session.
    src["結果日"] = src["日期"].shift(-1)
    src["結果日開"] = src["日盤開"].shift(-1)
    src["結果日高"] = src["日盤高"].shift(-1)
    src["結果日低"] = src["日盤低"].shift(-1)
    src["結果日收"] = src["日盤收"].shift(-1)

    maint = maintenance.copy()
    maint["date"] = pd.to_datetime(maint["date"], errors="coerce").dt.normalize()
    maint["margin_maintenance"] = pd.to_numeric(maint["margin_maintenance"], errors="coerce")
    maint = maint.dropna(subset=["date", "margin_maintenance"]).drop_duplicates("date", keep="last")
    src = src.merge(maint.rename(columns={"date": "日期"}), on="日期", how="left", validate="one_to_one")

    # No forward-fill: a missing market data date stays missing and is excluded.
    src["單日急降_pct點"] = src["margin_maintenance"].shift(1) - src["margin_maintenance"]
    src["三日急降_pct點"] = src["margin_maintenance"].shift(3) - src["margin_maintenance"]
    feature_values = {
        "維持率水準": src["margin_maintenance"],
        "單日急降幅度": src["單日急降_pct點"],
        "三日急降幅度": src["三日急降_pct點"],
    }
    for name, values in feature_values.items():
        src[f"{name}_歷史分位_pct"] = _historical_percentile(values, settings.history_window)
        src[f"{name}_五分位"] = _bucket(src[f"{name}_歷史分位_pct"])

    target = targets.copy()
    target["trading_date"] = pd.to_datetime(target["trading_date"], errors="coerce").dt.normalize()
    target["range_ratio"] = pd.to_numeric(target["range_ratio"], errors="coerce")
    target = target.dropna(subset=["trading_date"]).drop_duplicates("trading_date", keep="last")
    src = src.merge(
        target.rename(columns={"trading_date": "結果日"}),
        on="結果日", how="left", validate="many_to_one",
    )

    open_ = src["結果日開"]
    prior_close = src["日盤收"]
    src["next_range_pct"] = (src["結果日高"] - src["結果日低"]) / open_.replace(0, np.nan) * 100.0
    src["next_open_close_pct"] = (src["結果日收"] - open_) / open_.replace(0, np.nan) * 100.0
    src["next_close_return_pct"] = (src["結果日收"] - prior_close) / prior_close.replace(0, np.nan) * 100.0
    src["next_up"] = np.where(
        src["結果日收"].notna() & open_.notna(),
        (src["結果日收"] > open_).astype(float),
        np.nan,
    )
    src["low_close_rebound_points"] = src["結果日收"] - src["結果日低"]
    src["劇烈以上"] = src["amplitude_label"].isin(["劇烈振幅", "極端振幅"]).astype(float)
    src.loc[src["amplitude_label"].isna(), "劇烈以上"] = np.nan

    end = settings.end or date.today()
    src["訊號日"] = src["日期"]
    src = src[(src["結果日"] >= pd.Timestamp(settings.start)) & (src["結果日"] <= pd.Timestamp(end))]
    valid = src["margin_maintenance"].notna() & src["next_range_pct"].notna()
    return src[valid].copy().reset_index(drop=True)


def _safe_mean(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.mean()) if len(values) else math.nan


def _summarize_group(data: pd.DataFrame, group_cols: list[str], settings: Settings) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, frame in data.groupby(group_cols, observed=False, dropna=False, sort=True):
        keys = keys if isinstance(keys, tuple) else (keys,)
        row = dict(zip(group_cols, keys))
        row["樣本數"] = len(frame)
        row["劇烈以上有效樣本數"] = int(frame["劇烈以上"].notna().sum())
        row["劇烈以上樣本數"] = int(frame["劇烈以上"].sum(skipna=True))
        row["劇烈以上發生率_pct"] = _safe_mean(frame["劇烈以上"]) * 100
        row["平均次日日盤振幅_pct"] = _safe_mean(frame["next_range_pct"])
        row["中位數次日日盤振幅_pct"] = float(frame["next_range_pct"].median()) if len(frame) else math.nan
        row["平均振幅倍數"] = _safe_mean(frame["range_ratio"])
        row["次日日盤上漲率_pct"] = _safe_mean(frame["next_up"]) * 100
        row["平均次日日盤開收_pct"] = _safe_mean(frame["next_open_close_pct"])
        row["平均訊號日收至次日收_pct"] = _safe_mean(frame["next_close_return_pct"])
        row["平均低點至收盤反彈點數"] = _safe_mean(frame["low_close_rebound_points"])
        row["樣本狀態"] = "足夠" if len(frame) >= settings.minimum_group_size else "樣本偏少"
        rows.append(row)
    return pd.DataFrame(rows)


def build_bucket_summary(detail: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    periods = [("全期", detail)]
    result_dates = pd.to_datetime(detail["結果日"])
    periods.extend([
        ("探索期", detail[result_dates.dt.date < settings.split_date]),
        ("驗證期", detail[result_dates.dt.date >= settings.split_date]),
    ])
    pieces = []
    for period, frame in periods:
        for feature in FEATURES:
            subset = frame.dropna(subset=[f"{feature}_五分位"])
            if subset.empty:
                continue
            grouped = _summarize_group(subset, [f"{feature}_五分位"], settings)
            grouped.insert(0, "研究期間", period)
            grouped.insert(1, "特徵", feature)
            grouped = grouped.rename(columns={f"{feature}_五分位": "五分位"})
            baseline = _safe_mean(subset["劇烈以上"])
            grouped["相對母體事件率_pct"] = grouped["劇烈以上發生率_pct"] - baseline * 100
            grouped["劇烈以上Lift"] = grouped["劇烈以上發生率_pct"] / (baseline * 100) if baseline > 0 else np.nan
            pieces.append(grouped)
    return pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()


def build_combination_summary(detail: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    frame = detail.copy()
    frame["低位"] = np.where(
        frame["維持率水準_歷史分位_pct"].notna(),
        frame["維持率水準_歷史分位_pct"] <= settings.low_percentile,
        np.nan,
    )
    frame["急降"] = np.where(
        frame["單日急降幅度_歷史分位_pct"].notna(),
        frame["單日急降幅度_歷史分位_pct"] >= settings.steep_drop_percentile,
        np.nan,
    )
    frame["低位急降分組"] = frame.apply(
        lambda r: ("低位" if r["低位"] else "非低位") + " × " + ("急降" if r["急降"] else "非急降")
        if pd.notna(r["低位"]) and pd.notna(r["急降"]) else np.nan,
        axis=1,
    )
    periods = [("全期", frame)]
    result_dates = pd.to_datetime(frame["結果日"])
    periods.extend([
        ("探索期", frame[result_dates.dt.date < settings.split_date]),
        ("驗證期", frame[result_dates.dt.date >= settings.split_date]),
    ])
    pieces = []
    for period, subset in periods:
        subset = subset.dropna(subset=["低位急降分組"])
        if subset.empty:
            continue
        grouped = _summarize_group(subset, ["低位急降分組"], settings)
        grouped.insert(0, "研究期間", period)
        grouped.insert(1, "低位門檻_pct", settings.low_percentile)
        grouped.insert(2, "急降門檻_pct", settings.steep_drop_percentile)
        pieces.append(grouped)
    return pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()


def build_yearly_summary(detail: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    work = detail.copy()
    work["結果年"] = pd.to_datetime(work["結果日"]).dt.year
    pieces = []
    for feature in FEATURES:
        grouped = _summarize_group(
            work.dropna(subset=[f"{feature}_五分位"]),
            ["結果年", f"{feature}_五分位"], settings,
        )
        if not grouped.empty:
            grouped.insert(1, "特徵", feature)
            grouped = grouped.rename(columns={f"{feature}_五分位": "五分位"})
            pieces.append(grouped)
    return pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()


def _bh_adjust(values: pd.Series) -> pd.Series:
    p = pd.to_numeric(values, errors="coerce")
    valid = p.notna()
    out = pd.Series(np.nan, index=values.index, dtype=float)
    if not valid.any():
        return out
    raw = p[valid].to_numpy()
    order = np.argsort(raw)
    ranked = raw[order] * len(raw) / np.arange(1, len(raw) + 1)
    ranked = np.minimum.accumulate(ranked[::-1])[::-1]
    adjusted = np.empty(len(raw))
    adjusted[order] = np.minimum(ranked, 1.0)
    out.loc[valid] = adjusted
    return out


def build_tests(detail: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for feature in FEATURES:
        for outcome in ("next_range_pct", "range_ratio", "劇烈以上"):
            usable = detail[[f"{feature}_歷史分位_pct", f"{feature}_五分位", outcome]].dropna()
            row: dict[str, Any] = {"特徵": feature, "結果指標": outcome, "樣本數": len(usable)}
            if len(usable) >= settings.minimum_group_size * 2 and usable[outcome].nunique() > 1:
                rho, p_value = spearmanr(usable[f"{feature}_歷史分位_pct"], usable[outcome])
                row["Spearman_rho"] = rho
                row["趨勢_p值"] = p_value
                low = usable[usable[f"{feature}_五分位"] == BUCKETS[0]][outcome]
                high = usable[usable[f"{feature}_五分位"] == BUCKETS[-1]][outcome]
                row["Q1平均"] = _safe_mean(low)
                row["Q5平均"] = _safe_mean(high)
                row["Q5減Q1"] = row["Q5平均"] - row["Q1平均"]
                if outcome == "劇烈以上" and min(len(low), len(high)) >= settings.minimum_group_size:
                    table = [[int(high.sum()), len(high) - int(high.sum())],
                             [int(low.sum()), len(low) - int(low.sum())]]
                    row["Q1Q5_Fisher_p值"] = fisher_exact(table, alternative="two-sided").pvalue
                five = pd.crosstab(usable[f"{feature}_五分位"], usable[outcome]).reindex(BUCKETS, fill_value=0)
                if five.shape[1] == 2 and (five.sum(axis=1) >= settings.minimum_group_size).all():
                    chi2, p_chi, _, _ = chi2_contingency(five)
                    row["五分位差異_p值"] = p_chi
                    row["Cramers_V"] = math.sqrt(chi2 / len(usable))
            rows.append(row)
    result = pd.DataFrame(rows)
    for col in ("趨勢_p值", "Q1Q5_Fisher_p值", "五分位差異_p值"):
        result[col.replace("p值", "q值")] = _bh_adjust(result[col])
    return result


def export_results(
    detail: pd.DataFrame,
    buckets: pd.DataFrame,
    combinations: pd.DataFrame,
    yearly: pd.DataFrame,
    tests: pd.DataFrame,
    settings: Settings,
    output_dir: Path,
) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{settings.start:%Y%m%d}_{settings.end:%Y%m%d}" if settings.end else f"{settings.start:%Y%m%d}_latest"
    frames = (detail, buckets, combinations, yearly, tests)
    names = (
        f"margin_maintenance_detail_{suffix}.csv",
        f"margin_maintenance_buckets_{suffix}.csv",
        f"margin_maintenance_low_drop_combo_{suffix}.csv",
        f"margin_maintenance_yearly_{suffix}.csv",
        f"margin_maintenance_tests_{suffix}.csv",
    )
    paths = tuple(output_dir / name for name in names)
    for frame, path in zip(frames, paths):
        frame.to_csv(path, index=False, encoding="utf-8-sig")
    manifest = {
        "analysis_name": ANALYSIS_NAME,
        "metric_version": METRIC_VERSION,
        "run_id": str(uuid.uuid4()),
        "settings": {**asdict(settings), "start": settings.start.isoformat(),
                     "end": settings.end.isoformat() if settings.end else None,
                     "split_date": settings.split_date.isoformat()},
        "source_view": SOURCE_VIEW,
        "target_view": TARGET_VIEW,
        "maintenance_table": MAINTENANCE_TABLE,
        "features": FEATURES,
        "outcomes": OUTCOMES,
        "rules": {
            "alignment": "訊號日 t 的維持率資料，只對應 t 之後下一個有效 TX 日盤結果",
            "historical_percentile": f"當日特徵與此前最多{settings.history_window}筆有效觀測比較，不包含當日及未來",
            "missing_dates": "不前向填補缺漏日期；無維持率資料的訊號日排除",
            "low_level": f"維持率水準歷史分位 <= {settings.low_percentile}%",
            "steep_drop": f"單日急降幅度歷史分位 >= {settings.steep_drop_percentile}%",
            "multiple_testing": "趨勢、Q1/Q5與五分位檢定各自採BH-FDR；僅供探索，不作因果結論",
        },
        "warnings": [
            "大盤融資維持率是市場整體代理變數，不等同個別帳戶追繳狀態",
            "下降可能與訊號日前大盤下跌和近期波動同時發生；本版分組結果不代表維持率的增量因果效果",
            "方向與反彈欄位是次要描述結果，主要研究目標仍為波動幅度",
        ],
    }
    manifest_path = output_dir / f"margin_maintenance_manifest_{suffix}.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return (*paths, manifest_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="大盤融資維持率與台指期次日日盤振幅、方向分層分析")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--split-date", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--history-window", type=int, default=100)
    parser.add_argument("--minimum-history", type=int, default=80)
    parser.add_argument("--low-percentile", type=float, default=20.0)
    parser.add_argument("--steep-drop-percentile", type=float, default=90.0)
    parser.add_argument("--minimum-group-size", type=int, default=20)
    parser.add_argument("--output-dir", type=Path, default=Path("../output/analysis/futures_margin_maintenance_scan"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(
        start=args.start, end=args.end, split_date=args.split_date, futures_id=args.futures_id,
        history_window=args.history_window, minimum_history=args.minimum_history,
        low_percentile=args.low_percentile, steep_drop_percentile=args.steep_drop_percentile,
        minimum_group_size=args.minimum_group_size,
    )
    settings.validate()
    source, maintenance, targets = load_inputs(settings)
    detail = prepare_detail(source, maintenance, targets, settings)
    buckets = build_bucket_summary(detail, settings)
    combinations = build_combination_summary(detail, settings)
    yearly = build_yearly_summary(detail, settings)
    tests = build_tests(detail, settings)
    paths = export_results(detail, buckets, combinations, yearly, tests, settings, args.output_dir)
    print(f"有效訊號日：{len(detail):,}")
    print(f"維持率觀測：{maintenance['margin_maintenance'].notna().sum():,}")
    print("輸出檔案：")
    for path in paths:
        print(f"  {path}")


if __name__ == "__main__":
    main()
