from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


SOURCE_VIEW = "report.v_futures_report_daily_corrected_turning"
SIGNED_EDGES = [-np.inf, -1.0, -0.75, -0.50, -0.25, 0.0,
                0.25, 0.50, 0.75, 1.0, np.inf]
SIGNED_LABELS = ["≤-1.00", "-1.00~-0.75", "-0.75~-0.50", "-0.50~-0.25",
                 "-0.25~0", "0~0.25", "0.25~0.50", "0.50~0.75",
                 "0.75~1.00", "≥1.00"]
MAG_EDGES = [0.0, 0.25, 0.50, 1.0, np.inf]
MAG_LABELS = ["0~0.25", "0.25~0.50", "0.50~1.00", "≥1.00"]
RATIO_EDGES = [0.0, 0.50, 1.0, 1.50, np.inf]
RATIO_LABELS = ["0~0.50", "0.50~1.00", "1.00~1.50", "≥1.50"]


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    minimum_sample_size: int = 30
    alpha: float = 0.05

    def __post_init__(self) -> None:
        if self.end and self.start > self.end:
            raise ValueError("start 不可晚於 end")
        if self.minimum_sample_size < 10:
            raise ValueError("minimum_sample_size 不可小於 10")
        if not 0 < self.alpha < 1:
            raise ValueError("alpha 必須介於 0 與 1 之間")


def load_source(settings: Settings) -> pd.DataFrame:
    """多抓起始日前資料計算前收，並保留結束日後第一個交易日作為目標。"""
    from common import db

    sql = f'''
        SELECT
            futures_id,
            "日期",
            "日盤開", "日盤高", "日盤低", "日盤收",
            "夜盤收"
        FROM {SOURCE_VIEW}
        WHERE futures_id = ?
          AND "日期" >= ?
        ORDER BY "日期"
    '''
    return db.sql_to_df(
        sql,
        params=(settings.futures_id, settings.start - timedelta(days=14)),
        parse_dates=["日期"],
    )


def _safe_pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.astype(float).div(denominator.astype(float).replace(0, np.nan)) * 100.0


def _direction(value: object, epsilon: float = 1e-12) -> str:
    if pd.isna(value):
        return "資料不足"
    number = float(value)
    if number > epsilon:
        return "漲"
    if number < -epsilon:
        return "跌"
    return "平"


def _relationship(row: pd.Series) -> str:
    day, night = row["日盤方向"], row["夜盤方向"]
    if "資料不足" in (day, night):
        return "資料不足"
    if "平" in (day, night):
        return "含平盤"
    return "反向" if day != night else "同向"


def _side_position(target: pd.Series, day_close: pd.Series,
                   night_close: pd.Series) -> pd.Series:
    low = pd.concat([day_close, night_close], axis=1).min(axis=1)
    high = pd.concat([day_close, night_close], axis=1).max(axis=1)
    between = target.between(low, high, inclusive="both")
    night_above = night_close > day_close
    night_outer = np.where(night_above, target > night_close, target < night_close)
    day_outer = np.where(night_above, target < day_close, target > day_close)
    return pd.Series(
        np.select([between, night_outer, day_outer],
                  ["兩者之間", "夜收外側", "日收外側"], default="資料不足"),
        index=target.index,
        dtype="string",
    )


def prepare_detail(source: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    frame = source.sort_values("日期").reset_index(drop=True).copy()
    numeric = ["日盤開", "日盤高", "日盤低", "日盤收", "夜盤收"]
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    detail = pd.DataFrame({
        "訊號日": frame["日期"],
        "隔日日盤日期": frame["日期"].shift(-1),
        "再前一日日盤收": frame["日盤收"].shift(1),
        "昨日日盤收": frame["日盤收"],
        "昨日夜盤收": frame["夜盤收"],
        "隔日日盤開": frame["日盤開"].shift(-1),
        "隔日日盤高": frame["日盤高"].shift(-1),
        "隔日日盤低": frame["日盤低"].shift(-1),
        "隔日日盤收": frame["日盤收"].shift(-1),
    })
    detail = detail[
        (detail["訊號日"].dt.date >= settings.start) &
        (True if settings.end is None else detail["訊號日"].dt.date <= settings.end)
    ].copy()

    detail["日盤漲跌幅_pct"] = _safe_pct(
        detail["昨日日盤收"] - detail["再前一日日盤收"], detail["再前一日日盤收"]
    )
    detail["夜盤漲跌幅_pct"] = _safe_pct(
        detail["昨日夜盤收"] - detail["昨日日盤收"], detail["昨日日盤收"]
    )
    detail["日盤方向"] = detail["日盤漲跌幅_pct"].map(_direction)
    detail["夜盤方向"] = detail["夜盤漲跌幅_pct"].map(_direction)
    detail["日夜關係"] = detail.apply(_relationship, axis=1)
    detail["日盤幅度_pct"] = detail["日盤漲跌幅_pct"].abs()
    detail["夜盤幅度_pct"] = detail["夜盤漲跌幅_pct"].abs()
    detail["反向比率"] = detail["夜盤幅度_pct"].div(
        detail["日盤幅度_pct"].replace(0, np.nan)
    )
    detail["日盤漲跌桶"] = pd.cut(
        detail["日盤漲跌幅_pct"], SIGNED_EDGES, labels=SIGNED_LABELS,
        right=False, ordered=True,
    )
    detail["夜盤漲跌桶"] = pd.cut(
        detail["夜盤漲跌幅_pct"], SIGNED_EDGES, labels=SIGNED_LABELS,
        right=False, ordered=True,
    )
    detail["夜盤幅度桶"] = pd.cut(
        detail["夜盤幅度_pct"], MAG_EDGES, labels=MAG_LABELS,
        right=False, ordered=True,
    )
    detail["反向比率桶"] = pd.cut(
        detail["反向比率"], RATIO_EDGES, labels=RATIO_LABELS,
        right=False, ordered=True,
    )

    low_anchor = detail[["昨日日盤收", "昨日夜盤收"]].min(axis=1)
    high_anchor = detail[["昨日日盤收", "昨日夜盤收"]].max(axis=1)
    anchor_span = (detail["昨日夜盤收"] - detail["昨日日盤收"]).abs()
    detail["隔日開盤位置"] = _side_position(
        detail["隔日日盤開"], detail["昨日日盤收"], detail["昨日夜盤收"]
    )
    detail["隔日收盤位置"] = _side_position(
        detail["隔日日盤收"], detail["昨日日盤收"], detail["昨日夜盤收"]
    )
    detail["隔日開盤在兩者之間"] = detail["隔日開盤位置"] == "兩者之間"
    detail["隔日收盤在兩者之間"] = detail["隔日收盤位置"] == "兩者之間"
    detail["隔日收在夜收外側"] = detail["隔日收盤位置"] == "夜收外側"
    detail["隔日收在日收外側"] = detail["隔日收盤位置"] == "日收外側"
    detail["隔日觸及昨日日收"] = (
        (detail["隔日日盤低"] <= detail["昨日日盤收"]) &
        (detail["隔日日盤高"] >= detail["昨日日盤收"])
    )
    detail["隔日觸及昨日夜收"] = (
        (detail["隔日日盤低"] <= detail["昨日夜盤收"]) &
        (detail["隔日日盤高"] >= detail["昨日夜盤收"])
    )
    detail["隔日同時觸及兩端"] = (
        (detail["隔日日盤低"] <= low_anchor) &
        (detail["隔日日盤高"] >= high_anchor)
    )
    detail["隔日日盤振幅_pct"] = _safe_pct(
        detail["隔日日盤高"] - detail["隔日日盤低"], detail["隔日日盤開"]
    )
    detail["隔日最高超過上緣_pct"] = _safe_pct(
        detail["隔日日盤高"] - high_anchor, high_anchor
    )
    detail["隔日最低跌破下緣_pct"] = _safe_pct(
        detail["隔日日盤低"] - low_anchor, low_anchor
    )
    detail["隔日收盤區間標準化位置"] = (
        (detail["隔日日盤收"] - low_anchor).div(anchor_span.replace(0, np.nan)) * 100.0
    )
    required = [
        "再前一日日盤收", "昨日日盤收", "昨日夜盤收", "隔日日盤日期",
        "隔日日盤開", "隔日日盤高", "隔日日盤低", "隔日日盤收",
    ]
    return detail.dropna(subset=required).reset_index(drop=True)


def _summary_row(keys: dict[str, object], group: pd.DataFrame) -> dict[str, object]:
    n = len(group)
    rate = lambda column: float(group[column].mean() * 100.0) if n else math.nan
    return {
        **keys,
        "樣本數": n,
        "隔日開盤在兩者之間_pct": rate("隔日開盤在兩者之間"),
        "隔日收盤在兩者之間_pct": rate("隔日收盤在兩者之間"),
        "隔日收在夜收外側_pct": rate("隔日收在夜收外側"),
        "隔日收在日收外側_pct": rate("隔日收在日收外側"),
        "隔日觸及昨日日收_pct": rate("隔日觸及昨日日收"),
        "隔日觸及昨日夜收_pct": rate("隔日觸及昨日夜收"),
        "隔日同時觸及兩端_pct": rate("隔日同時觸及兩端"),
        "隔日日盤平均振幅_pct": group["隔日日盤振幅_pct"].mean(),
        "隔日日盤中位振幅_pct": group["隔日日盤振幅_pct"].median(),
        "平均最高超過上緣_pct": group["隔日最高超過上緣_pct"].mean(),
        "平均最低跌破下緣_pct": group["隔日最低跌破下緣_pct"].mean(),
        "隔日收盤標準化位置_中位數": group["隔日收盤區間標準化位置"].median(),
    }


def summarize(detail: pd.DataFrame, group_columns: list[str],
              minimum_sample_size: int = 1) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    observed = detail.dropna(subset=group_columns)
    grouper = group_columns[0] if len(group_columns) == 1 else group_columns
    for key, group in observed.groupby(grouper, observed=True, sort=False):
        if len(group) < minimum_sample_size:
            continue
        values = (key,) if len(group_columns) == 1 else key
        keys = dict(zip(group_columns, values))
        rows.append(_summary_row(keys, group))
    return pd.DataFrame(rows)


def build_matrix(detail: pd.DataFrame) -> pd.DataFrame:
    matrix = summarize(detail, ["日盤漲跌桶", "夜盤漲跌桶"])
    if matrix.empty:
        return matrix
    matrix["日盤桶順序"] = matrix["日盤漲跌桶"].map(
        {label: index for index, label in enumerate(SIGNED_LABELS)}
    )
    matrix["夜盤桶順序"] = matrix["夜盤漲跌桶"].map(
        {label: index for index, label in enumerate(SIGNED_LABELS)}
    )
    return matrix.sort_values(["日盤桶順序", "夜盤桶順序"]).drop(
        columns=["日盤桶順序", "夜盤桶順序"]
    )


def build_layered(detail: pd.DataFrame, minimum_sample_size: int) -> pd.DataFrame:
    base = summarize(
        detail,
        ["日盤方向", "夜盤方向", "日夜關係", "夜盤幅度桶"],
        minimum_sample_size,
    )
    opposite = summarize(
        detail[detail["日夜關係"] == "反向"],
        ["日盤方向", "夜盤方向", "日夜關係", "夜盤幅度桶", "反向比率桶"],
        minimum_sample_size,
    )
    if len(base):
        base["分層類型"] = "方向×夜盤幅度"
    if len(opposite):
        opposite["分層類型"] = "反向×夜盤幅度×反向比率"
    return pd.concat([base, opposite], ignore_index=True, sort=False)


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _two_proportion_test(success_a: int, n_a: int,
                         success_b: int, n_b: int) -> tuple[float, float]:
    if min(n_a, n_b) == 0:
        return math.nan, math.nan
    pooled = (success_a + success_b) / (n_a + n_b)
    se = math.sqrt(pooled * (1 - pooled) * (1 / n_a + 1 / n_b))
    if se == 0:
        return 0.0, 1.0
    z = (success_a / n_a - success_b / n_b) / se
    return z, 2 * (1 - _normal_cdf(abs(z)))


def _bh_adjust(series: pd.Series) -> pd.Series:
    result = pd.Series(np.nan, index=series.index, dtype=float)
    clean = pd.to_numeric(series, errors="coerce").dropna().sort_values()
    if clean.empty:
        return result
    adjusted = (clean * len(clean) / np.arange(1, len(clean) + 1))[::-1].cummin()[::-1]
    result.loc[clean.index] = adjusted.clip(upper=1.0)
    return result


def compare_opposite_same(detail: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    outcomes = [
        "隔日開盤在兩者之間", "隔日收盤在兩者之間", "隔日收在夜收外側",
        "隔日收在日收外側", "隔日觸及昨日日收", "隔日觸及昨日夜收",
        "隔日同時觸及兩端",
    ]
    rows: list[dict[str, object]] = []
    usable = detail[detail["日夜關係"].isin(["反向", "同向"])].copy()
    for bucket, frame in usable.groupby("夜盤幅度桶", observed=True):
        opposite = frame[frame["日夜關係"] == "反向"]
        same = frame[frame["日夜關係"] == "同向"]
        if min(len(opposite), len(same)) < settings.minimum_sample_size:
            continue
        for outcome in outcomes:
            a_success = int(opposite[outcome].sum())
            b_success = int(same[outcome].sum())
            z, p_value = _two_proportion_test(
                a_success, len(opposite), b_success, len(same)
            )
            rows.append({
                "夜盤幅度桶": str(bucket), "比較結果": outcome,
                "反向樣本數": len(opposite), "同向樣本數": len(same),
                "反向機率_pct": a_success / len(opposite) * 100.0,
                "同向機率_pct": b_success / len(same) * 100.0,
                "反向減同向_百分點": (
                    a_success / len(opposite) - b_success / len(same)
                ) * 100.0,
                "z值": z, "p值": p_value,
            })
    result = pd.DataFrame(rows)
    if len(result):
        result["FDR_q值"] = _bh_adjust(result["p值"])
        result["FDR顯著"] = result["FDR_q值"] < settings.alpha
        result = result.sort_values(["FDR顯著", "FDR_q值"], ascending=[False, True])
    return result.reset_index(drop=True)


def build_yearly(detail: pd.DataFrame, minimum_sample_size: int) -> pd.DataFrame:
    work = detail.copy()
    work["年"] = pd.to_datetime(work["訊號日"]).dt.year
    return summarize(
        work[work["日夜關係"].isin(["反向", "同向"])],
        ["年", "日夜關係", "夜盤幅度桶"],
        minimum_sample_size,
    )


def export_reports(detail: pd.DataFrame, matrix: pd.DataFrame, layered: pd.DataFrame,
                   comparisons: pd.DataFrame, yearly: pd.DataFrame,
                   settings: Settings, output_dir: Path) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    end_value = settings.end or pd.to_datetime(detail["訊號日"]).max().date()
    suffix = f"{settings.start:%Y%m%d}_{end_value:%Y%m%d}"
    paths = (
        output_dir / f"day_night_layered_detail_{suffix}.csv",
        output_dir / f"day_night_bucket_matrix_{suffix}.csv",
        output_dir / f"day_night_layered_summary_{suffix}.csv",
        output_dir / f"day_night_opposite_vs_same_tests_{suffix}.csv",
        output_dir / f"day_night_layered_yearly_{suffix}.csv",
        output_dir / f"day_night_layered_manifest_{suffix}.json",
    )
    for frame, path in zip((detail, matrix, layered, comparisons, yearly), paths[:5]):
        frame.to_csv(path, index=False, encoding="utf-8-sig")
    manifest = {
        "settings": {**asdict(settings), "start": settings.start.isoformat(),
                     "end": settings.end.isoformat() if settings.end else None},
        "source_view": SOURCE_VIEW,
        "date_alignment": "訊號日後下一個實際存在的交易日",
        "day_return": "(昨日日盤收-再前一日日盤收)/再前一日日盤收*100",
        "night_return": "(昨日夜盤收-昨日日盤收)/昨日日盤收*100",
        "opposite": "日盤漲跌幅與夜盤漲跌幅異號",
        "between": "隔日價格位於昨日日盤收與昨日夜盤收的閉區間內",
        "night_outer": "隔日收盤超過昨日夜收，沿夜盤方向延伸",
        "day_outer": "隔日收盤越過昨日日收，完全收復夜盤走勢",
        "comparison": "相同夜盤絕對幅度桶內，比較反向與同向；全項BH-FDR校正",
    }
    paths[5].write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="日盤收、夜盤收與隔日日盤路徑分層回測")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--minimum-sample-size", type=int, default=30)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path("backtest_output/futures_day_night_next_day_layered"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(
        args.start, args.end, args.futures_id, args.minimum_sample_size, args.alpha
    )
    detail = prepare_detail(load_source(settings), settings)
    if detail.empty:
        raise SystemExit("查無完整有效資料")
    matrix = build_matrix(detail)
    layered = build_layered(detail, settings.minimum_sample_size)
    comparisons = compare_opposite_same(detail, settings)
    yearly = build_yearly(detail, settings.minimum_sample_size)
    paths = export_reports(
        detail, matrix, layered, comparisons, yearly, settings, args.output_dir
    )
    print(f"有效訊號日：{len(detail):,}")
    print(f"二維矩陣有效格：{len(matrix):,}")
    print(f"分層摘要：{len(layered):,}")
    print(f"反向vs同向檢定：{len(comparisons):,}")
    if len(comparisons):
        print(f"FDR顯著：{int(comparisons['FDR顯著'].sum()):,}")
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
