from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd


SOURCE_VIEW = "report.v_futures_report_daily_corrected_turning"

GAP_EDGES = [0.0, 0.10, 0.25, 0.50, 0.75, 1.00, np.inf]
GAP_LABELS = ["0~0.10", "0.10~0.25", "0.25~0.50", "0.50~0.75", "0.75~1.00", "≥1.00"]
RECOVERY_THRESHOLDS = (25, 50, 75, 100)
FIXED_MOVE_THRESHOLDS = (0.10, 0.25, 0.50)


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
    """保留 end 之後的資料，讓 end 當日可對齊下一個實際交易日。"""
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
        params=(settings.futures_id, settings.start),
        parse_dates=["日期"],
    )


def _safe_pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = pd.to_numeric(denominator, errors="coerce").replace(0, np.nan)
    return pd.to_numeric(numerator, errors="coerce").div(denominator) * 100.0


def _anchor_position(day_close: pd.Series, night_close: pd.Series,
                     target: pd.Series) -> pd.Series:
    low = pd.concat([day_close, night_close], axis=1).min(axis=1)
    high = pd.concat([day_close, night_close], axis=1).max(axis=1)
    between = target.between(low, high, inclusive="both")
    night_above = night_close > day_close
    night_outer = np.where(night_above, target > night_close, target < night_close)
    day_outer = np.where(night_above, target < day_close, target > day_close)
    same_anchor = night_close == day_close
    labels = np.select(
        [same_anchor, between, night_outer, day_outer],
        ["日收等於夜收", "日收與夜收之間", "夜收外側", "日收外側"],
        default="資料不足",
    )
    return pd.Series(labels, index=target.index, dtype="string")


def prepare_detail(source: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    frame = source.sort_values("日期").reset_index(drop=True).copy()
    for column in ["日盤開", "日盤高", "日盤低", "日盤收", "夜盤收"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    detail = pd.DataFrame({
        "訊號日": frame["日期"],
        "昨日日盤收": frame["日盤收"],
        "昨日夜盤收": frame["夜盤收"],
        "隔日日盤日期": frame["日期"].shift(-1),
        "隔日日盤開": frame["日盤開"].shift(-1),
        "隔日日盤高": frame["日盤高"].shift(-1),
        "隔日日盤低": frame["日盤低"].shift(-1),
        "隔日日盤收": frame["日盤收"].shift(-1),
    })
    date_mask = detail["訊號日"].dt.date >= settings.start
    if settings.end is not None:
        date_mask &= detail["訊號日"].dt.date <= settings.end
    detail = detail[date_mask].copy()

    required = [
        "昨日日盤收", "昨日夜盤收", "隔日日盤日期", "隔日日盤開",
        "隔日日盤高", "隔日日盤低", "隔日日盤收",
    ]
    detail = detail.dropna(subset=required).reset_index(drop=True)

    detail["開盤減夜收_點"] = detail["隔日日盤開"] - detail["昨日夜盤收"]
    detail["開盤距夜收_點"] = detail["開盤減夜收_點"].abs()
    detail["開盤距夜收_pct"] = _safe_pct(
        detail["開盤距夜收_點"], detail["昨日夜盤收"]
    )
    detail["開盤相對夜收"] = np.select(
        [detail["開盤減夜收_點"] > 0, detail["開盤減夜收_點"] < 0],
        ["開在夜收上方", "開在夜收下方"],
        default="開盤等於夜收",
    )
    detail["夜收方向"] = np.select(
        [detail["昨日夜盤收"] > detail["隔日日盤開"],
         detail["昨日夜盤收"] < detail["隔日日盤開"]],
        ["向上", "向下"],
        default="無距離",
    )

    detail["開盤減昨日日收_點"] = detail["隔日日盤開"] - detail["昨日日盤收"]
    detail["開盤距昨日日收_點"] = detail["開盤減昨日日收_點"].abs()
    detail["開盤距昨日日收_pct"] = _safe_pct(
        detail["開盤距昨日日收_點"], detail["昨日日盤收"]
    )
    detail["開盤錨點位置"] = _anchor_position(
        detail["昨日日盤收"], detail["昨日夜盤收"], detail["隔日日盤開"]
    )

    toward_sign = np.sign(detail["昨日夜盤收"] - detail["隔日日盤開"])
    toward_points = np.where(
        toward_sign > 0,
        detail["隔日日盤高"] - detail["隔日日盤開"],
        detail["隔日日盤開"] - detail["隔日日盤低"],
    )
    away_points = np.where(
        toward_sign > 0,
        detail["隔日日盤開"] - detail["隔日日盤低"],
        detail["隔日日盤高"] - detail["隔日日盤開"],
    )
    detail["盤中最大向夜收_點"] = np.maximum(toward_points, 0.0)
    detail["盤中最大背離夜收_點"] = np.maximum(away_points, 0.0)
    detail["盤中最大向夜收_pct"] = _safe_pct(
        detail["盤中最大向夜收_點"], detail["隔日日盤開"]
    )
    detail["盤中最大背離夜收_pct"] = _safe_pct(
        detail["盤中最大背離夜收_點"], detail["隔日日盤開"]
    )
    detail["盤中收回原始距離_pct"] = _safe_pct(
        detail["盤中最大向夜收_點"], detail["開盤距夜收_點"]
    )
    detail["盤中向夜收幅度大於背離幅度"] = (
        detail["盤中最大向夜收_點"] > detail["盤中最大背離夜收_點"]
    )

    close_points = toward_sign * (detail["隔日日盤收"] - detail["隔日日盤開"])
    detail["收盤向夜收移動_點"] = close_points
    detail["收盤向夜收移動_pct"] = _safe_pct(close_points, detail["隔日日盤開"])
    detail["收盤收回原始距離_pct"] = _safe_pct(
        close_points, detail["開盤距夜收_點"]
    )
    detail["收盤往夜收方向"] = close_points > 0
    detail["收盤比開盤更接近夜收"] = (
        (detail["隔日日盤收"] - detail["昨日夜盤收"]).abs()
        < detail["開盤距夜收_點"]
    )
    detail["收盤抵達或越過夜收"] = detail["收盤收回原始距離_pct"] >= 100.0

    for threshold in RECOVERY_THRESHOLDS:
        detail[f"盤中收回至少{threshold}pct"] = (
            detail["盤中收回原始距離_pct"] >= threshold
        )
    for threshold in FIXED_MOVE_THRESHOLDS:
        label = f"{threshold:.2f}"
        detail[f"盤中向夜收至少{label}pct"] = (
            detail["盤中最大向夜收_pct"] >= threshold
        )

    detail["開盤距夜收桶"] = pd.cut(
        detail["開盤距夜收_pct"], GAP_EDGES, labels=GAP_LABELS,
        right=False, ordered=True,
    )
    detail["開盤距昨日日收桶"] = pd.cut(
        detail["開盤距昨日日收_pct"], GAP_EDGES, labels=GAP_LABELS,
        right=False, ordered=True,
    )

    # 開盤等於夜收時沒有「往夜收方向」，不納入收束研究。
    detail = detail[detail["開盤距夜收_點"] > 0].copy()
    return detail.reset_index(drop=True)


def _rate(group: pd.DataFrame, column: str) -> float:
    return float(group[column].mean() * 100.0) if len(group) else math.nan


def _summary_row(keys: dict[str, object], group: pd.DataFrame) -> dict[str, object]:
    row: dict[str, object] = {
        **keys,
        "樣本數": len(group),
        "開盤距夜收平均_pct": group["開盤距夜收_pct"].mean(),
        "開盤距夜收中位數_pct": group["開盤距夜收_pct"].median(),
        "盤中最大向夜收平均_pct": group["盤中最大向夜收_pct"].mean(),
        "盤中最大向夜收中位數_pct": group["盤中最大向夜收_pct"].median(),
        "盤中收回原始距離中位數_pct": group["盤中收回原始距離_pct"].median(),
        "盤中向夜收幅度大於背離幅度_pct": _rate(
            group, "盤中向夜收幅度大於背離幅度"
        ),
        "收盤往夜收方向_pct": _rate(group, "收盤往夜收方向"),
        "收盤比開盤更接近夜收_pct": _rate(group, "收盤比開盤更接近夜收"),
        "收盤抵達或越過夜收_pct": _rate(group, "收盤抵達或越過夜收"),
        "收盤向夜收平均移動_pct": group["收盤向夜收移動_pct"].mean(),
        "收盤向夜收中位移動_pct": group["收盤向夜收移動_pct"].median(),
        "收盤收回原始距離中位數_pct": group["收盤收回原始距離_pct"].median(),
    }
    for threshold in RECOVERY_THRESHOLDS:
        row[f"盤中收回至少{threshold}pct_機率"] = _rate(
            group, f"盤中收回至少{threshold}pct"
        )
    for threshold in FIXED_MOVE_THRESHOLDS:
        label = f"{threshold:.2f}"
        row[f"盤中向夜收至少{label}pct_機率"] = _rate(
            group, f"盤中向夜收至少{label}pct"
        )
    return row


def summarize(detail: pd.DataFrame, group_columns: list[str],
              minimum_sample_size: int = 1) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    work = detail.dropna(subset=group_columns)
    grouper = group_columns[0] if len(group_columns) == 1 else group_columns
    for key, group in work.groupby(grouper, observed=True, sort=False):
        if len(group) < minimum_sample_size:
            continue
        values = (key,) if len(group_columns) == 1 else key
        rows.append(_summary_row(dict(zip(group_columns, values)), group))
    return pd.DataFrame(rows)


def build_night_gap_summary(detail: pd.DataFrame) -> pd.DataFrame:
    overall = summarize(detail, ["開盤距夜收桶"])
    if len(overall):
        overall.insert(0, "分層類型", "距離")
        overall.insert(1, "開盤相對夜收", "全部")
    directional = summarize(detail, ["開盤相對夜收", "開盤距夜收桶"])
    if len(directional):
        directional.insert(0, "分層類型", "方向×距離")
    return pd.concat([overall, directional], ignore_index=True, sort=False)


def build_anchor_position_summary(detail: pd.DataFrame,
                                  minimum_sample_size: int) -> pd.DataFrame:
    return summarize(
        detail,
        ["開盤錨點位置", "開盤距夜收桶"],
        minimum_sample_size,
    )


def build_gap_matrix(detail: pd.DataFrame,
                     minimum_sample_size: int) -> pd.DataFrame:
    return summarize(
        detail,
        ["開盤距夜收桶", "開盤距昨日日收桶"],
        minimum_sample_size,
    )


def build_yearly(detail: pd.DataFrame, minimum_sample_size: int) -> pd.DataFrame:
    work = detail.copy()
    work["年"] = pd.to_datetime(work["訊號日"]).dt.year
    return summarize(work, ["年", "開盤距夜收桶"], minimum_sample_size)


def _bh_adjust(series: pd.Series) -> pd.Series:
    result = pd.Series(np.nan, index=series.index, dtype=float)
    clean = pd.to_numeric(series, errors="coerce").dropna().sort_values()
    if clean.empty:
        return result
    adjusted = (clean * len(clean) / np.arange(1, len(clean) + 1))[::-1].cummin()[::-1]
    result.loc[clean.index] = adjusted.clip(upper=1.0)
    return result


def build_statistical_tests(detail: pd.DataFrame, settings: Settings) -> pd.DataFrame:
    from scipy.stats import chi2_contingency, kruskal, spearmanr

    rows: list[dict[str, object]] = []
    metrics = [
        ("盤中最大向夜收_pct", "盤中向夜收絕對幅度"),
        ("盤中收回原始距離_pct", "盤中收回原始距離比例"),
        ("收盤向夜收移動_pct", "收盤向夜收移動幅度"),
        ("收盤收回原始距離_pct", "收盤收回原始距離比例"),
        ("收盤往夜收方向", "收盤往夜收方向"),
    ]
    scopes = [("全部", detail)] + [
        (str(name), group)
        for name, group in detail.groupby("開盤相對夜收", observed=True)
    ]
    for scope, frame in scopes:
        for column, label in metrics:
            pair = frame[["開盤距夜收_pct", column]].dropna()
            if len(pair) < settings.minimum_sample_size:
                continue
            rho, p_value = spearmanr(pair["開盤距夜收_pct"], pair[column])
            rows.append({
                "檢定類型": "Spearman距離趨勢",
                "範圍": scope,
                "開盤距夜收桶": "全部",
                "結果指標": label,
                "樣本數": len(pair),
                "統計值": rho,
                "p值": p_value,
                "說明": "正值代表開盤距夜收越遠，該收束指標傾向越高",
            })

    usable_positions = ["夜收外側", "日收與夜收之間", "日收外側"]
    for bucket, frame in detail.groupby("開盤距夜收桶", observed=True):
        groups = [
            group for name, group in frame.groupby("開盤錨點位置", observed=True)
            if name in usable_positions and len(group) >= settings.minimum_sample_size
        ]
        if len(groups) < 2:
            continue
        values = [group["盤中收回原始距離_pct"].dropna() for group in groups]
        values = [value for value in values if len(value) >= settings.minimum_sample_size]
        if len(values) >= 2:
            statistic, p_value = kruskal(*values)
            rows.append({
                "檢定類型": "Kruskal位置差異",
                "範圍": "開盤錨點位置",
                "開盤距夜收桶": str(bucket),
                "結果指標": "盤中收回原始距離比例",
                "樣本數": sum(map(len, values)),
                "統計值": statistic,
                "p值": p_value,
                "說明": "相同夜收距離桶內，比較開盤相對昨日日收的位置",
            })

        table = []
        for group in groups:
            success = int(group["收盤往夜收方向"].sum())
            table.append([success, len(group) - success])
        if len(table) >= 2 and all(sum(row) > 0 for row in table):
            statistic, p_value, _, _ = chi2_contingency(table)
            rows.append({
                "檢定類型": "卡方位置差異",
                "範圍": "開盤錨點位置",
                "開盤距夜收桶": str(bucket),
                "結果指標": "收盤往夜收方向",
                "樣本數": sum(map(sum, table)),
                "統計值": statistic,
                "p值": p_value,
                "說明": "相同夜收距離桶內，比較開盤相對昨日日收的位置",
            })

    result = pd.DataFrame(rows)
    if len(result):
        result["FDR_q值"] = _bh_adjust(result["p值"])
        result["FDR顯著"] = result["FDR_q值"] < settings.alpha
        result = result.sort_values(["FDR顯著", "FDR_q值"], ascending=[False, True])
    return result.reset_index(drop=True)


def export_reports(detail: pd.DataFrame, night_gap: pd.DataFrame,
                   anchor_position: pd.DataFrame, gap_matrix: pd.DataFrame,
                   yearly: pd.DataFrame, tests: pd.DataFrame,
                   settings: Settings, output_dir: Path) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    end_value = settings.end or pd.to_datetime(detail["訊號日"]).max().date()
    suffix = f"{settings.start:%Y%m%d}_{end_value:%Y%m%d}"
    paths = (
        output_dir / f"open_night_convergence_detail_{suffix}.csv",
        output_dir / f"open_night_convergence_by_gap_{suffix}.csv",
        output_dir / f"open_night_convergence_by_anchor_position_{suffix}.csv",
        output_dir / f"open_night_convergence_gap_matrix_{suffix}.csv",
        output_dir / f"open_night_convergence_yearly_{suffix}.csv",
        output_dir / f"open_night_convergence_tests_{suffix}.csv",
        output_dir / f"open_night_convergence_manifest_{suffix}.json",
    )
    frames = (detail, night_gap, anchor_position, gap_matrix, yearly, tests)
    for frame, path in zip(frames, paths[:6]):
        frame.to_csv(path, index=False, encoding="utf-8-sig")

    manifest = {
        "settings": {
            **asdict(settings),
            "start": settings.start.isoformat(),
            "end": settings.end.isoformat() if settings.end else None,
        },
        "source_view": SOURCE_VIEW,
        "date_alignment": "訊號日之後下一個實際存在的交易日",
        "research_question": "隔日日盤開盤距離昨日夜盤收越遠時，盤中與收盤是否更傾向往夜盤收方向移動",
        "night_gap": "abs(隔日日盤開-昨日夜盤收)/昨日夜盤收*100",
        "intraday_toward": "夜收在開盤上方使用日盤高-開盤；夜收在開盤下方使用開盤-日盤低",
        "intraday_recovery_ratio": "盤中最大向夜收點數/開盤距夜收點數*100；100%代表曾觸及或越過夜收",
        "close_toward": "sign(夜收-開盤)*(日盤收-開盤)；正值代表收盤往夜收方向",
        "close_recovery_ratio": "收盤向夜收移動點數/開盤距夜收點數*100，可為負數或超過100%",
        "closer_at_close": "abs(日盤收-夜盤收) < abs(日盤開-夜盤收)",
        "anchor_position": "隔日開盤相對昨日日盤收與昨日夜盤收的位置",
        "multiple_testing": "所有統計檢定一起使用Benjamini-Hochberg FDR校正",
        "ohlc_limit": "日K可衡量最大向夜收與背離幅度，但無法判斷兩者發生先後；加入停損需改用分鐘K",
    }
    paths[6].write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="隔日日盤開盤向昨日夜盤收方向收束回測")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--minimum-sample-size", type=int, default=30)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path("backtest_output/futures_open_to_night_close_convergence"),
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
    night_gap = build_night_gap_summary(detail)
    anchor_position = build_anchor_position_summary(
        detail, settings.minimum_sample_size
    )
    gap_matrix = build_gap_matrix(detail, settings.minimum_sample_size)
    yearly = build_yearly(detail, settings.minimum_sample_size)
    tests = build_statistical_tests(detail, settings)
    paths = export_reports(
        detail, night_gap, anchor_position, gap_matrix, yearly, tests,
        settings, args.output_dir,
    )
    print(f"有效交易日：{len(detail):,}")
    print(f"夜收距離摘要：{len(night_gap):,}")
    print(f"開盤錨點位置摘要：{len(anchor_position):,}")
    print(f"雙距離矩陣：{len(gap_matrix):,}")
    print(f"年度摘要：{len(yearly):,}")
    print(f"統計檢定：{len(tests):,}")
    if len(tests):
        print(f"FDR顯著：{int(tests['FDR顯著'].sum()):,}")
    print("\n輸出：")
    for path in paths:
        print(path.resolve())


if __name__ == "__main__":
    main()
