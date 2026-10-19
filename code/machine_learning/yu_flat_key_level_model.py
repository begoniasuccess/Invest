"""Yu 扁平化關鍵點位資料集與同尺度候選回測。

目標刻意保持單純：Yu 截至某日曾列出的有效點位，在對應時間尺度下都視為
重要答案；不區分進場、停利、停損，也不因重複引用增加權重。任何歷史日的
候選與答案都只允許使用當日以前已知的資料。
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from machine_learning import yu_key_k_stage1 as stage1
from machine_learning import yu_key_k_stage2 as stage2
from machine_learning import yu_strategy_model as v1


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    required = {"date_column", "taiex", "futures", "features", "scoring", "output", "flat_labels"}
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError(f"Yu扁平點位設定缺少欄位：{missing}")
    return config


def classify_time_scale(evidence: str, has_reference_date: bool) -> str:
    text = evidence.upper().replace(" ", "")
    for minutes in (60, 30, 15, 5, 1):
        if re.search(rf"{minutes}(?:分K|分鐘K|MIN)", text):
            return f"{minutes}分K"
    if any(word in text for word in ("盤中", "DIF", "日盤轉折", "夜盤轉折", "轉折點")):
        return "盤中動態"
    if any(word in text for word in ("MA", "均線", "日均", "乖離")) and not has_reference_date:
        return "動態指標"
    daily_words = ("開盤", "收盤", "高點", "低點", "最高", "最低", "中關", "長紅", "長黑", "大量K", "帶量")
    if has_reference_date or any(word in text for word in daily_words):
        return "日K"
    return "未分類"


def flatten_yu_labels(
    labels: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    config: dict[str, Any],
) -> pd.DataFrame:
    points = stage2.normalize_yu_points(labels, start, end).copy()
    if points.empty:
        return points
    points["時間尺度"] = points.apply(
        lambda row: classify_time_scale(
            "；".join(x for x in (str(row.get("Yu依據", "")), str(row.get("Yu附註", ""))) if x != "nan"),
            bool(row["含明示日期"]),
        ), axis=1,
    )
    # 重複引用不增加權重。容許極小人工抄錄差，依設定將點位量化後去重。
    unit = float(config["flat_labels"]["deduplicate_points"])
    points["點位去重鍵"] = (points["Yu點位"] / unit).round() * unit
    grouped = points.groupby(["時間尺度", "點位去重鍵"], as_index=False, dropna=False).agg(
        Yu點位=("Yu點位", "median"),
        首次標記日期=("分析日期", "min"),
        最後標記日期=("分析日期", "max"),
        原始引用次數=("Yu標註ID", "nunique"),
        原始角色=("點位角色", lambda x: "|".join(dict.fromkeys(map(str, x)))),
        明示參考日期=("明示參考日期", lambda x: "|".join(dict.fromkeys(y for y in map(str, x) if y and y != "nan"))),
        Yu依據範例=("Yu依據", lambda x: next((str(y) for y in x if pd.notna(y) and str(y).strip()), "")),
    )
    grouped.insert(0, "Yu扁平點位ID", [f"YL-{i:04d}" for i in range(1, len(grouped) + 1)])
    grouped["監督標籤"] = 1
    grouped["標籤定義"] = "截至首次標記日期後，Yu曾列為重要；不區分交易用途與重要程度"
    return grouped.sort_values(["首次標記日期", "時間尺度", "Yu點位"]).reset_index(drop=True)


def _deduplicate_candidate_levels(levels: pd.DataFrame, unit: float) -> pd.DataFrame:
    if levels.empty:
        return levels
    work = levels.copy()
    work["候選點位去重鍵"] = (work["TX關鍵點位"] / unit).round() * unit
    work = work.sort_values(["關鍵K排名", "關鍵K目前有效度"], ascending=[True, False])
    work = work.drop_duplicates("候選點位去重鍵", keep="first").reset_index(drop=True)
    work.insert(0, "候選點位排名", np.arange(1, len(work) + 1))
    return work


def label_daily_candidates(
    analysis_date: pd.Timestamp,
    candidates: pd.DataFrame,
    flat_labels: pd.DataFrame,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    tolerance = float(config["flat_labels"]["hit_tolerance_points"])
    known = flat_labels.loc[
        (flat_labels["時間尺度"] == "日K")
        & (flat_labels["首次標記日期"] <= analysis_date)
    ].copy()
    result = candidates.copy()
    result["分析日期"] = analysis_date
    result["候選時間尺度"] = "日K"
    if result.empty:
        return result, _metric_row(analysis_date, result, known, tolerance)
    if known.empty:
        result["最近Yu扁平點位ID"] = ""
        result["最近Yu點位"] = np.nan
        result["距最近Yu點位"] = np.nan
        result["監督標籤"] = 0
        return result, _metric_row(analysis_date, result, known, tolerance)
    values = known["Yu點位"].to_numpy(dtype=float)
    ids = known["Yu扁平點位ID"].to_numpy()
    candidate_values = result["TX關鍵點位"].to_numpy(dtype=float)
    distances = np.abs(candidate_values[:, None] - values[None, :])
    nearest = distances.argmin(axis=1)
    result["最近Yu扁平點位ID"] = ids[nearest]
    result["最近Yu點位"] = values[nearest]
    result["距最近Yu點位"] = distances[np.arange(len(result)), nearest]
    result["監督標籤"] = (result["距最近Yu點位"] <= tolerance).astype(int)
    return result, _metric_row(analysis_date, result, known, tolerance)


def _metric_row(
    analysis_date: pd.Timestamp,
    candidates: pd.DataFrame,
    known: pd.DataFrame,
    tolerance: float,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "分析日期": analysis_date,
        "當時已知Yu日K點位數": len(known),
        "候選點位數": len(candidates),
        "命中候選數": int(candidates.get("監督標籤", pd.Series(dtype=int)).sum()),
    }
    for top_n in (5, 10, 20, 50):
        top = candidates.head(top_n)
        hits = int(top.get("監督標籤", pd.Series(dtype=int)).sum())
        row[f"前{top_n}命中數"] = hits
        row[f"前{top_n}精準率_pct"] = round(hits / len(top) * 100, 2) if len(top) else np.nan
    if known.empty or candidates.empty:
        row["Yu點位被找回數"] = 0
        row["Yu點位召回率_pct"] = np.nan if known.empty else 0.0
        row["Yu點位最近候選平均誤差"] = np.nan
    else:
        distances = np.abs(
            known["Yu點位"].to_numpy(dtype=float)[:, None]
            - candidates["TX關鍵點位"].to_numpy(dtype=float)[None, :]
        ).min(axis=1)
        row["Yu點位被找回數"] = int((distances <= tolerance).sum())
        row["Yu點位召回率_pct"] = round(float((distances <= tolerance).mean() * 100), 2)
        row["Yu點位最近候選平均誤差"] = round(float(distances.mean()), 3)
    return row


def build_flat_dataset(
    labels: pd.DataFrame,
    taiex: pd.DataFrame,
    futures: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    date_column = config["date_column"]
    flat = flatten_yu_labels(labels, start, end, config)
    analysis_dates = sorted(pd.to_datetime(labels["分析日期"].dropna().unique()))
    analysis_dates = [pd.Timestamp(x).normalize() for x in analysis_dates if start <= pd.Timestamp(x) <= end]
    datasets, metrics = [], []
    for analysis_date in analysis_dates:
        # 核心防洩漏：即使呼叫端傳入未來列，每個歷史日都在此重新裁切。
        taiex_cut = taiex.loc[pd.to_datetime(taiex[date_column]) <= analysis_date].copy()
        futures_cut = futures.loc[pd.to_datetime(futures[date_column]) <= analysis_date].copy()
        _, key_k, levels = stage1.analyze_as_of(taiex_cut, futures_cut, analysis_date, config)
        candidates = _deduplicate_candidate_levels(
            levels, float(config["flat_labels"]["candidate_deduplicate_points"])
        )
        labeled, metric = label_daily_candidates(analysis_date, candidates, flat, config)
        if not labeled.empty:
            datasets.append(labeled)
        metrics.append(metric)
    dataset = pd.concat(datasets, ignore_index=True) if datasets else pd.DataFrame()
    daily_metrics = pd.DataFrame(metrics)
    summary = summarize(flat, dataset, daily_metrics)
    return flat, dataset, daily_metrics, summary


def summarize(flat: pd.DataFrame, dataset: pd.DataFrame, metrics: pd.DataFrame) -> pd.DataFrame:
    rows = [
        ("Yu扁平點位總數", len(flat)),
        ("Yu日K扁平點位數", int((flat["時間尺度"] == "日K").sum()) if not flat.empty else 0),
        ("分析交易日數", len(metrics)),
        ("候選訓練列數", len(dataset)),
        ("正例候選列數", int(dataset["監督標籤"].sum()) if not dataset.empty else 0),
    ]
    if not metrics.empty:
        for top_n in (5, 10, 20, 50):
            rows.append((f"平均前{top_n}精準率_pct", round(float(metrics[f"前{top_n}精準率_pct"].mean()), 2)))
        rows.extend([
            ("平均Yu點位召回率_pct", round(float(metrics["Yu點位召回率_pct"].mean()), 2)),
            ("平均Yu點位最近候選誤差", round(float(metrics["Yu點位最近候選平均誤差"].mean()), 3)),
        ])
    return pd.DataFrame(rows, columns=["指標", "數值"])


def load_reports(config: dict[str, Any], start: pd.Timestamp, end: pd.Timestamp):
    return stage2.load_reports_for_period(config, start, end)


def run_build(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    start, end = pd.Timestamp(args.start).normalize(), pd.Timestamp(args.end).normalize()
    if start > end:
        raise ValueError("start 不可晚於 end")
    labels = stage2.load_yu_labels(args.labels, args.sheet)
    taiex, futures = load_reports(config, start, end)
    flat, dataset, metrics, summary = build_flat_dataset(labels, taiex, futures, start, end, config)
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    stamp = f"{start:%Y%m%d}_{end:%Y%m%d}"
    v1._write_csv(flat, output / f"yu_flat_levels_{stamp}.csv")
    v1._write_csv(dataset, output / f"yu_flat_candidate_training_{stamp}.csv")
    v1._write_csv(metrics, output / f"yu_flat_daily_metrics_{stamp}.csv")
    v1._write_csv(summary, output / f"yu_flat_summary_{stamp}.csv")
    print(summary.to_string(index=False))
    print(f"輸出位置：{output.resolve()}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Yu扁平化關鍵點位資料集")
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build", help="建立扁平答案、候選訓練列與命中報表")
    build.add_argument("--config", required=True)
    build.add_argument("--labels", required=True)
    build.add_argument("--sheet", default=0)
    build.add_argument("--start", required=True)
    build.add_argument("--end", required=True)
    build.add_argument("--output-dir", default="../output/machine_learningyu_flat_key_level")
    build.set_defaults(func=run_build)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        args.func(args)
        return 0
    except Exception as exc:
        print(f"[錯誤] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
