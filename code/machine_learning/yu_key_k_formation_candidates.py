"""Yu 關鍵日K修正版候選器：只看K棒形成當時可知的資訊。

本程式刻意不使用距目前價格、K棒年齡、形成後突破／觸碰或目前有效度。
它提供兩個命令：
1. ``candidates``：截至指定日，依形成特徵列出候選K及同日TX映射點位。
2. ``build-labels``：將Yu歷史分析中明示引用的日K日期，整理成一列一K資料集。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from machine_learning import yu_flat_key_level_model as flat_model
from machine_learning import yu_key_k_stage1 as stage1
from machine_learning import yu_key_k_stage2 as stage2
from machine_learning import yu_strategy_model as v1


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    required = {"date_column", "taiex", "futures", "features", "scoring", "formation_candidates"}
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError(f"形成候選器設定缺少欄位：{missing}")
    return config


def explain_formation(row: pd.Series, config: dict[str, Any]) -> str:
    """用連續分數產生可讀原因，避免有分數卻全部顯示一般K。"""
    settings = config["formation_candidates"]
    threshold = float(settings["reason_score_threshold"])
    reasons: list[str] = []
    if row.get("大量K分數", 0) >= threshold:
        reasons.append("成交量顯著")
    if row.get("波段高分數", 0) >= threshold:
        reasons.append("截至當日波段高")
    if row.get("波段低分數", 0) >= threshold:
        reasons.append("截至當日波段低")
    if row.get("長實體分數", 0) >= threshold:
        direction = "長紅" if row.get("實體點數", 0) > 0 else "長黑" if row.get("實體點數", 0) < 0 else "長實體"
        reasons.append(direction)
    if row.get("長上影分數", 0) >= threshold:
        reasons.append("長上影")
    if row.get("長下影分數", 0) >= threshold:
        reasons.append("長下影")
    if not reasons:
        components = {
            "成交量偏高": row.get("大量K分數", 0),
            "接近波段高": row.get("波段高分數", 0),
            "接近波段低": row.get("波段低分數", 0),
            "實體偏大": row.get("長實體分數", 0),
            "上影偏長": row.get("長上影分數", 0),
            "下影偏長": row.get("長下影分數", 0),
        }
        reasons = [max(components, key=components.get)]
    return "＋".join(reasons)


def select_formation_candidates(
    formation: pd.DataFrame,
    cutoff: pd.Timestamp,
    config: dict[str, Any],
) -> pd.DataFrame:
    date_column = config["date_column"]
    settings = config["formation_candidates"]
    frame = formation.loc[pd.to_datetime(formation[date_column]) <= cutoff].copy()
    frame = frame.loc[frame["形成重要度"] >= float(settings["minimum_score"])].copy()
    frame["候選原因"] = frame.apply(lambda row: explain_formation(row, config), axis=1)
    frame = frame.sort_values(["形成重要度", date_column], ascending=[False, False])
    limit = settings.get("top_k")
    if limit is not None:
        frame = frame.head(int(limit))
    frame = frame.reset_index(drop=True)
    frame.insert(0, "形成候選排名", np.arange(1, len(frame) + 1))
    frame["資料截止日"] = cutoff
    frame["候選分數定義"] = "僅使用K棒形成當日及更早資料；未使用後續行情與目前有效度"
    return frame


def map_candidates_to_tx(
    candidates: pd.DataFrame,
    futures: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    if candidates.empty:
        return pd.DataFrame()
    keys = candidates.rename(columns={"形成候選排名": "關鍵K排名"}).copy()
    keys["目前有效度"] = keys["形成重要度"]
    keys["目前狀態"] = "不評估"
    keys["形成原因"] = keys["候選原因"]
    levels = stage1.map_key_k_to_tx(keys, futures, config)
    if levels.empty:
        return levels
    return levels.rename(columns={
        "關鍵K排名": "形成候選排名",
        "關鍵K目前有效度": "關鍵K形成分數",
        "關鍵K目前狀態": "後續狀態（本階段不使用）",
    })


def build_candidates_as_of(
    taiex: pd.DataFrame,
    futures: pd.DataFrame,
    cutoff: pd.Timestamp,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    date_column = config["date_column"]
    cutoff = pd.Timestamp(cutoff).normalize()
    taiex_cut = taiex.loc[pd.to_datetime(taiex[date_column]) <= cutoff].copy()
    futures_cut = futures.loc[pd.to_datetime(futures[date_column]) <= cutoff].copy()
    formation = stage1.build_formation_features(taiex_cut, cutoff, config)
    candidates = select_formation_candidates(formation, cutoff, config)
    levels = map_candidates_to_tx(candidates, futures_cut, config)
    return formation, candidates, levels


def extract_yu_daily_k_references(
    labels: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    """只取Yu在日K尺度文字中明示的來源日期；不使用價格猜日期。"""
    points = stage2.normalize_yu_points(labels, start, end)
    rows: list[dict[str, Any]] = []
    for _, point in points.iterrows():
        evidence = "；".join(
            str(x) for x in (point.get("Yu依據"), point.get("Yu附註"))
            if pd.notna(x) and str(x).strip()
        )
        scale = flat_model.classify_time_scale(evidence, bool(point["含明示日期"]))
        if scale != "日K" or not point["明示參考日期"]:
            continue
        for raw_date in str(point["明示參考日期"]).split("|"):
            source_date = pd.to_datetime(raw_date, errors="coerce")
            if pd.isna(source_date) or source_date > point["分析日期"]:
                continue
            rows.append({
                "來源K日期": source_date.normalize(),
                "首次標記日期": point["分析日期"],
                "Yu點位": point["Yu點位"],
                "Yu點位角色": point["點位角色"],
                "Yu原始依據": point["Yu依據"],
                "Yu標註ID": point["Yu標註ID"],
            })
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    return result.sort_values(["來源K日期", "首次標記日期"]).reset_index(drop=True)


def build_k_training_table(
    formation: pd.DataFrame,
    references: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    config: dict[str, Any],
) -> pd.DataFrame:
    date_column = config["date_column"]
    data = formation.loc[pd.to_datetime(formation[date_column]).between(start, end)].copy()
    if data.empty:
        return data
    if references.empty:
        data["Yu曾標記關鍵K"] = 0
        data["Yu首次標記日期"] = pd.NaT
        data["Yu引用次數"] = 0
        return data
    ref = references.groupby("來源K日期", as_index=False).agg(
        Yu首次標記日期=("首次標記日期", "min"),
        Yu最後標記日期=("首次標記日期", "max"),
        Yu引用次數=("Yu標註ID", "nunique"),
        Yu點位數=("Yu點位", "nunique"),
        Yu點位範例=("Yu點位", lambda x: "|".join(map(lambda v: f"{float(v):g}", list(dict.fromkeys(x))[:8]))),
        Yu依據範例=("Yu原始依據", lambda x: next((str(v) for v in x if pd.notna(v) and str(v).strip()), "")),
    )
    data = data.merge(ref, left_on=date_column, right_on="來源K日期", how="left")
    data["Yu曾標記關鍵K"] = data["來源K日期"].notna().astype(int)
    data["Yu引用次數"] = data["Yu引用次數"].fillna(0).astype(int)
    data["Yu點位數"] = data["Yu點位數"].fillna(0).astype(int)
    data["特徵可用截止日"] = data[date_column]
    data["標籤備註"] = np.where(
        data["Yu曾標記關鍵K"].eq(1),
        "正例：Yu於來源K形成日或之後曾明示引用此日期",
        "暫定負例：指定Yu歷史資料中未明示引用此日期",
    )
    return data.sort_values(date_column).reset_index(drop=True)


def summarize_training(data: pd.DataFrame, references: pd.DataFrame) -> pd.DataFrame:
    positives = data.loc[data["Yu曾標記關鍵K"].eq(1)] if not data.empty else data
    rows = [
        ("K棒樣本數", len(data)),
        ("Yu正例K棒數", len(positives)),
        ("暫定負例K棒數", len(data) - len(positives)),
        ("正例比例_pct", round(len(positives) / len(data) * 100, 2) if len(data) else np.nan),
        ("Yu日K明示引用列數", len(references)),
        ("未落入交易日樣本的引用日期數", int(len(set(references.get("來源K日期", [])) - set(data.get("日期", []))))),
    ]
    return pd.DataFrame(rows, columns=["指標", "數值"])


def load_reports(config: dict[str, Any], start: pd.Timestamp, end: pd.Timestamp):
    return stage2.load_reports_for_period(config, start, end)


def run_candidates(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    cutoff = pd.Timestamp(args.date).normalize()
    history_days = int(config["features"]["load_calendar_days"])
    start = cutoff - pd.Timedelta(days=history_days)
    taiex, futures = load_reports(config, start, cutoff)
    formation, candidates, levels = build_candidates_as_of(taiex, futures, cutoff, config)
    output = Path(args.output_dir); output.mkdir(parents=True, exist_ok=True)
    stamp = f"{cutoff:%Y%m%d}"
    v1._write_csv(formation, output / f"yu_formation_features_{stamp}.csv")
    v1._write_csv(candidates, output / f"yu_formation_candidates_{stamp}.csv")
    v1._write_csv(levels, output / f"yu_formation_candidate_levels_{stamp}.csv")
    print(candidates[["形成候選排名", config["date_column"], "候選原因", "形成重要度"]].head(30).to_string(index=False))
    print(f"輸出位置：{output.resolve()}")


def run_build_labels(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    start, end = pd.Timestamp(args.start).normalize(), pd.Timestamp(args.end).normalize()
    if start > end:
        raise ValueError("start 不可晚於 end")
    labels = stage2.load_yu_labels(args.labels, args.sheet)
    taiex, _ = load_reports(config, start, end)
    formation = stage1.build_formation_features(taiex, end, config)
    references = extract_yu_daily_k_references(labels, start, end)
    training = build_k_training_table(formation, references, start, end, config)
    summary = summarize_training(training, references)
    output = Path(args.output_dir); output.mkdir(parents=True, exist_ok=True)
    stamp = f"{start:%Y%m%d}_{end:%Y%m%d}"
    v1._write_csv(references, output / f"yu_daily_k_references_{stamp}.csv")
    v1._write_csv(training, output / f"yu_k_formation_training_{stamp}.csv")
    v1._write_csv(summary, output / f"yu_k_formation_summary_{stamp}.csv")
    print(summary.to_string(index=False)); print(f"輸出位置：{output.resolve()}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Yu關鍵日K形成候選器")
    sub = parser.add_subparsers(dest="command", required=True)
    candidates = sub.add_parser("candidates")
    candidates.add_argument("--config", required=True); candidates.add_argument("--date", required=True)
    candidates.add_argument("--output-dir", default="../output/machine_learningyu_key_k_formation")
    candidates.set_defaults(func=run_candidates)
    labels = sub.add_parser("build-labels")
    labels.add_argument("--config", required=True); labels.add_argument("--labels", required=True)
    labels.add_argument("--sheet", default=0); labels.add_argument("--start", required=True); labels.add_argument("--end", required=True)
    labels.add_argument("--output-dir", default="../output/machine_learningyu_key_k_formation")
    labels.set_defaults(func=run_build_labels)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv); args.func(args); return 0
    except Exception as exc:
        print(f"[錯誤] {exc}"); return 1


if __name__ == "__main__":
    raise SystemExit(main())
