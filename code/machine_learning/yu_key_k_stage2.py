"""Yu 關鍵 K 第二階段：將 Yu 歷史點位反向配對至當時可見的加權 K。

這一階段只建立可稽核的標註資料，不直接訓練模型。每個分析日只能使用該日
以及更早的加權／TX 資料；沒有足夠證據的配對會保留為低可信或未配對，且
未被 Yu 提及的 K 只標為「未標註」，不直接當成負例。
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from machine_learning import yu_key_k_stage1 as stage1
from machine_learning import yu_strategy_model as v1


POINT_SPECS = (
    ("進場", "進場點", "進場依據", "進場附註"),
    ("停利1", "停利1_點位", "停利1_依據", "停利1_附註"),
    ("停利2", "停利2_點位", "停利2_依據", "停利2_附註"),
    ("停損1", "停損1_點位", "停損1_依據", "停損1_附註"),
    ("停損2", "停損2_點位", "停損2_依據", "停損2_附註"),
)

DATE_PATTERN = re.compile(
    r"(?<!\d)(?:(?P<year>20\d{2})[年/\-.])?"
    r"(?P<month>1[0-2]|0?[1-9])[月/\-.]"
    r"(?P<day>3[01]|[12]\d|0?[1-9])(?:日)?(?!\d)"
)


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    required = {"date_column", "taiex", "futures", "features", "scoring", "output", "matching"}
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError(f"關鍵K第二階段設定缺少欄位：{missing}")
    return config


def load_yu_labels(path: str | Path, sheet_name: str | int = 0) -> pd.DataFrame:
    frame = pd.read_excel(path, sheet_name=sheet_name)
    required = {"分析日期", *[field for spec in POINT_SPECS for field in spec[1:3]]}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Yu歷史分析表缺少欄位：{missing}")
    frame["分析日期"] = pd.to_datetime(frame["分析日期"], errors="coerce").dt.normalize()
    return frame.dropna(subset=["分析日期"]).reset_index(drop=True)


def _text(value: Any) -> str:
    return "" if pd.isna(value) else str(value).strip()


def extract_reference_dates(text: str, analysis_date: pd.Timestamp) -> list[pd.Timestamp]:
    """解析 1/13、2026-1-13、1月13日；不得回傳分析日以後日期。"""
    values: list[pd.Timestamp] = []
    for match in DATE_PATTERN.finditer(text):
        year = int(match.group("year") or analysis_date.year)
        month, day = int(match.group("month")), int(match.group("day"))
        try:
            candidate = pd.Timestamp(year=year, month=month, day=day)
        except ValueError:
            continue
        if not match.group("year") and candidate > analysis_date:
            candidate = candidate.replace(year=candidate.year - 1)
        if candidate <= analysis_date and candidate not in values:
            values.append(candidate)
    if any(word in text for word in ("今日", "今天", "當日")) and analysis_date not in values:
        values.append(analysis_date)
    return values


def infer_expected_kinds(text: str) -> list[str]:
    kinds = []
    rules = (
        ("高點", ("高點", "最高", "高檔")),
        ("低點", ("低點", "最低", "低檔")),
        ("開盤", ("開盤", "開 ", "開價")),
        ("收盤", ("收盤", "收 ", "收價")),
        ("實K中關", ("實K中關", "實體中關")),
        ("全K中關", ("中關", "高低中值")),
    )
    for kind, words in rules:
        if any(word in text for word in words):
            kinds.append(kind)
    return list(dict.fromkeys(kinds))


def normalize_yu_points(labels: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    scoped = labels.loc[labels["分析日期"].between(start, end)].copy()
    for source_row, row in scoped.iterrows():
        analysis_date = row["分析日期"]
        for point_role, point_col, basis_col, note_col in POINT_SPECS:
            point = pd.to_numeric(pd.Series([row.get(point_col)]), errors="coerce").iloc[0]
            if pd.isna(point):
                continue
            basis, note = _text(row.get(basis_col)), _text(row.get(note_col))
            evidence = "；".join(x for x in (basis, note) if x)
            refs = extract_reference_dates(evidence, analysis_date)
            rows.append({
                "Yu標註ID": f"{analysis_date:%Y%m%d}-{int(source_row):04d}-{point_role}",
                "來源列": int(source_row) + 2,
                "分析日期": analysis_date,
                "點位角色": point_role,
                "Yu點位": float(point),
                "Yu依據": basis,
                "Yu附註": note,
                "明示參考日期": "|".join(f"{x:%Y-%m-%d}" for x in refs),
                "明示點位類型": "|".join(infer_expected_kinds(evidence)),
                "含明示日期": int(bool(refs)),
            })
    return pd.DataFrame(rows)


def _map_all_dates_to_tx(snapshot: pd.DataFrame, futures: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """沿用第一階段映射規則，但保留當時所有候選K，而非只取前20根。"""
    keys = snapshot.copy().reset_index(drop=True)
    keys.insert(0, "關鍵K排名", np.arange(1, len(keys) + 1))
    return stage1.map_key_k_to_tx(keys, futures, config)


def score_point_matches(
    point: pd.Series,
    levels: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    settings = config["matching"]
    analysis_date = pd.Timestamp(point["分析日期"]).normalize()
    earliest = analysis_date - pd.Timedelta(days=int(settings["candidate_calendar_days"]))
    candidates = levels.loc[
        (levels["加權關鍵K日期"] <= analysis_date)
        & (levels["加權關鍵K日期"] >= earliest)
    ].copy()
    if candidates.empty:
        return candidates
    refs = {
        pd.Timestamp(x) for x in str(point["明示參考日期"]).split("|") if x and x != "nan"
    }
    expected = {x for x in str(point["明示點位類型"]).split("|") if x and x != "nan"}
    candidates["點位誤差"] = (candidates["TX關鍵點位"] - float(point["Yu點位"])).abs()
    candidates["日期明示命中"] = candidates["加權關鍵K日期"].isin(refs).astype(int)
    candidates["類型文字命中"] = candidates["點位類型"].isin(expected).astype(int)
    age = (analysis_date - candidates["加權關鍵K日期"]).dt.days.clip(lower=0)
    tolerance = float(settings["price_tolerance_points"])
    candidates["價格相似分數"] = np.exp(-candidates["點位誤差"] / tolerance)
    candidates["日期接近分數"] = np.exp(-age / float(settings["age_decay_calendar_days"]))
    candidates["配對分數"] = (
        candidates["價格相似分數"] * float(settings["weights"]["price"])
        + candidates["日期明示命中"] * float(settings["weights"]["explicit_date"])
        + candidates["類型文字命中"] * float(settings["weights"]["kind"])
        + candidates["日期接近分數"] * float(settings["weights"]["recency"])
    )
    candidates["Yu標註ID"] = point["Yu標註ID"]
    candidates["分析日期"] = analysis_date
    candidates["點位角色_Yu"] = point["點位角色"]
    candidates["Yu點位"] = point["Yu點位"]
    candidates["Yu依據"] = point["Yu依據"]
    candidates["明示參考日期"] = point["明示參考日期"]
    candidates["明示點位類型"] = point["明示點位類型"]
    candidates = candidates.sort_values(
        ["配對分數", "點位誤差", "關鍵K形成重要度"], ascending=[False, True, False]
    ).head(int(settings["top_matches_per_point"])).reset_index(drop=True)
    candidates.insert(candidates.columns.get_loc("Yu標註ID") + 1, "候選排名", np.arange(1, len(candidates) + 1))
    return candidates


def classify_matches(matches: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    if matches.empty:
        return matches
    settings = config["matching"]
    exact = float(settings["exact_tolerance_points"])
    acceptable = float(settings["price_tolerance_points"])
    matches = matches.copy()
    matches["配對可信度"] = np.select(
        [
            (matches["日期明示命中"] == 1) & (matches["點位誤差"] <= exact),
            (matches["日期明示命中"] == 1) & (matches["點位誤差"] <= acceptable),
            (matches["點位誤差"] <= exact) & (matches["類型文字命中"] == 1),
            matches["點位誤差"] <= acceptable,
        ],
        ["高", "中", "中", "低"],
        default="不採用",
    )
    return matches


def build_key_k_labels(points: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    """每個Yu點位只採排名1且中/高可信配對；其他K維持未標註。"""
    if matches.empty:
        return pd.DataFrame()
    chosen = matches.loc[
        (matches["候選排名"] == 1) & matches["配對可信度"].isin(["高", "中"])
    ].copy()
    if chosen.empty:
        return pd.DataFrame()
    grouped = chosen.groupby(["分析日期", "加權關鍵K日期"], as_index=False).agg(
        Yu命中點位數=("Yu標註ID", "nunique"),
        Yu標註ID=("Yu標註ID", lambda x: "|".join(map(str, x))),
        Yu點位角色=("點位角色_Yu", lambda x: "|".join(dict.fromkeys(map(str, x)))),
        最佳配對可信度=("配對可信度", lambda x: "高" if "高" in set(x) else "中"),
        最小點位誤差=("點位誤差", "min"),
        最大配對分數=("配對分數", "max"),
    )
    grouped["監督標籤"] = 1
    grouped["標籤說明"] = "Yu歷史分析明示或可高可信反推的關鍵K正例"
    return grouped.sort_values(["分析日期", "加權關鍵K日期"]).reset_index(drop=True)


def build_summary(points: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    top = matches.loc[matches["候選排名"] == 1].copy() if not matches.empty else matches
    rows = [
        ("Yu歷史點位數", len(points)),
        ("含明示參考日期點位數", int(points["含明示日期"].sum()) if not points.empty else 0),
        ("有候選配對點位數", int(top["Yu標註ID"].nunique()) if not top.empty else 0),
    ]
    for confidence in ("高", "中", "低", "不採用"):
        count = int((top["配對可信度"] == confidence).sum()) if not top.empty else 0
        rows.append((f"最佳配對_{confidence}", count))
    accepted = top.loc[top["配對可信度"].isin(["高", "中"])] if not top.empty else top
    rows.extend([
        ("可作正例點位數", len(accepted)),
        ("可作正例唯一K數", int(accepted["加權關鍵K日期"].nunique()) if not accepted.empty else 0),
        ("可作正例點位平均誤差", round(float(accepted["點位誤差"].mean()), 3) if not accepted.empty else np.nan),
    ])
    return pd.DataFrame(rows, columns=["指標", "數值"])


def compare_history(
    labels: pd.DataFrame,
    taiex: pd.DataFrame,
    futures: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    date_column = config["date_column"]
    points = normalize_yu_points(labels, start, end)
    if points.empty:
        raise RuntimeError("指定期間沒有可供配對的Yu點位")
    all_matches = []
    for analysis_date, daily_points in points.groupby("分析日期", sort=True):
        # 防止呼叫端傳入未來資料：每個歷史分析日都重新裁切。
        taiex_cut = taiex.loc[pd.to_datetime(taiex[date_column]) <= analysis_date].copy()
        futures_cut = futures.loc[pd.to_datetime(futures[date_column]) <= analysis_date].copy()
        formation = stage1.build_formation_features(taiex_cut, analysis_date, config)
        snapshot = stage1.add_asof_validity(formation, analysis_date, config)
        levels = _map_all_dates_to_tx(snapshot, futures_cut, config)
        for _, point in daily_points.iterrows():
            matched = score_point_matches(point, levels, config)
            if not matched.empty:
                all_matches.append(matched)
    matches = pd.concat(all_matches, ignore_index=True) if all_matches else pd.DataFrame()
    matches = classify_matches(matches, config)
    key_labels = build_key_k_labels(points, matches)
    summary = build_summary(points, matches)
    return points, matches, key_labels, summary


def load_reports_for_period(config: dict[str, Any], start: pd.Timestamp, end: pd.Timestamp):
    history_days = int(config["features"]["load_calendar_days"])
    load_start = (start - pd.Timedelta(days=history_days)).date()
    date_column = config["date_column"]
    taiex_fields = stage1._unique([date_column, *config["taiex"]["fields"]])
    futures_fields = stage1._unique([
        date_column, *config["futures"].get("entity_filter", {}).keys(),
        *[item["field"] for item in config["futures"]["mapped_fields"]],
    ])
    taiex = stage1._load_relation(
        config["taiex"]["relation"], taiex_fields, date_column, load_start, end.date()
    )
    futures = stage1._load_relation(
        config["futures"]["relation"], futures_fields, date_column, load_start, end.date(),
        config["futures"].get("entity_filter"),
    )
    return taiex, futures


def run_compare(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    start, end = pd.Timestamp(args.start).normalize(), pd.Timestamp(args.end).normalize()
    if start > end:
        raise ValueError("start 不可晚於 end")
    labels = load_yu_labels(args.labels, args.sheet)
    taiex, futures = load_reports_for_period(config, start, end)
    points, matches, key_labels, summary = compare_history(
        labels, taiex, futures, start, end, config
    )
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    stamp = f"{start:%Y%m%d}_{end:%Y%m%d}"
    v1._write_csv(points, output / f"yu_key_k_yu_points_{stamp}.csv")
    v1._write_csv(matches, output / f"yu_key_k_matches_{stamp}.csv")
    v1._write_csv(key_labels, output / f"yu_key_k_positive_labels_{stamp}.csv")
    v1._write_csv(summary, output / f"yu_key_k_match_summary_{stamp}.csv")
    print(summary.to_string(index=False))
    print(f"輸出位置：{output.resolve()}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Yu 關鍵K第二階段歷史比對")
    subparsers = parser.add_subparsers(dest="command", required=True)
    compare = subparsers.add_parser("compare", help="將Yu歷史點位反向配對至加權關鍵K")
    compare.add_argument("--config", required=True)
    compare.add_argument("--labels", required=True)
    compare.add_argument("--sheet", default=0)
    compare.add_argument("--start", required=True)
    compare.add_argument("--end", required=True)
    compare.add_argument("--output-dir", default="../output/machine_learningyu_key_k_stage2")
    compare.set_defaults(func=run_compare)
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
