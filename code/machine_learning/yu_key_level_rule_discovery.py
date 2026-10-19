"""反推 Yu 的加權關鍵 K／點位規則，並驗證加權到 TX 的映射方式。

分析指令（從 code 目錄執行）：

    python -m machine_learning.yu_key_level_rule_discovery analyze `
      --config machine_learning/configs/yu_key_level_rule_discovery.json `
      --taiex-labels yu_taiex_analysis_2025.csv yu_taiex_analysis_2026.csv `
      --futures-labels yu_futures_analysis_2026.xlsx `
      --start 2025-11-03 --end 2026-08-31

即時候選指令：

    python -m machine_learning.yu_key_level_rule_discovery predict `
      --config machine_learning/configs/yu_key_level_rule_discovery.json `
      --date 2026-09-08 `
      --thresholds ../output/machine_learningyu_key_level_rule_discovery/yu_rule_thresholds_20251103_20260831.csv

所有行情特徵只使用該根 K 形成日及更早資料；歷史配對也會逐分析日裁切，
不允許截止日後行情參與計算。Yu 未引用的 K 只稱「未標記」，不當作已證實負例。
"""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from machine_learning import yu_key_k_stage1 as stage1
from machine_learning import yu_key_k_stage2 as stage2
from machine_learning import yu_strategy_model as v1


NUMBER_RE = re.compile(r"(?<![\d/])([1-9]\d{3,5}(?:\.\d+)?)(?![\d/])")
# 斜線若左右都是數字就是日期（11/05），不可拿來切句。
CLAUSE_SEPARATOR_RE = re.compile(r"(?<!\d)/|/(?!\d)|[|｜；;\n]+")
K_WORDS = ("開盤", "收盤", "高點", "低點", "最高", "最低", "中關", "中值", "實K", "K棒", "大量", "長紅", "長黑", "上影", "下影", "波段", "前高", "前低", "跳空", "缺口")
EXCLUDE_ONLY_WORDS = ("均線", "日均", "MA", "選擇權", "履約價")


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    required = {"date_column", "taiex", "futures", "features", "scoring", "discovery", "mapping"}
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError(f"規則反推設定缺少欄位：{missing}")
    return config


def _text(value: Any) -> str:
    return "" if pd.isna(value) else re.sub(r"\s+", " ", str(value)).strip()


def _read_table(path: str | Path, sheet: str | int = 0) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, encoding="utf-8-sig")
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet)
    raise ValueError(f"不支援的標記檔格式：{path}")


def _read_taiex_label_table(path: str | Path) -> pd.DataFrame:
    """讀取舊版分析檔或已整理好的 Yu 點位活頁簿。"""
    path = Path(path)
    if path.suffix.lower() in {".xlsx", ".xls"}:
        with pd.ExcelFile(path) as workbook:
            if "加權點位整理" in workbook.sheet_names:
                return pd.read_excel(workbook, sheet_name="加權點位整理")
    return _read_table(path)


def _normalize_cleaned_taiex_labels(
    frame: pd.DataFrame, path: str | Path, start: pd.Timestamp, end: pd.Timestamp,
) -> pd.DataFrame:
    """將「Yu點位資料整理」直接轉成候選覆蓋驗證所需格式。"""
    required = {"分析日期", "點位角色", "點位", "點位分類", "點位類型"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{path} 的加權點位整理缺少欄位：{missing}")

    cleaned = frame.copy()
    cleaned["分析日期"] = pd.to_datetime(cleaned["分析日期"], errors="coerce").dt.normalize()
    cleaned["點位"] = pd.to_numeric(cleaned["點位"], errors="coerce")
    cleaned = cleaned.loc[
        cleaned["分析日期"].between(start, end) & cleaned["點位"].notna()
    ].copy()

    def value(row: pd.Series, column: str, default: str = "") -> str:
        item = row.get(column, default)
        return default if pd.isna(item) else str(item).strip()

    rows: list[dict[str, Any]] = []
    for position, (_, row) in enumerate(cleaned.iterrows(), start=1):
        analysis_date = pd.Timestamp(row["分析日期"]).normalize()
        category = value(row, "點位分類")
        point_id = value(row, "Yu點位ID") or value(row, "整理後點位ID")
        if not point_id:
            point_id = f"TAIEX-{analysis_date:%Y%m%d}-{position:05d}"
        rows.append({
            "Yu點位ID": point_id,
            "分析日期": analysis_date,
            "點位角色": value(row, "點位角色"),
            "Yu加權點位": float(row["點位"]),
            "Yu原始依據": value(row, "Yu原始依據"),
            "Yu點位子句": value(row, "Yu點位子句"),
            "子句序號": position,
            "子句解析狀態": value(row, "子句解析狀態", "已整理"),
            "當日描述規則": value(row, "當日描述規則"),
            "來源檔": value(row, "來源檔", Path(path).name),
            "來源欄位": value(row, "來源欄位"),
            "明示參考日期": value(row, "明示參考日期"),
            "明示點位類型": value(row, "點位類型", "未明示"),
            "規則文字標籤": value(row, "規則文字標籤"),
            "是否K棒型點位": int(category == "K棒固定點位"),
            "已整理點位分類": category,
            "資料處理狀態": value(row, "資料處理狀態"),
        })
    return pd.DataFrame(rows)


def _infer_level_kind(text: str) -> str:
    rules = (
        ("缺口上緣", ("缺口上緣",)), ("缺口下緣", ("缺口下緣",)),
        ("實K中關", ("實K中關", "實體中關")), ("全K中關", ("全K中關", "高低中值", "中關", "中值")),
        ("高點", ("高點", "最高")), ("低點", ("低點", "最低")),
        ("開盤", ("開盤", "開價")), ("收盤", ("收盤", "收價")),
    )
    found = []
    for priority, (kind, words) in enumerate(rules):
        for word in words:
            position = text.rfind(word)
            if position >= 0:
                found.append((position, -priority, kind))
    return max(found)[2] if found else "未明示"


RULE_NAMES = ("大量K", "跳空缺口", "波段高", "波段低", "長上影", "長下影", "長紅黑K")
REFERENCE_PARTS = ("開盤", "高點", "低點", "收盤", "實K中關", "全K中關", "缺口上緣", "缺口下緣")


def _infer_rule_tags(text: str) -> str:
    rules = (
        ("大量K", ("大量", "帶量", "爆量", "量K")),
        ("跳空缺口", ("跳空", "缺口")),
        ("波段高", ("波段高", "前高", "趨勢高", "頸線壓力")),
        ("波段低", ("波段低", "前低", "趨勢低", "頸線支撐")),
        ("長上影", ("長上影", "上長影")), ("長下影", ("長下影", "下長影")),
        ("長紅黑K", ("長紅", "紅K", "實紅", "長黑", "黑K", "實黑")),
    )
    return "|".join(name for name, words in rules if any(word in text for word in words))


def _point_token_pattern(point: float) -> re.Pattern[str]:
    token = f"{point:g}"
    return re.compile(rf"(?<!\d){re.escape(token)}(?:\.0+)?(?!\d)")


def _clause_for_point(reason: str, point: float, allow_structured_point: bool = False) -> tuple[str, str, int]:
    """把共用理由拆成子句，只讓同一子句的規則修飾目標點位。"""
    text = _text(reason)
    clauses = [_text(x) for x in CLAUSE_SEPARATOR_RE.split(text) if _text(x)]
    if not clauses:
        return text, "無法切分", 0
    target_index = next((i for i, clause in enumerate(clauses) if _point_token_pattern(point).search(clause)), None)
    if target_index is None:
        if allow_structured_point:
            return text, "點位由結構欄提供", 1
        return text, "找不到點位所在子句", 0
    clause = clauses[target_index]
    inherited: list[str] = []
    # 「10/30高點28525/開盤28350」後半句可繼承緊鄰前句的日期；
    # 「大量K高點/開盤」亦可繼承規則，但有自己規則文字的子句絕不繼承。
    refs = stage2.extract_reference_dates(clause, pd.Timestamp("2099-12-31"))
    tags = _infer_rule_tags(clause)
    if target_index > 0:
        previous = clauses[target_index - 1]
        if not refs:
            date_tokens = re.findall(r"(?<!\d)(\d{1,2}/\d{1,2})(?!\d)", previous)
            if date_tokens:
                inherited.append(date_tokens[-1])
        if not refs and not tags and _infer_level_kind(clause) != "未明示":
            previous_tags = _infer_rule_tags(previous)
            if previous_tags:
                inherited.append(previous_tags)
    enriched = " ".join([*inherited, clause]).strip()
    return enriched, "已定位子句", target_index + 1


def _daily_rule_context(row: pd.Series) -> tuple[str, str]:
    preferred = (
        "今日盤勢狀況", "價量描述", "線型描述", "盤型描述",
        "分析結論", "明日壓力原文", "明日支撐原文",
    )
    text = " | ".join(_text(row.get(col)) for col in preferred if _text(row.get(col)))
    return text, _infer_rule_tags(text)


def _is_k_based(text: str, kind: str, refs: list[pd.Timestamp]) -> bool:
    last_k = max((text.rfind(word) for word in K_WORDS), default=-1)
    last_excluded = max((text.rfind(word) for word in EXCLUDE_ONLY_WORDS), default=-1)
    if last_excluded > last_k:
        return False
    if any(word in text for word in K_WORDS) or refs or kind != "未明示":
        return True
    return False


def _extract_text_points(text: str) -> list[tuple[float, str, int, str]]:
    """從舊版自由文字抽點位；reason 保留該點附近片段供日期／類型判斷。"""
    text = _text(text)
    if not text:
        return []
    rows: list[tuple[float, str, int, str]] = []
    for match in NUMBER_RE.finditer(text):
            value = float(match.group(1))
            if not 5_000 <= value <= 100_000:
                continue
            clause, status, sequence = _clause_for_point(text, value)
            rows.append((value, clause, sequence, status))
    return rows


def normalize_taiex_labels(paths: list[str | Path], start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for path in paths:
        frame = _read_taiex_label_table(path)
        if "分析日期" not in frame:
            raise ValueError(f"{path} 缺少分析日期")
        if {"點位", "點位分類", "點位類型"}.issubset(frame.columns):
            cleaned = _normalize_cleaned_taiex_labels(frame, path, start, end)
            rows.extend(cleaned.to_dict("records"))
            continue
        frame["分析日期"] = pd.to_datetime(frame["分析日期"], errors="coerce").dt.normalize()
        frame = frame.loc[frame["分析日期"].between(start, end)]
        structured = any(col.startswith(("壓力1_點位", "支撐1_點位")) for col in frame.columns)
        for source_row, row in frame.iterrows():
            analysis_date = row["分析日期"]
            daily_context, daily_tags = _daily_rule_context(row)
            items: list[tuple[str, float, str, str, str, int, str]] = []
            if structured:
                for role, max_n in (("壓力", 12), ("支撐", 12)):
                    for index in range(1, max_n + 1):
                        point_col, reason_col = f"{role}{index}_點位", f"{role}{index}_依據"
                        if point_col not in frame:
                            continue
                        point = pd.to_numeric(pd.Series([row.get(point_col)]), errors="coerce").iloc[0]
                        if pd.notna(point):
                            full_reason = _text(row.get(reason_col))
                            reason, parse_status, sequence = _clause_for_point(full_reason, float(point), allow_structured_point=True)
                            items.append((role, float(point), reason, full_reason, point_col, sequence, parse_status))
            else:
                for col, role in (("盤後壓力", "壓力"), ("盤後支撐", "支撐"), ("盤前壓力", "壓力"), ("盤前支撐", "支撐")):
                    full_reason = _text(row.get(col, ""))
                    for point, reason, sequence, parse_status in _extract_text_points(full_reason):
                        items.append((role, point, reason, full_reason, col, sequence, parse_status))
            for role, point, reason, full_reason, source_col, sequence, parse_status in items:
                refs = stage2.extract_reference_dates(reason, analysis_date)
                kind = _infer_level_kind(reason)
                point_tags = _infer_rule_tags(reason)
                same_day = analysis_date in refs
                tags = "|".join(dict.fromkeys(
                    tag for source in (point_tags, daily_tags if same_day else "")
                    for tag in source.split("|") if tag
                ))
                key = (analysis_date, role, round(point, 6), reason)
                if key in seen:
                    continue
                seen.add(key)
                rows.append({
                    "Yu點位ID": f"TAIEX-{analysis_date:%Y%m%d}-{len(rows)+1:05d}",
                    "分析日期": analysis_date, "點位角色": role, "Yu加權點位": point,
                    "Yu原始依據": full_reason, "Yu點位子句": reason, "子句序號": sequence,
                    "子句解析狀態": parse_status, "當日描述規則": daily_tags,
                    "來源檔": Path(path).name, "來源欄位": source_col,
                    "明示參考日期": "|".join(f"{x:%Y-%m-%d}" for x in refs),
                    "明示點位類型": kind, "規則文字標籤": tags,
                    "是否K棒型點位": int(_is_k_based(reason, kind, refs)),
                })
    result = pd.DataFrame(rows)
    return result.sort_values(["分析日期", "點位角色", "Yu加權點位"]).reset_index(drop=True) if not result.empty else result


def load_market_data(config: dict[str, Any], start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame]:
    history_days = int(config["features"]["load_calendar_days"])
    load_start = (start - pd.Timedelta(days=history_days)).date()
    date_column = config["date_column"]
    taiex_fields = stage1._unique([date_column, *config["taiex"]["fields"]])
    futures_fields = stage1._unique([
        date_column, *config["futures"].get("entity_filter", {}).keys(),
        *config["futures"]["fields"],
    ])
    taiex = stage1._load_relation(config["taiex"]["relation"], taiex_fields, date_column, load_start, end.date())
    futures = stage1._load_relation(
        config["futures"]["relation"], futures_fields, date_column, load_start, end.date(),
        config["futures"].get("entity_filter"),
    )
    return taiex, futures


def build_taiex_candidate_levels(taiex: pd.DataFrame, cutoff: pd.Timestamp, config: dict[str, Any]) -> pd.DataFrame:
    """建立截止日可見的開高低收、中關及缺口邊緣。"""
    date_col = config["date_column"]; cmap = config["taiex"]["column_map"]
    frame = taiex.loc[pd.to_datetime(taiex[date_col]) <= cutoff].copy().sort_values(date_col)
    for col in cmap.values():
        if col in frame:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    rows = []
    previous = None
    for _, bar in frame.iterrows():
        source_date = pd.Timestamp(bar[date_col]).normalize()
        values = {
            "開盤": bar[cmap["open"]], "高點": bar[cmap["high"]],
            "低點": bar[cmap["low"]], "收盤": bar[cmap["close"]],
        }
        values["實K中關"] = (values["開盤"] + values["收盤"]) / 2
        values["全K中關"] = (values["高點"] + values["低點"]) / 2
        for kind, value in values.items():
            if pd.notna(value):
                rows.append({"來源K日期": source_date, "候選點位類型": kind, "加權候選點位": float(value), "候選來源": "K棒"})
        if previous is not None:
            prev_high, prev_low = previous[cmap["high"]], previous[cmap["low"]]
            if pd.notna(values["低點"]) and pd.notna(prev_high) and values["低點"] > prev_high:
                rows.extend([
                    {"來源K日期": source_date, "候選點位類型": "缺口上緣", "加權候選點位": float(values["低點"]), "候選來源": "向上跳空"},
                    {"來源K日期": source_date, "候選點位類型": "缺口下緣", "加權候選點位": float(prev_high), "候選來源": "向上跳空"},
                ])
            if pd.notna(values["高點"]) and pd.notna(prev_low) and values["高點"] < prev_low:
                rows.extend([
                    {"來源K日期": source_date, "候選點位類型": "缺口上緣", "加權候選點位": float(prev_low), "候選來源": "向下跳空"},
                    {"來源K日期": source_date, "候選點位類型": "缺口下緣", "加權候選點位": float(values["高點"]), "候選來源": "向下跳空"},
                ])
        previous = bar
    return pd.DataFrame(rows)


def match_yu_levels(points: pd.DataFrame, taiex: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    settings = config["discovery"]; results = []
    for analysis_date, daily in points.loc[points["是否K棒型點位"].eq(1)].groupby("分析日期", sort=True):
        # 關鍵防洩漏：候選池逐分析日重建，絕不看 analysis_date 之後行情。
        levels = build_taiex_candidate_levels(taiex, analysis_date, config)
        earliest = analysis_date - pd.Timedelta(days=int(settings["candidate_calendar_days"]))
        levels = levels.loc[levels["來源K日期"].between(earliest, analysis_date)].copy()
        for _, point in daily.iterrows():
            candidates = levels.copy()
            refs = {pd.Timestamp(x) for x in str(point["明示參考日期"]).split("|") if x and x != "nan"}
            expected = str(point["明示點位類型"])
            candidates["點位誤差"] = (candidates["加權候選點位"] - point["Yu加權點位"]).abs()
            candidates["日期明示命中"] = candidates["來源K日期"].isin(refs).astype(int)
            candidates["類型文字命中"] = (candidates["候選點位類型"] == expected).astype(int)
            age = (analysis_date - candidates["來源K日期"]).dt.days.clip(lower=0)
            candidates["配對分數"] = (
                np.exp(-candidates["點位誤差"] / float(settings["price_tolerance_points"])) * float(settings["weights"]["price"])
                + candidates["日期明示命中"] * float(settings["weights"]["explicit_date"])
                + candidates["類型文字命中"] * float(settings["weights"]["kind"])
                + np.exp(-age / float(settings["age_decay_calendar_days"])) * float(settings["weights"]["recency"])
            )
            best = candidates.sort_values(["配對分數", "點位誤差"], ascending=[False, True]).head(1)
            if best.empty:
                continue
            best = best.assign(**{key: point[key] for key in point.index})
            results.append(best)
    matched = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    if matched.empty:
        return matched
    exact = float(settings["exact_tolerance_points"]); acceptable = float(settings["price_tolerance_points"])
    matched["配對可信度"] = np.select([
        (matched["日期明示命中"].eq(1) & matched["點位誤差"].le(exact)),
        (matched["日期明示命中"].eq(1) & matched["點位誤差"].le(acceptable)),
        (matched["類型文字命中"].eq(1) & matched["點位誤差"].le(exact)),
        matched["點位誤差"].le(acceptable),
    ], ["高", "中", "中", "低"], default="不採用")
    matched["是否採用正例"] = matched["配對可信度"].isin(settings["accepted_confidence"]).astype(int)
    return matched.sort_values(["分析日期", "Yu點位ID"]).reset_index(drop=True)


def build_k_training(taiex: pd.DataFrame, matches: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, config: dict[str, Any]) -> pd.DataFrame:
    formation = stage1.build_formation_features(taiex, end, config)
    formation = add_gap_features(formation, config)
    accepted = matches.loc[matches["是否採用正例"].eq(1)].copy() if not matches.empty else matches
    effective_start = min(start, accepted["來源K日期"].min()) if not accepted.empty else start
    formation = formation.loc[formation[config["date_column"]].between(effective_start, end)].copy()
    if accepted.empty:
        formation["Yu曾標記關鍵K"] = 0
        formation["Yu引用點位數"] = 0
        return formation
    labels = accepted.groupby("來源K日期", as_index=False).agg(
        Yu首次引用日=("分析日期", "min"), Yu最後引用日=("分析日期", "max"),
        Yu引用點位數=("Yu點位ID", "nunique"),
        Yu引用部位=("候選點位類型", lambda x: "|".join(dict.fromkeys(map(str, x)))),
        Yu規則文字=("規則文字標籤", lambda x: "|".join(dict.fromkeys(v for item in x for v in str(item).split("|") if v))),
    )
    result = formation.merge(labels, left_on=config["date_column"], right_on="來源K日期", how="left")
    result["Yu曾標記關鍵K"] = result["來源K日期"].notna().astype(int)
    result["Yu引用點位數"] = result["Yu引用點位數"].fillna(0).astype(int)
    rule_text = result["Yu規則文字"].fillna("").astype(str)
    for rule_name in RULE_NAMES:
        result[f"Yu規則_{rule_name}"] = rule_text.str.split("|").apply(lambda values: int(rule_name in values))
    part_sets = accepted.groupby("來源K日期")["候選點位類型"].agg(set)
    mapped_parts = result[config["date_column"]].map(part_sets)
    for part in REFERENCE_PARTS:
        result[f"Yu引用部位_{part}"] = mapped_parts.apply(
            lambda values, expected=part: int(isinstance(values, set) and expected in values)
        )
    # 相容舊報表名稱；這兩欄只代表引用部位，不能再當作波段形成原因。
    result["Yu選中高點"] = result["Yu引用部位_高點"]
    result["Yu選中低點"] = result["Yu引用部位_低點"]
    result["Yu形成原因_波段高K"] = result["Yu規則_波段高"]
    result["Yu形成原因_波段低K"] = result["Yu規則_波段低"]
    result["標籤性質"] = np.where(result["Yu曾標記關鍵K"].eq(1), "正例", "未標記（不是已證實負例）")
    return result.sort_values(config["date_column"]).reset_index(drop=True)


def build_reference_part_summary(training: pd.DataFrame) -> pd.DataFrame:
    """分開統計 Yu 引用哪個 K 棒部位，不把部位誤當成形成原因。"""
    rows: list[dict[str, Any]] = []
    key_k = training.loc[training["Yu曾標記關鍵K"].eq(1)]
    for part in REFERENCE_PARTS:
        column = f"Yu引用部位_{part}"
        if column not in key_k:
            continue
        selected = key_k.loc[key_k[column].eq(1)]
        rows.append({"引用部位": part, "被引用K棒數": len(selected),
                     "占全部關鍵K比例": len(selected) / len(key_k) if len(key_k) else np.nan,
                     "平均每根Yu引用點位數": selected["Yu引用點位數"].mean() if len(selected) else np.nan})
    return pd.DataFrame(rows).sort_values("被引用K棒數", ascending=False).reset_index(drop=True)


def add_gap_features(formation: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """用當日與前一交易日資料定義真正未重疊跳空，不使用未來回補結果。"""
    frame = formation.copy(); cmap = config["taiex"]["column_map"]
    high = pd.to_numeric(frame[cmap["high"]], errors="coerce")
    low = pd.to_numeric(frame[cmap["low"]], errors="coerce")
    close = pd.to_numeric(frame[cmap["close"]], errors="coerce")
    raw_gap_col = config["taiex"].get("gap_column", "跳空缺口")
    view_gap = pd.to_numeric(frame.get(raw_gap_col, pd.Series(np.nan, index=frame.index)), errors="coerce")
    upward = (low - high.shift(1)).clip(lower=0)
    downward = (low.shift(1) - high).clip(lower=0)
    frame["向上缺口點數"] = upward
    frame["向下缺口點數"] = downward
    frame["跳空缺口絕對點數"] = np.maximum(upward, downward)
    frame["跳空缺口_昨收_pct_重算"] = frame["跳空缺口絕對點數"] / close.shift(1).abs().replace(0, np.nan)
    frame["跳空缺口_ATR倍數"] = frame["跳空缺口絕對點數"] / pd.to_numeric(frame["ATR"], errors="coerce").replace(0, np.nan)
    frame["跳空方向_重算"] = np.select([upward.gt(0), downward.gt(0)], ["向上", "向下"], default="無缺口")
    frame["View開盤跳空點數"] = view_gap
    frame["View開盤跳空絕對點數"] = view_gap.abs()
    frame["View開盤跳空方向"] = np.select([view_gap.gt(0), view_gap.lt(0)], ["向上", "向下"], default="無或缺值")
    frame["View與未重疊缺口方向一致"] = np.where(
        frame["跳空方向_重算"].eq("無缺口"), np.nan,
        frame["View開盤跳空方向"].eq(frame["跳空方向_重算"]).astype(int),
    )
    return frame


def build_rule_label_audit(training: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """輸出每個規則正例及行情證據，讓文字標籤可以逐筆人工檢查。"""
    rows: list[dict[str, Any]] = []
    evidence = {
        "大量K": ["量比_5", "量比_10", "量比_20", "成交量百分位_60"],
        "跳空缺口": ["View開盤跳空點數", "跳空缺口絕對點數", "跳空方向_重算"],
        "波段高": ["是否截至當日5日新高", "是否截至當日20日新高", "是否截至當日60日新高"],
        "波段低": ["是否截至當日5日新低", "是否截至當日20日新低", "是否截至當日60日新低"],
        "長上影": ["上影點數", "上影_ATR倍數", "上影百分位_60"],
        "長下影": ["下影點數", "下影_ATR倍數", "下影百分位_60"],
        "長紅黑K": ["紅黑方向", "實體絕對點數", "實體_ATR倍數", "實體百分位_60"],
    }
    for _, row in training.iterrows():
        active = [rule for rule in RULE_NAMES if int(row.get(f"Yu規則_{rule}", 0) or 0) == 1]
        for rule in dict.fromkeys(active):
            item = {
                "來源K日期": row[config["date_column"]], "規則": rule,
                "標籤來源": "Yu文字明示形成原因",
                "Yu規則文字": row.get("Yu規則文字", ""), "Yu引用點位數": row.get("Yu引用點位數", 0),
            }
            item.update({col: row.get(col) for col in evidence[rule]})
            rows.append(item)
    return pd.DataFrame(rows)


def build_gap_matches(matches: pd.DataFrame, training: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    if matches.empty:
        return pd.DataFrame()
    tagged = matches.loc[
        matches["是否採用正例"].eq(1)
        & matches["規則文字標籤"].fillna("").str.split("|").apply(lambda x: "跳空缺口" in x)
    ].copy()
    if tagged.empty:
        return tagged
    columns = [config["date_column"], "View開盤跳空點數", "View開盤跳空方向",
               "跳空缺口絕對點數", "跳空方向_重算", "View與未重疊缺口方向一致"]
    market = training[[col for col in columns if col in training]].drop_duplicates(config["date_column"])
    result = tagged.merge(market, left_on="來源K日期", right_on=config["date_column"], how="left")
    result["缺口標籤是否由缺口邊緣配對"] = result["候選點位類型"].isin(["缺口上緣", "缺口下緣"]).astype(int)
    result["缺口標籤稽核狀態"] = np.where(
        result["缺口標籤是否由缺口邊緣配對"].eq(1), "通過：Yu點位直接配到缺口邊緣", "待確認：文字提到缺口但點位配到一般K部位"
    )
    return result


def build_gap_definition_candidates(taiex: pd.DataFrame, end: pd.Timestamp, config: dict[str, Any]) -> pd.DataFrame:
    """逐相鄰交易日建立四種缺口邊界，讓資料判斷 Yu 採用哪一種定義。"""
    date_col = config["date_column"]; cmap = config["taiex"]["column_map"]
    frame = taiex.loc[pd.to_datetime(taiex[date_col]) <= end].copy().sort_values(date_col).reset_index(drop=True)
    for col in cmap.values():
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    rows: list[dict[str, Any]] = []
    for i in range(1, len(frame)):
        previous, current = frame.iloc[i - 1], frame.iloc[i]
        values = [previous[cmap[x]] for x in ("open", "high", "low", "close")] + [current[cmap[x]] for x in ("open", "high", "low", "close")]
        if any(pd.isna(value) for value in values):
            continue
        prev_open, prev_high, prev_low, prev_close, cur_open, cur_high, cur_low, cur_close = map(float, values)
        direction = "向上" if cur_open > prev_close else "向下" if cur_open < prev_close else "無"
        definitions = {
            "昨收至今開": (prev_close, cur_open),
            "昨收至今收": (prev_close, cur_close),
            "前後實體邊界": ((max(prev_open, prev_close), min(cur_open, cur_close)) if direction == "向上" else (max(cur_open, cur_close), min(prev_open, prev_close))),
            "前後全K未重疊": ((prev_high, cur_low) if direction == "向上" else (cur_high, prev_low)),
        }
        for definition, (first, second) in definitions.items():
            lower, upper = sorted((first, second)); valid = upper > lower
            if definition == "前後實體邊界":
                valid = valid and ((direction == "向上" and min(cur_open, cur_close) > max(prev_open, prev_close)) or (direction == "向下" and max(cur_open, cur_close) < min(prev_open, prev_close)))
            elif definition == "前後全K未重疊":
                valid = valid and ((direction == "向上" and cur_low > prev_high) or (direction == "向下" and cur_high < prev_low))
            if direction == "無" or not valid:
                continue
            previous_date = pd.Timestamp(previous[date_col]).normalize(); formation_date = pd.Timestamp(current[date_col]).normalize()
            rows.append({"缺口候選ID": f"GAP-{formation_date:%Y%m%d}-{definition}", "缺口前交易日": previous_date,
                         "缺口形成日": formation_date, "缺口方向": direction, "缺口定義": definition,
                         "缺口下緣": lower, "缺口上緣": upper, "缺口寬度": upper - lower,
                         "前日開盤": prev_open, "前日最高": prev_high, "前日最低": prev_low, "前日收盤": prev_close,
                         "形成日開盤": cur_open, "形成日最高": cur_high, "形成日最低": cur_low, "形成日收盤": cur_close})
    return pd.DataFrame(rows)


def compare_gap_definitions(points: pd.DataFrame, candidates: pd.DataFrame, config: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """只用明示日期和上下緣語意比較定義，避免靠最近價格猜事件。"""
    if candidates.empty:
        return pd.DataFrame(), pd.DataFrame()
    tagged = points.loc[points["規則文字標籤"].fillna("").str.split("|").apply(lambda x: "跳空缺口" in x) & points["明示點位類型"].isin(["缺口上緣", "缺口下緣"])]
    rows: list[dict[str, Any]] = []
    for _, point in tagged.iterrows():
        refs = {pd.Timestamp(x) for x in str(point["明示參考日期"]).split("|") if x and x != "nan"}
        if not refs:
            continue
        cutoff = pd.Timestamp(point["分析日期"]).normalize()
        pool = candidates.loc[candidates["缺口形成日"].le(cutoff) & (candidates["缺口形成日"].isin(refs) | candidates["缺口前交易日"].isin(refs))]
        edge_col = str(point["明示點位類型"])
        for definition, group in pool.groupby("缺口定義"):
            ranked = group.copy()
            ranked["定義配對誤差"] = (pd.to_numeric(ranked[edge_col], errors="coerce") - float(point["Yu加權點位"])).abs()
            best = ranked.sort_values(["定義配對誤差", "缺口形成日"]).iloc[0]
            rows.append({**point.to_dict(), **best.to_dict(), "事件邊緣類型": edge_col, "事件邊緣": best[edge_col], "定義配對誤差": best["定義配對誤差"]})
    detail = pd.DataFrame(rows)
    if detail.empty:
        return detail, pd.DataFrame()
    exact = float(config["discovery"]["exact_tolerance_points"]); acceptable = float(config["discovery"]["price_tolerance_points"])
    summary = detail.groupby("缺口定義", as_index=False).agg(可比較Yu點位數=("Yu點位ID", "nunique"), 平均絕對誤差=("定義配對誤差", "mean"), 中位絕對誤差=("定義配對誤差", "median"), P90絕對誤差=("定義配對誤差", lambda x: x.quantile(.9)))
    for tolerance, label in ((exact, "精確命中率"), (acceptable, "容許命中率")):
        rates = detail.assign(hit=detail["定義配對誤差"].le(tolerance)).groupby("缺口定義")["hit"].mean()
        summary[label] = summary["缺口定義"].map(rates)
    summary = summary.sort_values(["精確命中率", "中位絕對誤差"], ascending=[False, True]).reset_index(drop=True)
    summary.insert(0, "缺口定義排名", np.arange(1, len(summary) + 1))
    return detail.sort_values(["分析日期", "Yu點位ID", "缺口定義"]), summary


def build_gap_events(taiex: pd.DataFrame, end: pd.Timestamp, config: dict[str, Any]) -> pd.DataFrame:
    """建立由前一交易日與形成日共同構成的未重疊缺口事件。"""
    date_col = config["date_column"]; cmap = config["taiex"]["column_map"]
    frame = taiex.loc[pd.to_datetime(taiex[date_col]) <= end].copy().sort_values(date_col).reset_index(drop=True)
    for col in cmap.values():
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    raw_gap_col = config["taiex"].get("gap_column", "跳空缺口")
    frame[raw_gap_col] = pd.to_numeric(frame.get(raw_gap_col), errors="coerce")
    rows: list[dict[str, Any]] = []
    for i in range(1, len(frame)):
        previous, current = frame.iloc[i - 1], frame.iloc[i]
        prev_high, prev_low = previous[cmap["high"]], previous[cmap["low"]]
        cur_high, cur_low = current[cmap["high"]], current[cmap["low"]]
        direction = None
        if pd.notna(cur_low) and pd.notna(prev_high) and cur_low > prev_high:
            direction, lower, upper = "向上", float(prev_high), float(cur_low)
        elif pd.notna(cur_high) and pd.notna(prev_low) and cur_high < prev_low:
            direction, lower, upper = "向下", float(cur_high), float(prev_low)
        else:
            continue
        formation_date = pd.Timestamp(current[date_col]).normalize()
        previous_date = pd.Timestamp(previous[date_col]).normalize()
        later = frame.iloc[i + 1:].copy()
        first_fill = pd.NaT
        if direction == "向上":
            filled = later[cmap["low"]].le(lower)
            if filled.any(): first_fill = pd.Timestamp(later.loc[filled, date_col].iloc[0]).normalize()
            extreme = pd.to_numeric(later[cmap["low"]], errors="coerce").min()
            remaining = max(0.0, min(upper, float(extreme)) - lower) if pd.notna(extreme) else upper - lower
        else:
            filled = later[cmap["high"]].ge(upper)
            if filled.any(): first_fill = pd.Timestamp(later.loc[filled, date_col].iloc[0]).normalize()
            extreme = pd.to_numeric(later[cmap["high"]], errors="coerce").max()
            remaining = max(0.0, upper - max(lower, float(extreme))) if pd.notna(extreme) else upper - lower
        rows.append({
            "缺口事件ID": f"GAP-{formation_date:%Y%m%d}-{direction}",
            "缺口前交易日": previous_date, "缺口形成日": formation_date, "缺口方向": direction,
            "缺口下緣": lower, "缺口上緣": upper, "未重疊缺口點數": upper - lower,
            "View開盤跳空點數": current.get(raw_gap_col),
            "前日收盤": previous[cmap["close"]], "形成日開盤": current[cmap["open"]],
            "截至分析終日是否完全回補": int(pd.notna(first_fill)), "首次完全回補日": first_fill,
            "截至分析終日剩餘缺口點數": remaining,
        })
    return pd.DataFrame(rows)


def match_yu_gap_events(points: pd.DataFrame, events: pd.DataFrame, taiex: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """把Yu缺口上下緣配到跨日事件；回補狀態逐分析日計算。"""
    if events.empty:
        return pd.DataFrame()
    date_col = config["date_column"]; cmap = config["taiex"]["column_map"]
    market = taiex.copy().sort_values(date_col)
    tagged = points.loc[points["規則文字標籤"].fillna("").str.split("|").apply(lambda x: "跳空缺口" in x)]
    rows: list[dict[str, Any]] = []
    tolerance = float(config["discovery"]["price_tolerance_points"])
    for _, point in tagged.iterrows():
        cutoff = pd.Timestamp(point["分析日期"]).normalize()
        refs = {pd.Timestamp(x) for x in str(point["明示參考日期"]).split("|") if x and x != "nan"}
        candidates = events.loc[events["缺口形成日"].le(cutoff)].copy()
        if refs:
            candidates = candidates.loc[candidates["缺口前交易日"].isin(refs) | candidates["缺口形成日"].isin(refs)]
        if candidates.empty:
            continue
        expected = str(point["明示點位類型"])
        edge_col = "缺口上緣" if expected == "缺口上緣" else "缺口下緣" if expected == "缺口下緣" else None
        if edge_col:
            candidates["事件邊緣"] = candidates[edge_col]
            candidates["事件邊緣類型"] = expected
        else:
            lower_error = (candidates["缺口下緣"] - float(point["Yu加權點位"])).abs()
            upper_error = (candidates["缺口上緣"] - float(point["Yu加權點位"])).abs()
            choose_upper = upper_error < lower_error
            candidates["事件邊緣"] = np.where(choose_upper, candidates["缺口上緣"], candidates["缺口下緣"])
            candidates["事件邊緣類型"] = np.where(choose_upper, "缺口上緣", "缺口下緣")
        candidates["事件配對誤差"] = (candidates["事件邊緣"] - float(point["Yu加權點位"])).abs()
        best = candidates.sort_values(["事件配對誤差", "缺口形成日"]).iloc[0]
        later = market.loc[(pd.to_datetime(market[date_col]) > best["缺口形成日"]) & (pd.to_datetime(market[date_col]) <= cutoff)]
        if best["缺口方向"] == "向上":
            filled = pd.to_numeric(later[cmap["low"]], errors="coerce").le(best["缺口下緣"])
        else:
            filled = pd.to_numeric(later[cmap["high"]], errors="coerce").ge(best["缺口上緣"])
        first_fill = pd.Timestamp(later.loc[filled, date_col].iloc[0]).normalize() if filled.any() else pd.NaT
        rows.append({**point.to_dict(), **best.to_dict(),
                     "事件配對可信度": "確定" if best["事件配對誤差"] <= tolerance and (refs or edge_col) else "可能" if best["事件配對誤差"] <= tolerance else "不採用",
                     "截至Yu分析日是否完全回補": int(pd.notna(first_fill)), "截至Yu分析日首次完全回補日": first_fill})
    return pd.DataFrame(rows)


def build_pivot_reference_snapshots(taiex: pd.DataFrame, matches: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """對Yu每次引用的高低點，以該次分析日為截止日建立防守／壓力快照。"""
    if matches.empty:
        return pd.DataFrame()
    date_col = config["date_column"]; cmap = config["taiex"]["column_map"]
    market = add_gap_features(stage1.build_formation_features(taiex, pd.to_datetime(taiex[date_col]).max(), config), config)
    market = market.sort_values(date_col).reset_index(drop=True)
    index_by_date = {pd.Timestamp(value).normalize(): i for i, value in enumerate(market[date_col])}
    selected = matches.loc[matches["是否採用正例"].eq(1) & matches["候選點位類型"].isin(["高點", "低點"])].copy()
    rows: list[dict[str, Any]] = []
    touch_ratio = float(config["discovery"].get("pivot_touch_buffer_atr", .15))
    for _, match in selected.iterrows():
        source_date = pd.Timestamp(match["來源K日期"]).normalize(); cutoff = pd.Timestamp(match["分析日期"]).normalize()
        if source_date not in index_by_date or cutoff not in index_by_date:
            continue
        source_i, cutoff_i = index_by_date[source_date], index_by_date[cutoff]
        if cutoff_i < source_i: continue
        source = market.iloc[source_i]; later = market.iloc[source_i + 1:cutoff_i + 1]
        kind = str(match["候選點位類型"]); level = float(match["加權候選點位"])
        atr = float(source["ATR"]) if pd.notna(source["ATR"]) and source["ATR"] else 1.0
        high = pd.to_numeric(later[cmap["high"]], errors="coerce"); low = pd.to_numeric(later[cmap["low"]], errors="coerce")
        close = pd.to_numeric(later[cmap["close"]], errors="coerce"); buffer = atr * touch_ratio
        if kind == "低點":
            reaction = high.max() - level if not high.empty else 0.0
            close_breaks = int(close.lt(level).sum()); recoveries = int((low.lt(level) & close.ge(level)).sum())
            status = "守住" if close_breaks == 0 else "跌破後收回" if close.iloc[-1] >= level else "已跌破"
        else:
            reaction = level - low.min() if not low.empty else 0.0
            close_breaks = int(close.gt(level).sum()); recoveries = int((high.gt(level) & close.le(level)).sum())
            status = "守住" if close_breaks == 0 else "突破後跌回" if close.iloc[-1] <= level else "已突破"
        touches = int(((low <= level + buffer) & (high >= level - buffer)).sum())
        rows.append({
            "Yu點位ID": match["Yu點位ID"], "分析日期": cutoff, "來源K日期": source_date,
            "高低類型": kind, "Yu點位": match["Yu加權點位"], "來源K實際點位": level,
            "形成至引用交易日數": cutoff_i - source_i, "形成後反應點數": reaction,
            "形成後反應_ATR": reaction / atr, "形成後觸碰次數": touches,
            "收盤有效突破次數": close_breaks, "盤中突破後收回次數": recoveries,
            "引用當下狀態": status, "來源K_ATR": atr,
            "來源K量比_10": source.get("量比_10"), "來源K實體_ATR": source.get("實體_ATR倍數"),
            "來源K上影_ATR": source.get("上影_ATR倍數"), "來源K下影_ATR": source.get("下影_ATR倍數"),
            "來源K是否60日新高": source.get("是否截至當日60日新高"),
            "來源K是否60日新低": source.get("是否截至當日60日新低"),
        })
    return pd.DataFrame(rows)


def build_pivot_feature_comparison(snapshots: pd.DataFrame) -> pd.DataFrame:
    if snapshots.empty:
        return pd.DataFrame()
    numeric = ["形成至引用交易日數", "形成後反應點數", "形成後反應_ATR", "形成後觸碰次數",
               "收盤有效突破次數", "盤中突破後收回次數", "來源K量比_10", "來源K實體_ATR",
               "來源K上影_ATR", "來源K下影_ATR", "來源K是否60日新高", "來源K是否60日新低"]
    rows = []
    for kind, group in snapshots.groupby("高低類型"):
        for feature in numeric:
            values = pd.to_numeric(group[feature], errors="coerce").dropna()
            rows.append({"高低類型": kind, "特徵": feature, "引用次數": len(group), "有效樣本": len(values),
                         "平均值": values.mean(), "中位數": values.median(), "P25": values.quantile(.25), "P75": values.quantile(.75)})
    return pd.DataFrame(rows)


def build_rule_selection_summary(
    thresholds: pd.DataFrame,
    gap_event_matches: pd.DataFrame,
    gap_definition_summary: pd.DataFrame | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not thresholds.empty:
        stable = thresholds.loc[thresholds.get("穩定排名", pd.Series(index=thresholds.index, dtype="Int64")).eq(1)]
        for _, row in stable.iterrows():
            rows.append({
                "規則": row["規則"], "可否進入每日候選": "可以",
                "使用欄位": row["欄位"], "比較符號": row["比較符號"], "門檻": row["門檻"],
                "訓練覆蓋率": row["訓練_正例覆蓋率"], "樣本外覆蓋率": row["樣本外_正例覆蓋率"],
                "樣本外表觀精確率": row["樣本外_表觀精確率"], "說明": "通過訓練與樣本外穩定條件",
            })
        represented = set(stable["規則"])
        for rule in sorted(set(thresholds["規則"]) - represented):
            rows.append({"規則": rule, "可否進入每日候選": "暫不採用", "說明": "沒有門檻同時通過訓練與樣本外條件"})
    if gap_definition_summary is not None and not gap_definition_summary.empty:
        best = gap_definition_summary.sort_values("缺口定義排名").iloc[0]
        rows.append({"規則": "跳空缺口", "可否進入每日候選": "事件方式",
                     "樣本外表觀精確率": best["精確命中率"],
                     "說明": f"最佳邊界={best['缺口定義']}；精確命中率={best['精確命中率']:.1%}；可比較{int(best['可比較Yu點位數'])}筆"})
    elif not gap_event_matches.empty:
        unique = gap_event_matches.drop_duplicates("Yu點位ID")
        confirmed = int(unique["事件配對可信度"].eq("確定").sum())
        rows.append({"規則": "跳空缺口", "可否進入每日候選": "事件方式",
                     "樣本外表觀精確率": confirmed / len(unique) if len(unique) else np.nan,
                     "說明": f"跨日缺口事件配對：{confirmed}/{len(unique)}筆確定"})
    return pd.DataFrame(rows)


def _threshold_metrics(selected: pd.Series, target: pd.Series) -> dict[str, float | int]:
    positive_n = int(target.sum()); unlabeled_n = int(target.eq(0).sum())
    pos_selected = int((selected & target.eq(1)).sum())
    unl_selected = int((selected & target.eq(0)).sum())
    coverage = pos_selected / positive_n if positive_n else math.nan
    unl_rate = unl_selected / unlabeled_n if unlabeled_n else math.nan
    lift = coverage / unl_rate if unl_rate else (math.inf if coverage else math.nan)
    return {
        "樣本數": int(len(target)), "正例總數": positive_n, "未標記總數": unlabeled_n,
        "門檻選中數": int(selected.sum()), "正例選中數": pos_selected,
        "正例覆蓋率": coverage, "未標記選取率": unl_rate,
        "相對提升倍數": lift,
        "表觀精確率": pos_selected / int(selected.sum()) if selected.any() else math.nan,
    }


def _apply_threshold(values: pd.Series, direction: str, threshold: float) -> pd.Series:
    valid = values.notna()
    return valid & ((values >= threshold) if direction == ">=" else (values <= threshold))


def discover_thresholds(training: pd.DataFrame, end: pd.Timestamp, config: dict[str, Any]) -> pd.DataFrame:
    """各規則使用自己的文字標籤找門檻，並以較晚期間做樣本外驗證。"""
    settings = config["discovery"]
    date_col = config["date_column"]
    eligible_end = end - pd.Timedelta(days=int(settings.get("label_maturity_calendar_days", 45)))
    eligible = training.loc[pd.to_datetime(training[date_col]) <= eligible_end].copy()
    split = eligible[date_col].quantile(float(settings.get("validation_fraction", .25))) if False else None
    if not eligible.empty:
        unique_dates = pd.Series(pd.to_datetime(eligible[date_col]).sort_values().unique())
        split_index = max(1, min(len(unique_dates) - 1, int(len(unique_dates) * (1 - float(settings.get("validation_fraction", .25))))))
        split = pd.Timestamp(unique_dates.iloc[split_index])
    rows = []
    for rule_name, features in settings["rule_features"].items():
        if rule_name in settings.get("event_rules_excluded_from_k_thresholds", []):
            continue
        target_col = settings.get("rule_target_columns", {}).get(rule_name, f"Yu規則_{rule_name}")
        if target_col not in eligible:
            continue
        train_mask = eligible[date_col].lt(split) if split is not None else pd.Series(False, index=eligible.index)
        valid_mask = ~train_mask
        train_target = eligible.loc[train_mask, target_col].astype(int)
        valid_target = eligible.loc[valid_mask, target_col].astype(int)
        minimum = int(settings.get("minimum_rule_positive_samples", 8))
        status = "可評估" if int(train_target.sum()) >= minimum and int(valid_target.sum()) >= 1 else "樣本不足"
        for feature in features:
            if feature not in eligible or not pd.api.types.is_numeric_dtype(eligible[feature]):
                continue
            values = pd.to_numeric(eligible[feature], errors="coerce")
            train_values = values.loc[train_mask]
            requested = settings.get("feature_directions", {}).get(feature, "both")
            directions = (">=", "<=") if requested == "both" else (requested,)
            for direction in directions:
                for quantile in settings["threshold_quantiles"]:
                    effective_q = float(quantile) if direction == ">=" else 1 - float(quantile)
                    non_null_train = train_values.dropna()
                    if non_null_train.empty:
                        continue
                    threshold = float(non_null_train.quantile(effective_q))
                    train_selected = _apply_threshold(train_values, direction, threshold)
                    validation_values = values.loc[valid_mask]
                    validation_selected = _apply_threshold(validation_values, direction, threshold)
                    # 排除 >=0 之類全選門檻；它沒有分類能力。
                    if int(train_selected.sum()) in {0, int(train_values.notna().sum())}:
                        continue
                    train_metrics = _threshold_metrics(train_selected, train_target)
                    validation_metrics = _threshold_metrics(validation_selected, valid_target)
                    coverage = float(train_metrics["正例覆蓋率"])
                    lift = float(train_metrics["相對提升倍數"])
                    score = coverage * math.log1p(min(lift, 100)) if np.isfinite(coverage) and np.isfinite(lift) else math.nan
                    rows.append({
                        "規則": rule_name, "欄位": feature, "比較符號": direction, "門檻": threshold,
                        "規則答案欄位": target_col, "評估狀態": status,
                        "門檻訓練起日": eligible.loc[train_mask, date_col].min(), "門檻訓練迄日": eligible.loc[train_mask, date_col].max(),
                        "樣本外起日": eligible.loc[valid_mask, date_col].min(), "樣本外迄日": eligible.loc[valid_mask, date_col].max(),
                        **{f"訓練_{k}": v for k, v in train_metrics.items()},
                        **{f"樣本外_{k}": v for k, v in validation_metrics.items()},
                        "門檻分數": score, "分析截止日": end, "標籤成熟截止日": eligible_end,
                        "未標記說明": "未被Yu明確寫成此規則，不等於真正負例",
                    })
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result = result.drop_duplicates(["規則", "欄位", "比較符號", "門檻"]).reset_index(drop=True)
    reliable = result["評估狀態"].eq("可評估")
    result["規則內排名"] = pd.Series(pd.NA, index=result.index, dtype="Int64")
    result.loc[reliable, "規則內排名"] = result.loc[reliable].groupby("規則")["門檻分數"].rank(method="first", ascending=False).astype("Int64")
    stable = (
        reliable
        & result["訓練_相對提升倍數"].gt(float(settings.get("stable_min_lift", 1.0)))
        & result["樣本外_相對提升倍數"].gt(float(settings.get("stable_min_lift", 1.0)))
        & result["樣本外_正例總數"].ge(int(settings.get("stable_min_validation_positives", 3)))
        & result["訓練_正例覆蓋率"].ge(float(settings.get("stable_min_coverage", .2)))
        & result["樣本外_正例覆蓋率"].ge(float(settings.get("stable_min_coverage", .2)))
    )
    train_lift = result["訓練_相對提升倍數"].clip(upper=20)
    valid_lift = result["樣本外_相對提升倍數"].replace(np.inf, 20).clip(upper=20)
    result["穩定分數"] = np.where(
        stable,
        np.sqrt(train_lift * valid_lift) * np.minimum(result["訓練_正例覆蓋率"], result["樣本外_正例覆蓋率"]),
        np.nan,
    )
    result["是否通過穩定門檻"] = stable.astype(int)
    result["穩定排名"] = pd.Series(pd.NA, index=result.index, dtype="Int64")
    result.loc[stable, "穩定排名"] = result.loc[stable].groupby("規則")["穩定分數"].rank(method="first", ascending=False).astype("Int64")
    return result.sort_values(["規則", "規則內排名"]).reset_index(drop=True)


def normalize_futures_points(path: str | Path, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    frame = _read_table(path)
    frame["分析日期"] = pd.to_datetime(frame["分析日期"], errors="coerce").dt.normalize()
    frame = frame.loc[frame["分析日期"].between(start, end)]
    rows = []
    specs = (("進場", "進場點", "進場依據"), ("停利1", "停利1_點位", "停利1_依據"),
             ("停利2", "停利2_點位", "停利2_依據"), ("停損1", "停損1_點位", "停損1_依據"),
             ("停損2", "停損2_點位", "停損2_依據"))
    for source_row, row in frame.iterrows():
        for role, col, reason_col in specs:
            point = pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0]
            if pd.notna(point) and point > 0:
                reason = _text(row.get(reason_col))
                refs = stage2.extract_reference_dates(reason, row["分析日期"])
                if not refs and any(word in reason for word in ("今日", "當日", "本日")):
                    refs = [row["分析日期"]]
                rows.append({"期貨點位ID": f"TX-{row['分析日期']:%Y%m%d}-{source_row:04d}-{role}",
                             "分析日期": row["分析日期"], "期貨點位角色": role,
                             "Yu期貨點位": float(point), "期貨原始依據": reason,
                             "明示參考日期": "|".join(f"{x:%Y-%m-%d}" for x in refs),
                             "明示點位類型": _infer_level_kind(reason),
                             "規則文字標籤": _infer_rule_tags(reason)})
    return pd.DataFrame(rows)


def _market_index(frame: pd.DataFrame, date_col: str) -> pd.DataFrame:
    return frame.drop_duplicates(date_col, keep="last").set_index(date_col)


def compare_mapping_methods(taiex_points: pd.DataFrame, futures_points: pd.DataFrame, taiex: pd.DataFrame, futures: pd.DataFrame, config: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    accepted = taiex_points.loc[taiex_points["是否採用正例"].eq(1)].copy()
    date_col = config["date_column"]; tc = config["taiex"]["column_map"]
    futures_maps = config["futures"].get("mapping_column_maps", {"日盤": config["futures"]["column_map"]})
    ti, fi = _market_index(taiex, date_col), _market_index(futures, date_col)
    rows = []
    for analysis_date, tx_daily in futures_points.groupby("分析日期"):
        source_levels = accepted.loc[accepted["分析日期"].eq(analysis_date)]
        if source_levels.empty or analysis_date not in ti.index or analysis_date not in fi.index:
            continue
        taiex_close = float(ti.loc[analysis_date, tc["close"]])
        for _, tx_point in tx_daily.iterrows():
            refs = {pd.Timestamp(x) for x in str(tx_point["明示參考日期"]).split("|") if x and x != "nan"}
            expected_kind = str(tx_point["明示點位類型"])
            aligned = source_levels.copy()
            if refs:
                aligned = aligned.loc[aligned["來源K日期"].isin(refs)]
            else:
                aligned = aligned.iloc[0:0]
            # 「今日高／低／收」只是直接讀取期貨行情，不能拿來證明加權映射公式。
            aligned = aligned.loc[aligned["來源K日期"].lt(analysis_date)]
            if expected_kind != "未明示":
                aligned = aligned.loc[aligned["候選點位類型"].eq(expected_kind)]
            else:
                aligned = aligned.iloc[0:0]
            for _, level in aligned.iterrows():
                source_date = level["來源K日期"]
                predictions = {"直接沿用加權點位": level["加權候選點位"]}
                if source_date in ti.index and source_date in fi.index:
                    kind_key = {"開盤": "open", "高點": "high", "低點": "low", "收盤": "close"}.get(level["候選點位類型"])
                    for session, fc in futures_maps.items():
                        analysis_tx_close = float(fi.loc[analysis_date, fc["close"]])
                        analysis_basis = analysis_tx_close - taiex_close
                        analysis_ratio = analysis_tx_close / taiex_close if taiex_close else np.nan
                        predictions[f"分析日收盤基差_{session}"] = level["加權候選點位"] + analysis_basis
                        predictions[f"分析日收盤比例_{session}"] = level["加權候選點位"] * analysis_ratio
                        source_basis = float(fi.loc[source_date, fc["close"]]) - float(ti.loc[source_date, tc["close"]])
                        predictions[f"來源K日收盤基差_{session}"] = level["加權候選點位"] + source_basis
                        if kind_key:
                            predictions[f"來源K同部位TX_{session}"] = float(fi.loc[source_date, fc[kind_key]])
                for method, predicted in predictions.items():
                    if pd.isna(predicted):
                        continue
                    rows.append({
                        **tx_point.to_dict(), "Yu加權點位ID": level["Yu點位ID"],
                        "來源K日期": source_date, "加權點位類型": level["候選點位類型"],
                        "Yu加權點位": level["Yu加權點位"], "映射方法": method,
                        "語意對齊": 1,
                        "映射期貨點位": float(predicted),
                        "映射誤差": float(predicted) - float(tx_point["Yu期貨點位"]),
                        "映射絕對誤差": abs(float(predicted) - float(tx_point["Yu期貨點位"])),
                    })
    candidates = pd.DataFrame(rows)
    if candidates.empty:
        return candidates, pd.DataFrame()
    # 只有日期和K棒部位語意均一致後，才允許在重複文字點位中去重。
    best = candidates.loc[candidates.groupby(["期貨點位ID", "映射方法"])["映射絕對誤差"].idxmin()].copy()
    summary = best.groupby("映射方法", as_index=False).agg(
        可比較期貨點位數=("期貨點位ID", "nunique"),
        平均絕對誤差=("映射絕對誤差", "mean"), 中位絕對誤差=("映射絕對誤差", "median"),
        P90絕對誤差=("映射絕對誤差", lambda x: x.quantile(.9)), 平均有號誤差=("映射誤差", "mean"),
    )
    for tolerance in config["mapping"]["hit_tolerances"]:
        hit = best.assign(hit=best["映射絕對誤差"].le(float(tolerance))).groupby("映射方法")["hit"].mean()
        summary[f"誤差{int(tolerance)}點內比例"] = summary["映射方法"].map(hit)
    summary = summary.sort_values(["中位絕對誤差", "平均絕對誤差"]).reset_index(drop=True)
    summary.insert(0, "映射排名", np.arange(1, len(summary) + 1))
    return best.sort_values(["分析日期", "期貨點位ID", "映射方法"]), summary


def build_rule_combination(training: pd.DataFrame, thresholds: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    rank_col = "穩定排名" if "穩定排名" in thresholds else "規則內排名"
    best = thresholds.loc[thresholds[rank_col].eq(1)].copy()
    maturity_end = pd.to_datetime(thresholds["標籤成熟截止日"], errors="coerce").max() if not thresholds.empty else pd.NaT
    validation_start = pd.to_datetime(best["樣本外起日"], errors="coerce").min() if not best.empty else pd.NaT
    data = training.copy()
    if pd.notna(maturity_end):
        data = data.loc[pd.to_datetime(data[config["date_column"]]) <= maturity_end]
    if pd.notna(validation_start):
        data = data.loc[pd.to_datetime(data[config["date_column"]]) >= validation_start]
    hit_cols = []
    for _, rule in best.iterrows():
        col = f"命中_{rule['規則']}"; values = pd.to_numeric(data[rule["欄位"]], errors="coerce")
        data[col] = (values >= rule["門檻"]).astype(int) if rule["比較符號"] == ">=" else (values <= rule["門檻"]).astype(int)
        hit_cols.append(col)
    data["命中規則數"] = data[hit_cols].sum(axis=1) if hit_cols else 0
    rows = []
    for minimum in range(1, len(hit_cols) + 1):
        selected = data["命中規則數"].ge(minimum); target = data["Yu曾標記關鍵K"].eq(1)
        rows.append({"資料集": "樣本外", "可用規則數": len(hit_cols), "至少命中規則數": minimum, "選中K數": int(selected.sum()),
                     "Yu正例選中數": int((selected & target).sum()),
                     "正例覆蓋率": (selected & target).sum() / target.sum() if target.sum() else np.nan,
                     "表觀精確率": (selected & target).sum() / selected.sum() if selected.sum() else np.nan})
    return pd.DataFrame(rows)


def map_prediction_levels_to_tx(
    levels: pd.DataFrame,
    taiex: pd.DataFrame,
    futures: pd.DataFrame,
    cutoff: pd.Timestamp,
    method: str,
    config: dict[str, Any],
) -> pd.DataFrame:
    """依分析階段勝出的單一公式映射；所有日期均先限制在 cutoff 以前。"""
    date_col = config["date_column"]; tc = config["taiex"]["column_map"]
    taiex = taiex.loc[pd.to_datetime(taiex[date_col]) <= cutoff].copy()
    futures = futures.loc[pd.to_datetime(futures[date_col]) <= cutoff].copy()
    ti, fi = _market_index(taiex, date_col), _market_index(futures, date_col)
    if cutoff not in ti.index or cutoff not in fi.index:
        raise ValueError(f"{cutoff.date()} 缺少加權或TX日報，無法映射")
    maps = config["futures"].get("mapping_column_maps", {"日盤": config["futures"]["column_map"]})
    result = levels.copy(); mapped = []
    for _, level in result.iterrows():
        source_date = pd.Timestamp(level["來源K日期"]).normalize(); taiex_level = float(level["加權候選點位"])
        value = np.nan
        if method == "直接沿用加權點位":
            value = taiex_level
        else:
            matched = re.fullmatch(r"(分析日收盤基差|分析日收盤比例|來源K日收盤基差|來源K同部位TX)_(.+)", method)
            if not matched or matched.group(2) not in maps:
                raise ValueError(f"不支援的映射方法：{method}")
            formula, session = matched.groups(); fc = maps[session]
            if formula == "分析日收盤基差":
                value = taiex_level + float(fi.loc[cutoff, fc["close"]]) - float(ti.loc[cutoff, tc["close"]])
            elif formula == "分析日收盤比例":
                value = taiex_level * float(fi.loc[cutoff, fc["close"]]) / float(ti.loc[cutoff, tc["close"]])
            elif source_date in ti.index and source_date in fi.index:
                if formula == "來源K日收盤基差":
                    value = taiex_level + float(fi.loc[source_date, fc["close"]]) - float(ti.loc[source_date, tc["close"]])
                else:
                    kind = {"開盤": "open", "高點": "high", "低點": "low", "收盤": "close"}.get(level["候選點位類型"])
                    if kind:
                        value = float(fi.loc[source_date, fc[kind]])
        mapped.append(value)
    result["映射方法"] = method
    result["TX候選點位"] = mapped
    result["資料截止日"] = cutoff
    return result


def run_analyze(args: argparse.Namespace) -> None:
    config = load_config(args.config); start = pd.Timestamp(args.start).normalize(); end = pd.Timestamp(args.end).normalize()
    if start > end:
        raise ValueError("start 不可晚於 end")
    points = normalize_taiex_labels(args.taiex_labels, start, end)
    taiex, futures = load_market_data(config, start, end)
    matches = match_yu_levels(points, taiex, config)
    training = build_k_training(taiex, matches, start, end, config)
    reference_part_summary = build_reference_part_summary(training)
    rule_audit = build_rule_label_audit(training, config)
    gap_matches = build_gap_matches(matches, training, config)
    gap_events = build_gap_events(taiex, end, config)
    gap_event_matches = match_yu_gap_events(points, gap_events, taiex, config)
    gap_definition_candidates = build_gap_definition_candidates(taiex, end, config)
    gap_definition_matches, gap_definition_summary = compare_gap_definitions(points, gap_definition_candidates, config)
    pivot_snapshots = build_pivot_reference_snapshots(taiex, matches, config)
    pivot_comparison = build_pivot_feature_comparison(pivot_snapshots)
    parse_rejected = points.loc[
        points["是否K棒型點位"].eq(1)
        & (~points["子句解析狀態"].isin(["已定位子句", "點位由結構欄提供"])
           | points["明示參考日期"].fillna("").eq("")
           | points["明示點位類型"].eq("未明示"))
    ].copy()
    if not parse_rejected.empty:
        parse_rejected["不採用原因"] = parse_rejected.apply(
            lambda row: "；".join([
                *([str(row["子句解析狀態"])] if row["子句解析狀態"] not in {"已定位子句", "點位由結構欄提供"} else []),
                *(["缺少明示參考日期"] if not _text(row["明示參考日期"]) else []),
                *(["缺少明示K棒部位"] if row["明示點位類型"] == "未明示" else []),
            ]), axis=1,
        )
    thresholds = discover_thresholds(training, end, config)
    stable_thresholds = thresholds.loc[thresholds.get("是否通過穩定門檻", pd.Series(index=thresholds.index, dtype=int)).eq(1)].copy()
    combinations = build_rule_combination(training, thresholds, config)
    rule_selection = build_rule_selection_summary(thresholds, gap_event_matches, gap_definition_summary)
    manual_rows = []
    for _, row in parse_rejected.iterrows():
        manual_rows.append({"資料類型": "點位解析", "分析日期": row["分析日期"], "Yu點位": row["Yu加權點位"],
                            "原文": row["Yu原始依據"], "待確認原因": row["不採用原因"]})
    if not gap_event_matches.empty:
        for _, row in gap_event_matches.loc[gap_event_matches["事件配對可信度"].ne("確定")].iterrows():
            manual_rows.append({"資料類型": "缺口事件", "分析日期": row["分析日期"], "Yu點位": row["Yu加權點位"],
                                "原文": row["Yu原始依據"], "待確認原因": f"缺口事件配對{row['事件配對可信度']}，誤差{row['事件配對誤差']:.2f}點"})
    manual_review = pd.DataFrame(manual_rows)
    mapping_detail = mapping_summary = future_points = pd.DataFrame()
    if args.futures_labels:
        future_points = normalize_futures_points(args.futures_labels, start, end)
        mapping_detail, mapping_summary = compare_mapping_methods(matches, future_points, taiex, futures, config)
        if not mapping_summary.empty:
            mapping_summary["訓練截止日"] = end
    output = Path(args.output_dir); output.mkdir(parents=True, exist_ok=True); stamp = f"{start:%Y%m%d}_{end:%Y%m%d}"
    files = {
        f"yu_labeled_taiex_points_{stamp}.csv": points,
        f"yu_taiex_point_matches_{stamp}.csv": matches,
        f"yu_key_k_training_{stamp}.csv": training,
        f"yu_reference_part_summary_{stamp}.csv": reference_part_summary,
        f"yu_rule_thresholds_{stamp}.csv": thresholds,
        f"yu_rule_combinations_{stamp}.csv": combinations,
        f"yu_parsed_clauses_{stamp}.csv": points,
        f"yu_rule_label_audit_{stamp}.csv": rule_audit,
        f"yu_gap_matches_{stamp}.csv": gap_matches,
        f"yu_gap_events_{stamp}.csv": gap_events,
        f"yu_gap_event_matches_{stamp}.csv": gap_event_matches,
        f"yu_gap_definition_candidates_{stamp}.csv": gap_definition_candidates,
        f"yu_gap_definition_matches_{stamp}.csv": gap_definition_matches,
        f"yu_gap_definition_summary_{stamp}.csv": gap_definition_summary,
        f"yu_pivot_reference_snapshots_{stamp}.csv": pivot_snapshots,
        f"yu_pivot_feature_comparison_{stamp}.csv": pivot_comparison,
        f"yu_stable_rule_thresholds_{stamp}.csv": stable_thresholds,
        f"yu_rule_selection_summary_{stamp}.csv": rule_selection,
        f"yu_manual_review_{stamp}.csv": manual_review,
        f"yu_parse_rejected_{stamp}.csv": parse_rejected,
        f"yu_labeled_futures_points_{stamp}.csv": future_points,
        f"yu_taiex_futures_mapping_detail_{stamp}.csv": mapping_detail,
        f"yu_taiex_futures_mapping_summary_{stamp}.csv": mapping_summary,
    }
    for name, frame in files.items():
        v1._write_csv(frame, output / name)
    rule_counts = {
        rule: int(training.get(f"Yu規則_{rule}", pd.Series(dtype=int)).sum())
        for rule in RULE_NAMES
    }
    manifest = {"version": 5, "start": str(start.date()), "end": str(end.date()), "taiex_label_files": [Path(x).name for x in args.taiex_labels],
                "futures_label_file": Path(args.futures_labels).name if args.futures_labels else None,
                "yu_points": len(points), "accepted_matches": int(matches.get("是否採用正例", pd.Series(dtype=int)).sum()),
                "positive_key_k": int(training.get("Yu曾標記關鍵K", pd.Series(dtype=int)).sum()),
                "rule_positive_k": rule_counts,
                "referenced_high_part_k": int(training.get("Yu引用部位_高點", pd.Series(dtype=int)).sum()),
                "referenced_low_part_k": int(training.get("Yu引用部位_低點", pd.Series(dtype=int)).sum()),
                "explicit_swing_high_k": int(training.get("Yu形成原因_波段高K", pd.Series(dtype=int)).sum()),
                "explicit_swing_low_k": int(training.get("Yu形成原因_波段低K", pd.Series(dtype=int)).sum()),
                "gap_events": len(gap_events), "gap_event_matches": len(gap_event_matches),
                "pivot_reference_snapshots": len(pivot_snapshots),
                "stable_rules": stable_thresholds["規則"].drop_duplicates().tolist() if not stable_thresholds.empty else [],
                "threshold_policy": "K棒形成原因與引用部位分層；波段高低只接受Yu文字明示，不再由引用高低點推定",
                "gap_policy": "同時比較昨收今開、昨收今收、前後實體、前後全K未重疊四種缺口邊界",
                "mapping_policy": "只比較明示參考日期及K棒部位均一致的歷史K；今日OHLC不列入映射證據",
                "future_data_policy": "每個分析日只使用該日及更早行情；未引用K為未標記，不是已證實負例"}
    (output / f"yu_rule_run_manifest_{stamp}.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"Yu加權點位：{len(points)}；採用配對：{manifest['accepted_matches']}；關鍵K正例：{manifest['positive_key_k']}")
    if not mapping_summary.empty:
        print("\n期現貨映射比較："); print(mapping_summary.to_string(index=False))
    print(f"\n輸出位置：{output.resolve()}")


def run_predict(args: argparse.Namespace) -> None:
    config = load_config(args.config); cutoff = pd.Timestamp(args.date).normalize()
    thresholds = pd.read_csv(args.thresholds, encoding="utf-8-sig")
    cutoff_col = "分析截止日" if "分析截止日" in thresholds else "訓練截止日"
    thresholds[cutoff_col] = pd.to_datetime(thresholds[cutoff_col], errors="coerce").dt.normalize()
    if thresholds[cutoff_col].max() > cutoff:
        raise ValueError("門檻檔使用了預測日之後的標記資料，已拒絕執行")
    start = cutoff - pd.Timedelta(days=int(config["features"]["load_calendar_days"]))
    taiex, futures = load_market_data(config, start, cutoff)
    formation = add_gap_features(stage1.build_formation_features(taiex, cutoff, config), config)
    rank_col = "穩定排名" if "穩定排名" in thresholds else "規則內排名"
    best = thresholds.loc[thresholds[rank_col].eq(1)].copy(); hit_cols = []
    if best.empty:
        raise ValueError("門檻檔沒有通過穩定驗證的規則，拒絕產生每日候選")
    for _, rule in best.iterrows():
        if rule["欄位"] not in formation:
            continue
        col = f"命中_{rule['規則']}"; values = pd.to_numeric(formation[rule["欄位"]], errors="coerce")
        formation[col] = (values >= rule["門檻"]).astype(int) if rule["比較符號"] == ">=" else (values <= rule["門檻"]).astype(int)
        hit_cols.append(col)
    formation["命中規則數"] = formation[hit_cols].sum(axis=1) if hit_cols else 0
    candidates = formation.loc[formation["命中規則數"].ge(int(config["discovery"]["prediction_min_rule_hits"]))].copy()
    candidates = candidates.sort_values(["命中規則數", config["date_column"]], ascending=[False, False]).head(int(config["discovery"]["prediction_top_k"]))
    levels = build_taiex_candidate_levels(taiex, cutoff, config)
    levels = levels.loc[levels["來源K日期"].isin(candidates[config["date_column"]])].copy()
    tx_levels = pd.DataFrame()
    if args.mapping_summary:
        mapping = pd.read_csv(args.mapping_summary, encoding="utf-8-sig")
        if "訓練截止日" in mapping:
            mapping["訓練截止日"] = pd.to_datetime(mapping["訓練截止日"], errors="coerce").dt.normalize()
            if mapping["訓練截止日"].max() > cutoff:
                raise ValueError("映射比較檔使用了預測日之後的資料，已拒絕執行")
        best_method = mapping.sort_values("映射排名").iloc[0]["映射方法"]
        tx_levels = map_prediction_levels_to_tx(levels, taiex, futures, cutoff, best_method, config)
    output = Path(args.output_dir); output.mkdir(parents=True, exist_ok=True); stamp = f"{cutoff:%Y%m%d}"
    v1._write_csv(candidates, output / f"yu_rule_key_k_candidates_{stamp}.csv")
    v1._write_csv(levels, output / f"yu_rule_key_levels_{stamp}.csv")
    if not tx_levels.empty:
        v1._write_csv(tx_levels, output / f"yu_rule_tx_levels_{stamp}.csv")
    print(candidates[[config["date_column"], "命中規則數", *hit_cols]].to_string(index=False))
    print(f"輸出位置：{output.resolve()}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Yu加權關鍵K規則反推與期現貨映射驗證")
    sub = parser.add_subparsers(dest="command", required=True)
    analyze = sub.add_parser("analyze")
    analyze.add_argument("--config", required=True); analyze.add_argument("--taiex-labels", nargs="+", required=True)
    analyze.add_argument("--futures-labels"); analyze.add_argument("--start", required=True); analyze.add_argument("--end", required=True)
    analyze.add_argument("--output-dir", default="../output/machine_learningyu_key_level_rule_discovery")
    analyze.set_defaults(func=run_analyze)
    predict = sub.add_parser("predict")
    predict.add_argument("--config", required=True); predict.add_argument("--date", required=True); predict.add_argument("--thresholds", required=True)
    predict.add_argument("--mapping-summary", help="analyze輸出的期現貨映射摘要；提供後會輸出TX候選點")
    predict.add_argument("--output-dir", default="../output/machine_learningyu_key_level_rule_discovery")
    predict.set_defaults(func=run_predict)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv); args.func(args); return 0
    except Exception as exc:
        print(f"[錯誤] {exc}"); return 1


if __name__ == "__main__":
    raise SystemExit(main())
