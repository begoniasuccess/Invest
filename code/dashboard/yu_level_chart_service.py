from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import copy
import json

import pandas as pd

from machine_learning import yu_key_k_stage1 as stage1
from machine_learning import yu_key_level_rule_discovery as discovery
from machine_learning.yu_candidate_review_system import analyze_as_of, cluster_levels, load_config
from common import db


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = PROJECT_ROOT / "machine_learning" / "configs" / "yu_candidate_review_v3.json"
STRICT_CONFIG = PROJECT_ROOT / "machine_learning" / "configs" / "yu_candidate_review_strict.json"
YU_HISTORY_START = pd.Timestamp("2025-01-01")
YU_ACTUAL_LABELS = PROJECT_ROOT / "Yu點位資料整理_2025_2026.xlsx"


def _text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value)


def _candidate_config(mode: str) -> dict:
    config = load_config(DEFAULT_CONFIG)
    if mode == "broad":
        return config
    if mode != "strict":
        raise ValueError("候選模式只支援 strict 或 broad")
    strict_spec = json.loads(STRICT_CONFIG.read_text(encoding="utf-8"))
    result = copy.deepcopy(config)
    result["name"] = strict_spec["name"]
    result["version"] = strict_spec["version"]
    result["candidate"].update(strict_spec["candidate_overrides"])
    return result


def _actual_yu_levels(start: pd.Timestamp, cutoff: pd.Timestamp) -> dict:
    if not YU_ACTUAL_LABELS.exists():
        raise ValueError(f"找不到Yu整理點位檔：{YU_ACTUAL_LABELS.name}")
    labels = discovery.normalize_taiex_labels([YU_ACTUAL_LABELS], start, cutoff)
    close_frame = db.query_to_df(
        'SELECT "收盤價" FROM report.v_taiex_report '
        'WHERE "日期" <= ? ORDER BY "日期" DESC LIMIT 1',
        (cutoff.strftime("%Y-%m-%d"),),
    )
    current_close = float(close_frame.iloc[0]["收盤價"]) if not close_frame.empty else 0.0
    zones: list[dict] = []
    if not labels.empty:
        labels = labels.copy()
        labels["Yu加權點位"] = pd.to_numeric(labels["Yu加權點位"], errors="coerce")
        labels = labels.dropna(subset=["Yu加權點位"])
        for rank, (level, group) in enumerate(
            labels.groupby("Yu加權點位", sort=False), start=1
        ):
            analysis_dates = sorted({f"{pd.Timestamp(x):%Y-%m-%d}" for x in group["分析日期"]})
            reference_dates = sorted({
                date.strip()
                for value in group["明示參考日期"].fillna("").astype(str)
                for date in value.split("|") if date.strip()
            })
            roles = sorted(set(group["點位角色"].fillna("").astype(str)) - {""})
            categories = sorted(set(group["已整理點位分類"].fillna("").astype(str)) - {""})
            kinds = sorted(set(group["明示點位類型"].fillna("").astype(str)) - {""})
            reasons = [x for x in group["Yu點位子句"].fillna("").astype(str).unique() if x]
            reason_text = "｜".join([
                *roles, *categories, *kinds,
                *(reasons[:3]),
            ])
            zones.append({
                "id": f"YU-ACTUAL-{float(level):.4f}", "rank": rank,
                "lower": round(float(level), 2), "upper": round(float(level), 2),
                "level": round(float(level), 2), "reasons": reason_text,
                "source_dates": reference_dates or analysis_dates,
                "analysis_dates": analysis_dates,
                "source_count": len(group), "status": "YU實際使用",
                "distance": round(float(level) - current_close, 2),
                "importance_score": 100.0, "importance_grade": "S",
                "direction_bias": "未分類", "direction_strength": 0.0,
                "candle_shapes": "Yu實際使用",
            })
    zones.sort(key=lambda row: (row["analysis_dates"][-1], row["level"]), reverse=True)
    for rank, row in enumerate(zones, start=1):
        row["rank"] = rank
    return {
        "start_date": start.strftime("%Y-%m-%d"),
        "as_of_date": cutoff.strftime("%Y-%m-%d"), "mode": "actual",
        "raw_count": len(labels), "count": len(zones), "zones": zones,
        "note": "直接取自Yu點位資料整理檔的實際使用點位；相同點位合併但保留全部使用日期。",
    }


def load_yu_chart_date_bounds() -> tuple[str, str]:
    """Yu頁面從2025開始，結束日採資料庫實際最新交易日。"""
    bounds = db.query_to_df(
        'SELECT MIN("日期") AS min_date, MAX("日期") AS max_date '
        'FROM report.v_taiex_report WHERE "日期" >= ?',
        (YU_HISTORY_START.strftime("%Y-%m-%d"),),
    )
    if bounds.empty or pd.isna(bounds.iloc[0]["max_date"]):
        raise ValueError("加權指數報表目前沒有2025年以後資料")
    source_min = max(YU_HISTORY_START, pd.Timestamp(bounds.iloc[0]["min_date"]).normalize())
    source_max = pd.Timestamp(bounds.iloc[0]["max_date"]).normalize()
    return source_min.strftime("%Y-%m-%d"), source_max.strftime("%Y-%m-%d")


@lru_cache(maxsize=16)
def load_yu_chart_levels(start_date: str, as_of_date: str, mode: str = "strict") -> dict:
    start = pd.Timestamp(start_date).normalize()
    cutoff = pd.Timestamp(as_of_date).normalize()
    if start > cutoff:
        raise ValueError("開始日期不可晚於結束日期")
    source_min, source_max = map(pd.Timestamp, load_yu_chart_date_bounds())
    if start < source_min:
        raise ValueError(f"Yu點位資料從 {source_min:%Y-%m-%d} 開始")
    if cutoff > source_max:
        raise ValueError(f"結束日期不可晚於資料庫最新交易日 {source_max:%Y-%m-%d}")
    if mode == "actual":
        return _actual_yu_levels(start, cutoff)
    config = _candidate_config(mode)
    taiex, futures = stage1.load_reports(config, cutoff)
    raw, all_zones, _, _ = analyze_as_of(taiex, futures, cutoff, config)

    # 這個頁面要看的是區間內「曾形成過」的全部 K 棒候選，不是截止日的 30 組審核快照。
    historical_raw = raw.loc[
        raw["market"].eq("TAIEX")
        & raw["source_category"].eq("STATIC_K_LEVEL")
        & pd.to_datetime(raw["source_date"]).between(start, cutoff)
    ].copy()
    current_atr = float(all_zones.attrs.get("current_atr", 1.0))
    historical, _ = cluster_levels(historical_raw, current_atr, config)
    if not historical.empty:
        historical["latest_source_date"] = historical["source_dates"].map(
            lambda value: max(str(value).split("|"))
        )
        historical = historical.sort_values(
            ["latest_source_date", "representative_level"], ascending=[False, False]
        ).reset_index(drop=True)
        historical["history_rank"] = historical.index + 1

    zones = []
    for row in historical.to_dict("records"):
        dates = [date for date in _text(row.get("source_dates")).split("|") if date]
        zones.append({
            "id": _text(row.get("zone_id")), "rank": int(row.get("history_rank", 0)),
            "lower": round(float(row["zone_lower"]), 2),
            "upper": round(float(row["zone_upper"]), 2),
            "level": round(float(row["representative_level"]), 2),
            "reasons": _text(row.get("source_rules")), "source_dates": dates,
            "source_count": int(row.get("source_count", 0)),
            "status": _text(row.get("current_status")),
            "distance": round(float(row.get("distance_from_current", 0)), 2),
            "importance_score": round(float(row.get("importance_score", 0)), 2),
            "importance_grade": _text(row.get("importance_grade")) or "D",
            "direction_bias": _text(row.get("direction_bias")) or "中性",
            "direction_strength": round(float(row.get("direction_strength", 0)), 2),
            "candle_shapes": _text(row.get("candle_shapes")),
        })
    return {
        "start_date": start.strftime("%Y-%m-%d"),
        "as_of_date": cutoff.strftime("%Y-%m-%d"),
        "mode": mode, "raw_count": len(historical_raw), "count": len(zones), "zones": zones,
        "note": "列出區間內曾形成的全部K棒候選；相近原始點位先合併，且不使用截止日以後資料。",
    }
