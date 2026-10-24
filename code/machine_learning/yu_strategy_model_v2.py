"""Yu 策略逆向分析 V2：加權選關鍵 K、映射同日期貨點位、再判斷進場情境。

核心時間規則：分析日期 D 只讀取日期 <= D 的日報；預測 D 時只用 D 以前
的 Yu 標籤。加權指數只負責選出重要日期與 K 棒，實際候選價格一律取同日 TX。
"""

from __future__ import annotations

import argparse
import json
import math
import re
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from common import db
from machine_learning import yu_strategy_model as v1


SCENARIOS = ("回測做多", "突破做多", "反彈做空", "跌破做空")
CONTINUATION_PATTERN = re.compile(r"抱單|持倉|移動停利|續抱")
BREAKOUT_PATTERN = re.compile(r"突破|站上|過高|破高|攻過")
BREAKDOWN_PATTERN = re.compile(r"跌破|殺破|破低|失守|摜破")
PULLBACK_PATTERN = re.compile(r"回測|壓回|支撐|低點|均線|日均|收盤|開盤|中關")
REBOUND_PATTERN = re.compile(r"反彈|反壓|壓力|高點|均線|日均|收盤|開盤|中關")


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    required = {"futures", "taiex", "direction_features", "direction_model", "candidate"}
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError(f"V2 設定檔缺少欄位：{missing}")
    return config


def _unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _source_columns(config: dict[str, Any], market: str) -> list[str]:
    date_column = config["date_column"]
    if market == "taiex":
        direction = [x.removeprefix("taiex__") for x in config["direction_features"] if x.startswith("taiex__")]
        return _unique([date_column, *config["taiex"]["key_fields"], *direction])
    futures = config["futures"]
    direction = [x.removeprefix("tx__") for x in config["direction_features"] if x.startswith("tx__")]
    fields = [item["field"] for item in futures["mapped_fields"] + futures["current_fields"]]
    fields += [futures["reference_price_field"], futures["range_field"], futures["volume_field"]]
    return _unique([date_column, *futures.get("entity_filter", {}).keys(), *fields, *direction])


def _load_relation(
    relation: str,
    columns: list[str],
    date_column: str,
    start: date,
    end: date,
    filters: dict[str, Any] | None = None,
) -> pd.DataFrame:
    where = [f"{v1.quote_column(date_column)} BETWEEN ? AND ?"]
    params: list[Any] = [start, end]
    for column, value in (filters or {}).items():
        where.append(f"{v1.quote_column(column)} = ?")
        params.append(value)
    statement = (
        f"SELECT {', '.join(v1.quote_column(c) for c in columns)} "
        f"FROM {v1.quote_relation(relation)} WHERE {' AND '.join(where)} "
        f"ORDER BY {v1.quote_column(date_column)}"
    )
    frame = db.query_to_df(statement, params)
    if frame.empty:
        raise RuntimeError(f"{relation} 在指定期間查無資料")
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise RuntimeError(f"{relation} 缺少欄位：{missing}")
    frame[date_column] = pd.to_datetime(frame[date_column], errors="coerce").dt.normalize()
    return frame.dropna(subset=[date_column]).drop_duplicates(date_column, keep="last").reset_index(drop=True)


def load_market_reports(config: dict[str, Any], start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    date_column = config["date_column"]
    taiex = _load_relation(
        config["taiex"]["relation"], _source_columns(config, "taiex"), date_column, start, end
    )
    futures = _load_relation(
        config["futures"]["relation"], _source_columns(config, "futures"), date_column,
        start, end, config["futures"].get("entity_filter"),
    )
    return taiex, futures


def merge_direction_report(taiex: pd.DataFrame, futures: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    date_column = config["date_column"]
    taiex_renamed = taiex.rename(columns={c: f"taiex__{c}" for c in taiex.columns if c != date_column})
    futures_renamed = futures.rename(columns={c: f"tx__{c}" for c in futures.columns if c != date_column})
    return taiex_renamed.merge(futures_renamed, on=date_column, how="inner", validate="one_to_one")


def _number(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def score_taiex_key_dates(history: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """在分析日當下可見資料中評分加權關鍵 K；不把加權點數當成期貨點數。"""
    if history.empty:
        return history.assign(關鍵K分數=[], 關鍵K原因=[])
    settings = config["taiex"]
    frame = history.copy().reset_index(drop=True)
    for column in ("成交金額", "20日均量", "實體_pct", "日振幅_昨收_pct", "跳空缺口_昨收_pct", "最高價", "最低價"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    volume_ratio = frame["成交金額"] / frame["20日均量"].replace(0, np.nan)
    body = frame["實體_pct"].abs().fillna(0)
    amplitude = frame["日振幅_昨收_pct"].abs().fillna(0)
    gap = frame["跳空缺口_昨收_pct"].abs().fillna(0)
    # 百分比欄位實際以小數儲存；截斷極端值，避免單一欄完全支配分數。
    frame["關鍵K分數"] = (
        volume_ratio.fillna(1).clip(0, 3) * 1.4
        + (body * 100).clip(0, 4) * 0.8
        + (amplitude * 100).clip(0, 5) * 0.5
        + (gap * 100).clip(0, 3) * 0.5
    )
    reasons: list[list[str]] = [[] for _ in range(len(frame))]
    for index in range(len(frame)):
        if volume_ratio.iloc[index] >= 1.3:
            reasons[index].append(f"加權放量{volume_ratio.iloc[index]:.2f}倍")
        if body.iloc[index] >= 0.01:
            reasons[index].append("加權實體K")
        if amplitude.iloc[index] >= 0.015:
            reasons[index].append("加權大振幅")
        if gap.iloc[index] >= 0.005:
            reasons[index].append("加權跳空")

    pivot_window = int(settings.get("pivot_window", 3))
    highs = frame["最高價"]
    lows = frame["最低價"]
    for index in range(len(frame)):
        left = max(0, index - pivot_window)
        right = min(len(frame), index + pivot_window + 1)
        if pd.notna(highs.iloc[index]) and highs.iloc[index] >= highs.iloc[left:right].max():
            frame.loc[index, "關鍵K分數"] += 2.0
            reasons[index].append("加權波段高點K")
        if pd.notna(lows.iloc[index]) and lows.iloc[index] <= lows.iloc[left:right].min():
            frame.loc[index, "關鍵K分數"] += 2.0
            reasons[index].append("加權波段低點K")
    frame["關鍵K原因"] = ["＋".join(items) if items else "一般K" for items in reasons]
    return frame


def select_taiex_key_dates(history: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    scored = score_taiex_key_dates(history, config)
    settings = config["taiex"]
    recent_count = int(settings.get("recent_dates_always_keep", 5))
    maximum = int(settings.get("maximum_key_dates", 25))
    threshold = float(settings.get("minimum_key_score", 3.0))
    selected = pd.concat([
        scored.loc[scored["關鍵K分數"] >= threshold].nlargest(maximum, "關鍵K分數"),
        scored.tail(recent_count),
    ]).drop_duplicates(config["date_column"])
    return selected.sort_values(config["date_column"]).reset_index(drop=True)


def _raw_candidate(
    analysis_date: pd.Timestamp,
    source_date: pd.Timestamp,
    price: Any,
    session: str,
    kind: str,
    source_field: str,
    source_kind: str,
    age_rows: int,
    reference: float,
    selector_score: float,
    selector_reason: str,
    taiex_pattern: Any = None,
    taiex_trend: Any = None,
) -> dict[str, Any] | None:
    numeric = _number(price)
    if numeric is None or numeric <= 0:
        return None
    return {
        "分析日期": analysis_date, "候選點位": numeric, "盤別": session,
        "點位類型": kind, "來源欄位": source_field, "來源日期": source_date,
        "來源種類": source_kind, "距今交易列數": age_rows,
        "距參考收盤點數": numeric - reference,
        "距參考收盤絕對點數": abs(numeric - reference),
        "加權關鍵K分數": selector_score, "加權選日原因": selector_reason,
        "加權K線型態": None if pd.isna(taiex_pattern) else taiex_pattern,
        "加權趨勢": None if pd.isna(taiex_trend) else taiex_trend,
    }


def generate_raw_candidates(
    futures: pd.DataFrame,
    taiex: pd.DataFrame,
    analysis_date: pd.Timestamp,
    config: dict[str, Any],
) -> pd.DataFrame:
    date_column = config["date_column"]
    settings = config["candidate"]
    lookback = int(settings["lookback_rows"])
    f_history = futures.loc[futures[date_column] <= analysis_date].tail(lookback).copy()
    t_history = taiex.loc[taiex[date_column] <= analysis_date].tail(lookback).copy()
    if f_history.empty or t_history.empty or f_history.iloc[-1][date_column] != analysis_date:
        return pd.DataFrame()
    current = f_history.iloc[-1]
    reference = _number(current[config["futures"]["reference_price_field"]])
    if reference is None:
        return pd.DataFrame()
    key_dates = select_taiex_key_dates(t_history, config)
    futures_by_date = f_history.set_index(date_column, drop=False)
    date_positions = {value: len(f_history) - 1 - index for index, value in enumerate(f_history[date_column])}
    rows: list[dict[str, Any]] = []

    # 加權只挑「哪一天的哪根 K 重要」；候選價格取該日 TX 的 OHLC/中關。
    for _, selector in key_dates.iterrows():
        source_date = selector[date_column]
        if source_date not in futures_by_date.index:
            continue
        source = futures_by_date.loc[source_date]
        age = int(date_positions[source_date])
        by_session: dict[str, dict[str, float]] = {}
        for spec in config["futures"]["mapped_fields"]:
            value = _number(source[spec["field"]])
            row = _raw_candidate(
                analysis_date, source_date, value, spec["session"], spec["kind"], spec["field"],
                "加權關鍵K映射TX", age, reference, float(selector["關鍵K分數"]),
                selector["關鍵K原因"], selector.get("K線型態"), selector.get("趨勢"),
            )
            if row:
                rows.append(row)
                by_session.setdefault(spec["session"], {})[spec["kind"]] = value
        for session, points in by_session.items():
            for needed, kind, label in [
                (("開盤", "收盤"), "實K中關", f"{session}開收中值"),
                (("高點", "低點"), "全K中關", f"{session}高低中值"),
            ]:
                if all(item in points for item in needed):
                    rows.append(_raw_candidate(
                        analysis_date, source_date, sum(points[item] for item in needed) / 2,
                        session, kind, label, "加權關鍵K映射TX", age, reference,
                        float(selector["關鍵K分數"]), selector["關鍵K原因"],
                        selector.get("K線型態"), selector.get("趨勢"),
                    ))

    for spec in config["futures"]["current_fields"]:
        rows.append(_raw_candidate(
            analysis_date, analysis_date, current[spec["field"]], spec["session"], spec["kind"],
            spec["field"], "當日期貨指標", 0, reference, 0.0, "當日期貨指標",
        ))
    for step in settings.get("round_steps", []):
        center = round(reference / step) * step
        for offset in range(-2, 3):
            rows.append(_raw_candidate(
                analysis_date, analysis_date, center + offset * step, "通用", f"{step}點整數關卡",
                f"round_{step}", "整數關卡", 0, reference, 0.0, "整數關卡",
            ))
    result = pd.DataFrame(row for row in rows if row is not None)
    return result.drop_duplicates(
        ["候選點位", "盤別", "點位類型", "來源日期", "來源欄位"], keep="first"
    ).reset_index(drop=True) if not result.empty else result


def cluster_candidates(raw: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    if raw.empty:
        return raw
    tolerance = float(config["candidate"]["cluster_tolerance_points"])
    ordered = raw.sort_values("候選點位").reset_index(drop=True)
    groups: list[list[int]] = []
    group_prices: list[list[float]] = []
    for index, row in ordered.iterrows():
        price = float(row["候選點位"])
        if not groups:
            groups.append([index])
            group_prices.append([price])
            continue
        if abs(price - float(np.median(group_prices[-1]))) <= tolerance:
            groups[-1].append(index)
            group_prices[-1].append(price)
        else:
            groups.append([index])
            group_prices.append([price])
    records = []
    for number, indices in enumerate(groups, 1):
        group = ordered.loc[indices].copy()
        # 代表點位採群內中位數，降低單一極端來源的影響。
        anchor = float(group["候選點位"].median())
        reference = float(
            group.iloc[0]["候選點位"] - group.iloc[0]["距參考收盤點數"]
        )
        evidence = group.sort_values(["加權關鍵K分數", "距今交易列數"], ascending=[False, True])
        records.append({
            "分析日期": group.iloc[0]["分析日期"], "點位群編號": number,
            "區間下緣": float(group["候選點位"].min()), "代表點位": anchor,
            "區間上緣": float(group["候選點位"].max()), "依據數": len(group),
            "來源日期數": int(group["來源日期"].nunique()),
            "距參考收盤點數": anchor - reference, "距參考收盤絕對點數": abs(anchor - reference),
            "最近來源列數": int(group["距今交易列數"].min()),
            "最高加權關鍵K分數": float(group["加權關鍵K分數"].max()),
            "主要盤別": str(evidence.iloc[0]["盤別"]), "主要類型": str(evidence.iloc[0]["點位類型"]),
            "主要來源日期": evidence.iloc[0]["來源日期"],
            "主要加權選日原因": str(evidence.iloc[0]["加權選日原因"]),
            "重疊依據": "｜".join(
                _unique(
                    f"{pd.Timestamp(row['來源日期']):%m/%d} {row['盤別']}{row['點位類型']}"
                    + (f"（{row['加權選日原因']}）" if row["來源種類"] == "加權關鍵K映射TX" else "")
                    for _, row in evidence.head(12).iterrows()
                )
            ),
        })
    return pd.DataFrame(records)


def infer_scenario(plan: pd.Series, reference_price: float | None = None) -> str | None:
    direction = v1.normalize_direction(plan.get("交易方向"))
    text = " ".join(str(plan.get(column) or "") for column in ("進場條件", "進場依據", "進場附註"))
    if CONTINUATION_PATTERN.search(text) or direction not in {"多", "空"}:
        return None
    if direction == "多" and BREAKOUT_PATTERN.search(text):
        return "突破做多"
    if direction == "空" and BREAKDOWN_PATTERN.search(text):
        return "跌破做空"
    if direction == "多" and PULLBACK_PATTERN.search(text):
        return "回測做多"
    if direction == "空" and REBOUND_PATTERN.search(text):
        return "反彈做空"
    entry = _number(plan.get("進場點"))
    if entry is not None and reference_price is not None:
        if direction == "多":
            return "回測做多" if entry <= reference_price else "突破做多"
        return "反彈做空" if entry >= reference_price else "跌破做空"
    return "回測做多" if direction == "多" else "反彈做空"


def scenario_compatible(clusters: pd.DataFrame, scenario: str, config: dict[str, Any]) -> pd.Series:
    minimum = float(config["candidate"].get("minimum_entry_distance_points", 0))
    signed = clusters["距參考收盤點數"]
    if scenario in {"回測做多", "跌破做空"}:
        return signed <= -minimum
    return signed >= minimum


def actionable_distance(futures_row: pd.Series, config: dict[str, Any]) -> float:
    settings = config["candidate"]
    daily_range = _number(futures_row[config["futures"]["range_field"]], 0.0) or 0.0
    calculated = max(
        float(settings["minimum_actionable_distance_points"]),
        daily_range * float(settings["range_distance_multiplier"]),
    )
    return min(calculated, float(settings["maximum_actionable_distance_points"]))


def match_plan_to_cluster(
    plan: pd.Series,
    clusters: pd.DataFrame,
    reference: float,
    config: dict[str, Any],
) -> dict[str, Any]:
    scenario = infer_scenario(plan, reference)
    base = {
        "分析日期": plan["分析日期"], "方案序號": plan.get("方案序號"),
        "交易方向": v1.normalize_direction(plan.get("交易方向")), "進場情境": scenario,
        "Yu進場點": plan.get("進場點"), "Yu進場條件": plan.get("進場條件"),
        "Yu進場依據": plan.get("進場依據"), "資料品質狀態": plan.get("資料品質狀態"),
    }
    entry = _number(plan.get("進場點"))
    if scenario is None:
        return {**base, "比對狀態": "排除：既有持倉或無交易方向"}
    if entry is None or clusters.empty:
        return {**base, "比對狀態": "無固定進場點或無候選"}
    compatible = clusters.loc[scenario_compatible(clusters, scenario, config)].copy()
    if compatible.empty:
        return {**base, "比對狀態": "無符合進場情境的候選"}
    compatible["差距點數"] = np.where(
        entry < compatible["區間下緣"], compatible["區間下緣"] - entry,
        np.where(entry > compatible["區間上緣"], entry - compatible["區間上緣"], 0.0),
    )
    best = compatible.sort_values(["差距點數", "依據數", "最近來源列數"], ascending=[True, False, True]).iloc[0]
    status = "命中" if best["差距點數"] <= float(config["candidate"]["match_tolerance_points"]) else "未命中：超過容許點數"
    return {**base, "比對狀態": status, **best.to_dict()}


def build_historical_matches(
    labels: pd.DataFrame,
    futures: pd.DataFrame,
    taiex: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    date_column = config["date_column"]
    futures_index = futures.set_index(date_column)
    cache: dict[pd.Timestamp, pd.DataFrame] = {}
    output = []
    for _, plan in labels.iterrows():
        analysis_date = plan["分析日期"]
        if analysis_date not in futures_index.index:
            output.append({"分析日期": analysis_date, "方案序號": plan.get("方案序號"), "比對狀態": "缺期貨日報"})
            continue
        clusters = cache.setdefault(
            analysis_date,
            cluster_candidates(generate_raw_candidates(futures, taiex, analysis_date, config), config),
        )
        reference = _number(futures_index.loc[analysis_date, config["futures"]["reference_price_field"]])
        output.append(match_plan_to_cluster(plan, clusters, reference, config))
    return pd.DataFrame(output)


def summarize_historical_matches(matches: pd.DataFrame) -> pd.DataFrame:
    required = {"比對狀態", "進場情境", "主要類型", "主要盤別", "差距點數"}
    if matches.empty or not required.issubset(matches.columns):
        return pd.DataFrame(columns=[
            "進場情境", "主要類型", "主要盤別", "命中筆數", "平均差距點數", "情境內占比_pct"
        ])
    hits = matches.loc[matches["比對狀態"] == "命中"].copy()
    if hits.empty:
        return pd.DataFrame(columns=[
            "進場情境", "主要類型", "主要盤別", "命中筆數", "平均差距點數", "情境內占比_pct"
        ])
    summary = hits.groupby(["進場情境", "主要類型", "主要盤別"], dropna=False).agg(
        命中筆數=("分析日期", "size"), 平均差距點數=("差距點數", "mean")
    ).reset_index()
    totals = hits.groupby("進場情境").size()
    summary["情境內占比_pct"] = summary.apply(
        lambda row: round(row["命中筆數"] / totals[row["進場情境"]] * 100, 2), axis=1
    )
    return summary.sort_values(["進場情境", "命中筆數"], ascending=[True, False]).reset_index(drop=True)


def rank_clusters(
    clusters: pd.DataFrame,
    historical_matches: pd.DataFrame,
    futures_row: pd.Series,
    target_date: pd.Timestamp,
    config: dict[str, Any],
) -> pd.DataFrame:
    if clusters.empty:
        return clusters
    settings = config["candidate"]
    max_distance = actionable_distance(futures_row, config)
    required_history_columns = {"分析日期", "比對狀態", "進場情境", "主要類型", "主要盤別"}
    if historical_matches.empty or not required_history_columns.issubset(historical_matches.columns):
        prior_hits = pd.DataFrame(columns=sorted(required_history_columns))
    else:
        prior_hits = historical_matches.loc[
            (historical_matches["分析日期"] < target_date)
            & (historical_matches["比對狀態"] == "命中")
        ].copy()
    blocks = []
    for scenario in SCENARIOS:
        block = clusters.loc[scenario_compatible(clusters, scenario, config)].copy()
        block = block.loc[block["距參考收盤絕對點數"] <= max_distance]
        if block.empty:
            continue
        scenario_hits = prior_hits.loc[prior_hits["進場情境"] == scenario]
        family_count = scenario_hits.groupby(["主要類型", "主要盤別"]).size() if not scenario_hits.empty else pd.Series(dtype=int)
        block["歷史同情境選用次數"] = [
            int(family_count.get((row["主要類型"], row["主要盤別"]), 0)) for _, row in block.iterrows()
        ]
        half_life = float(settings["recency_half_life_rows"])
        block["新鮮度"] = np.power(0.5, block["最近來源列數"] / half_life)
        distance_penalty = block["距參考收盤絕對點數"] / max_distance * 12
        block["候選分數"] = (
            np.minimum(block["依據數"], 8) * 4
            + np.minimum(block["最高加權關鍵K分數"], 12) * 2
            + block["新鮮度"] * 12
            + np.log1p(block["歷史同情境選用次數"]) * 6
            - distance_penalty
        )
        block["進場情境"] = scenario
        block["位置角色"] = "支撐" if scenario in {"回測做多", "跌破做空"} else "壓力"
        block["可交易最大距離"] = max_distance
        blocks.append(block.nlargest(int(settings["top_per_scenario"]), "候選分數"))
    if not blocks:
        return pd.DataFrame()
    return pd.concat(blocks, ignore_index=True).sort_values(["進場情境", "候選分數"], ascending=[True, False])


def build_direction_samples(labels: pd.DataFrame, combined: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    daily = []
    for analysis_date, group in labels.groupby("分析日期"):
        item = {"分析日期": analysis_date}
        for source, target in (("分析方向", "分析方向_標籤"), ("交易方向", "交易方向_標籤")):
            directions = {v1.normalize_direction(value) for value in group[source]} - {"無"}
            item[target] = next(iter(directions)) if len(directions) == 1 else "無"
        daily.append(item)
    frame = pd.DataFrame(daily)
    return frame.merge(
        combined, left_on="分析日期", right_on=config["date_column"], how="inner"
    ).sort_values("分析日期").reset_index(drop=True)


def direction_walk_forward(
    samples: pd.DataFrame,
    config: dict[str, Any],
    evaluation_start: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    # 沿用 V1 的透明決策樹與逐日 walk-forward，但 V2 特徵已改成加權為主、期貨為輔且移除月份。
    results, rules = v1.walk_forward_direction(samples, config)
    if evaluation_start is not None:
        results = results.loc[results["分析日期"] >= evaluation_start].reset_index(drop=True)
    metrics = v1.direction_metrics(results)
    baselines = []
    for prefix in ("看盤", "交易"):
        valid = results.dropna(subset=[f"{prefix}預測"])
        if valid.empty:
            baselines.append({"目標": prefix, "模型命中率_pct": None, "多數類基準_pct": None, "相對基準提升_pct": None})
            continue
        actual = valid[f"{prefix}實際"]
        baseline = actual.value_counts(normalize=True).max() * 100
        model_rate = (valid[f"{prefix}預測"] == actual).mean() * 100
        baselines.append({
            "目標": prefix, "模型命中率_pct": round(model_rate, 2),
            "多數類基準_pct": round(float(baseline), 2),
            "相對基準提升_pct": round(float(model_rate - baseline), 2),
        })
    return results, metrics.merge(pd.DataFrame(baselines), on="目標", how="left"), rules


def confusion_report(results: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for prefix in ("看盤", "交易"):
        valid = results.dropna(subset=[f"{prefix}預測"])
        matrix = pd.crosstab(valid[f"{prefix}實際"], valid[f"{prefix}預測"])
        for actual in ("多", "空", "無"):
            for predicted in ("多", "空", "無"):
                rows.append({"目標": prefix, "實際": actual, "預測": predicted, "日數": int(matrix.get(predicted, {}).get(actual, 0))})
    return pd.DataFrame(rows)


def _load_inputs(args: argparse.Namespace, config: dict[str, Any]):
    labels = v1.load_yu_labels(args.labels, config.get("labels_sheet"))
    start = pd.Timestamp(getattr(args, "start", None) or labels["分析日期"].min()).normalize()
    end = pd.Timestamp(getattr(args, "end", None) or getattr(args, "date", None)).normalize()
    lookback_days = int(config["candidate"]["lookback_rows"]) * 3
    # 指定較晚的回測起日時，仍載入更早的 Yu 標籤與日報供訓練；
    # start 只限制評估區間，不可把既有歷史訓練資料一起切掉。
    history_start = min(start, labels["分析日期"].min()) - pd.Timedelta(days=lookback_days)
    taiex, futures = load_market_reports(config, history_start.date(), end.date())
    return labels, taiex, futures, start, end


def run_analyze(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    labels, taiex, futures, start, end = _load_inputs(args, config)
    evaluation_labels = labels.loc[labels["分析日期"].between(start, end)].copy()
    training_labels = labels.loc[labels["分析日期"] <= end].copy()
    combined = merge_direction_report(taiex, futures, config)
    matches = build_historical_matches(evaluation_labels, futures, taiex, config)
    source_summary = summarize_historical_matches(matches)
    samples = build_direction_samples(training_labels, combined, config)
    directions, metrics, rules = direction_walk_forward(samples, config, evaluation_start=start)
    confusion = confusion_report(directions)
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    v1._write_csv(matches, output / "yu_v2_candidate_matches.csv")
    v1._write_csv(source_summary, output / "yu_v2_candidate_source_summary.csv")
    v1._write_csv(directions, output / "yu_v2_direction_walk_forward.csv")
    v1._write_csv(metrics, output / "yu_v2_direction_metrics.csv")
    v1._write_csv(confusion, output / "yu_v2_direction_confusion.csv")
    (output / "yu_v2_direction_rules.txt").write_text(rules, encoding="utf-8")
    hit = int((matches.get("比對狀態") == "命中").sum())
    eligible = int(matches.get("Yu進場點", pd.Series(dtype=float)).notna().sum())
    print(f"加權選K→期貨點位群：{hit}/{eligible} 筆命中")
    print(metrics.to_string(index=False))
    print(f"輸出位置：{output.resolve()}")


def run_predict(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    labels, taiex, futures, _, target = _load_inputs(args, config)
    combined = merge_direction_report(taiex, futures, config)
    target_row = combined.loc[combined[config["date_column"]] == target]
    if target_row.empty:
        raise RuntimeError(f"{target.date()} 缺少加權或期貨日報")
    prior_labels = labels.loc[labels["分析日期"] < target].copy()
    matches = build_historical_matches(prior_labels, futures, taiex, config)
    samples = build_direction_samples(prior_labels, combined, config)
    prediction = v1.predict_direction_for_date(samples, target_row, target, config)
    raw = generate_raw_candidates(futures, taiex, target, config)
    clusters = cluster_candidates(raw, config)
    futures_row = futures.loc[futures[config["date_column"]] == target].iloc[0]
    rankings = rank_clusters(clusters, matches, futures_row, target, config)
    taiex_history = taiex.loc[taiex[config["date_column"]] <= target].tail(
        int(config["candidate"]["lookback_rows"])
    )
    key_dates = select_taiex_key_dates(taiex_history, config)
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    v1._write_csv(prediction, output / f"yu_v2_direction_prediction_{target:%Y%m%d}.csv")
    v1._write_csv(rankings, output / f"yu_v2_candidate_zones_{target:%Y%m%d}.csv")
    v1._write_csv(key_dates, output / f"yu_v2_taiex_key_dates_{target:%Y%m%d}.csv")
    print(prediction.to_string(index=False))
    print(f"四種情境候選區已輸出：{output.resolve()}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Yu 策略逆向分析 V2")
    subparsers = parser.add_subparsers(dest="command", required=True)
    analyze = subparsers.add_parser("analyze", help="歷史選點比對與方向 walk-forward")
    analyze.add_argument("--config", required=True)
    analyze.add_argument("--labels", required=True)
    analyze.add_argument("--start", required=True)
    analyze.add_argument("--end", required=True)
    analyze.add_argument("--output-dir", default="../output/machine_learningyu_strategy_v2")
    analyze.set_defaults(func=run_analyze)
    predict = subparsers.add_parser("predict", help="指定日期方向與四情境候選區")
    predict.add_argument("--config", required=True)
    predict.add_argument("--labels", required=True)
    predict.add_argument("--date", required=True)
    predict.add_argument("--output-dir", default="../output/machine_learningyu_strategy_v2")
    predict.set_defaults(func=run_predict)
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
