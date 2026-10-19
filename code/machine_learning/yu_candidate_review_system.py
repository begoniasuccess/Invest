"""Yu關鍵點位候選產生與人工審核系統。

只使用 ``--date`` 當日及以前資料。此模組不嘗試猜Yu最後交易點，而是：
1. 以透明規則產生高涵蓋率原始候選；2. 合併相近點位但保留來源；
3. 匯出Yu審核與漏點補登表；4. 可選擇寫入PostgreSQL。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import uuid
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from common import db
from machine_learning import yu_key_k_stage1 as stage1
from machine_learning import yu_key_level_rule_discovery as discovery
from machine_learning import yu_strategy_model as v1


REVIEW_ACTIONS = "KEEP|DROP|MERGE|EDIT|UNCERTAIN"
REASON_CODES = (
    "KEEP_OVERLAP|KEEP_STRUCTURE|KEEP_FIRST_RETEST|DROP_TOO_CLOSE|"
    "DROP_WEAK_STRUCTURE|DROP_TOO_OLD|DROP_OVERTESTED|DROP_BROKEN|"
    "MERGE_SAME_ZONE|WRONG_SOURCE_TYPE|MISSING"
)


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    required = {"date_column", "taiex", "futures", "features", "scoring", "candidate"}
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError(f"候選審核設定缺少欄位：{missing}")
    return config


def _stable_id(prefix: str, *parts: Any) -> str:
    text = "|".join("" if pd.isna(x) else str(x) for x in parts)
    return f"{prefix}_{hashlib.sha1(text.encode('utf-8')).hexdigest()[:16]}"


def _number(value: Any, default: float = 0.0) -> float:
    value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return default if pd.isna(value) else float(value)


def _formation_hits(row: pd.Series, config: dict[str, Any]) -> list[str]:
    c = config["candidate"]
    if c.get("selection_mode", "broad") == "strict":
        strict = c["strict"]
        families: dict[str, list[str]] = {"volume": [], "body": [], "shadow": [], "pivot": []}
        volume_ratio = _number(row.get("量比_20"))
        volume_pct = _number(row.get("成交量百分位_60"))
        body_atr = _number(row.get("實體_ATR倍數"))
        body_pct = _number(row.get("實體百分位_60"))
        upper_atr = _number(row.get("上影_ATR倍數"))
        upper_pct = _number(row.get("上影百分位_60"))
        lower_atr = _number(row.get("下影_ATR倍數"))
        lower_pct = _number(row.get("下影百分位_60"))

        # 嚴格模式改採「ATR門檻 AND 滾動百分位」，避免任一普通條件就入選。
        if volume_ratio >= float(strict["large_volume_ratio"]) and volume_pct >= float(strict["large_volume_percentile"]):
            families["volume"].append("大量K")
        if body_atr >= float(strict["long_body_atr"]) and body_pct >= float(strict["long_body_percentile"]):
            families["body"].append("長紅K" if row.get("實體點數", 0) > 0 else "長黑K")
        if upper_atr >= float(strict["long_shadow_atr"]) and upper_pct >= float(strict["long_shadow_percentile"]):
            families["shadow"].append("長上影")
        if lower_atr >= float(strict["long_shadow_atr"]) and lower_pct >= float(strict["long_shadow_percentile"]):
            families["shadow"].append("長下影")
        for window in [int(x) for x in strict["pivot_windows"]]:
            if row.get(f"是否截至當日{window}日新高", 0) == 1:
                families["pivot"].append(f"截至當日{window}日高")
            if row.get(f"是否截至當日{window}日新低", 0) == 1:
                families["pivot"].append(f"截至當日{window}日低")

        active_family_count = sum(bool(values) for values in families.values())
        if active_family_count >= int(strict["minimum_signal_families"]):
            return [hit for values in families.values() for hit in values]

        # 單一訊號只有達到極端門檻才獨立成為關鍵K；波段新高低不單獨放行。
        if volume_ratio >= float(strict["extreme_volume_ratio"]) and volume_pct >= float(strict["extreme_percentile"]):
            return families["volume"]
        if body_atr >= float(strict["extreme_body_atr"]) and body_pct >= float(strict["extreme_percentile"]):
            return families["body"]
        extreme_shadow = float(strict["extreme_shadow_atr"])
        if ((upper_atr >= extreme_shadow and upper_pct >= float(strict["extreme_percentile"])) or
                (lower_atr >= extreme_shadow and lower_pct >= float(strict["extreme_percentile"]))):
            return families["shadow"]
        return []

    hits: list[str] = []
    if (row.get("量比_20", 0) >= float(c["large_volume_ratio"]) or
            row.get("成交量百分位_60", 0) >= float(c["large_volume_percentile"])):
        hits.append("大量K")
    if (row.get("實體_ATR倍數", 0) >= float(c["long_body_atr"]) or
            row.get("實體百分位_60", 0) >= float(c["long_body_percentile"])):
        hits.append("長紅K" if row.get("實體點數", 0) > 0 else "長黑K")
    if (row.get("上影_ATR倍數", 0) >= float(c["long_shadow_atr"]) or
            row.get("上影百分位_60", 0) >= float(c["long_shadow_percentile"])):
        hits.append("長上影")
    if (row.get("下影_ATR倍數", 0) >= float(c["long_shadow_atr"]) or
            row.get("下影百分位_60", 0) >= float(c["long_shadow_percentile"])):
        hits.append("長下影")
    for window in [int(x) for x in c["pivot_windows"]]:
        if row.get(f"是否截至當日{window}日新高", 0) == 1:
            hits.append(f"截至當日{window}日高")
        if row.get(f"是否截至當日{window}日新低", 0) == 1:
            hits.append(f"截至當日{window}日低")
    return hits


def _parts_for_rule(rule: str) -> list[str]:
    if rule == "長上影":
        return ["高點", "實體上緣"]
    if rule == "長下影":
        return ["低點", "實體下緣"]
    return ["開盤", "高點", "低點", "收盤", "實K中關", "全K中關"]


def _original_role(part: str) -> str:
    if part in {"高點", "實體上緣", "布林上軌"}:
        return "RESISTANCE"
    if part in {"低點", "實體下緣", "布林下軌"}:
        return "SUPPORT"
    return "BOTH"


def _part_values(row: pd.Series, cmap: dict[str, str]) -> dict[str, float]:
    open_ = _number(row[cmap["open"]], np.nan)
    high = _number(row[cmap["high"]], np.nan)
    low = _number(row[cmap["low"]], np.nan)
    close = _number(row[cmap["close"]], np.nan)
    return {
        "開盤": open_, "高點": high, "低點": low, "收盤": close,
        "實體上緣": max(open_, close), "實體下緣": min(open_, close),
        "實K中關": (open_ + close) / 2, "全K中關": (high + low) / 2,
    }


def _candle_profile(row: pd.Series, cmap: dict[str, str]) -> dict[str, Any]:
    """將OHLC轉成Yu可理解的K棒型態與方向強度；只使用該根K自身資料。"""
    values = _part_values(row, cmap)
    open_, high, low, close = (values[x] for x in ("開盤", "高點", "低點", "收盤"))
    if not all(np.isfinite(x) for x in (open_, high, low, close)):
        return {"candle_shape": "資料不足", "direction_bias": "中性", "direction_strength": 0.0,
                "shape_prior_score": 0.0}
    full_range = max(0.0, high - low)
    body = close - open_
    body_abs = abs(body)
    upper = max(0.0, high - max(open_, close))
    lower = max(0.0, min(open_, close) - low)
    # 期貨日K幾乎不會四價完全相等；以全K振幅極小於ATR作近似，但不把一般十字誤判為一字線。
    atr = max(_number(row.get("ATR"), 0.0), 1.0)
    if full_range <= max(1.0, atr * 0.015):
        return {"candle_shape": "一字線", "direction_bias": "中性", "direction_strength": 0.0,
                "shape_prior_score": 100.0}
    body_ratio, upper_ratio, lower_ratio = body_abs / full_range, upper / full_range, lower / full_range
    color = "紅" if body > 0 else "黑" if body < 0 else "十字"
    sign = 1.0 if body > 0 else -1.0 if body < 0 else 0.0
    if body_ratio <= 0.08:
        if lower_ratio >= 0.65 and upper_ratio <= 0.15:
            shape, prior = "T字線", 62.0
        elif upper_ratio >= 0.65 and lower_ratio <= 0.15:
            shape, prior = "倒T字線", 30.0
        else:
            shape, prior = "十字線", 42.0
    elif body_ratio >= 0.80 and upper_ratio <= 0.10 and lower_ratio <= 0.10:
        shape, prior = f"大{color}實體K", 90.0
    elif lower_ratio >= 0.55 and upper_ratio <= 0.12:
        # Yu說黑K反之：紅K長下影偏強，黑K長下影代表空方曾被收復，方向強度較弱。
        shape, prior = f"長下影{color}K", 72.0 if color == "紅" else 40.0
    elif upper_ratio >= 0.55 and lower_ratio <= 0.12:
        shape, prior = f"長上影{color}K", 72.0 if color == "黑" else 40.0
    elif lower_ratio >= 0.12 and upper_ratio <= 0.10:
        shape, prior = f"下影線{color}K", 80.0 if color == "紅" else 40.0
    elif upper_ratio >= 0.12 and lower_ratio <= 0.10:
        shape, prior = f"上影線{color}K", 80.0 if color == "黑" else 40.0
    elif upper_ratio >= 0.10 and lower_ratio >= 0.10:
        shape, prior = f"上下影{color}K", 52.0
    else:
        shape, prior = f"小{color}K", 46.0
    # 方向採有正負號的獨立欄位；點位重要度則永遠是0～100正分。
    return {
        "candle_shape": shape,
        "direction_bias": "偏多" if sign > 0 else "偏空" if sign < 0 else "中性",
        "direction_strength": round(sign * prior, 2),
        "shape_prior_score": prior,
    }


def _point_relevance_score(rule: str, part: str) -> float:
    """同一關鍵K的各價位並非同權；影線端點與相應實體邊界優先。"""
    if rule == "長上影":
        return {"高點": 100.0, "實體上緣": 82.0}.get(part, 55.0)
    if rule == "長下影":
        return {"低點": 100.0, "實體下緣": 82.0}.get(part, 55.0)
    if rule.startswith("截至當日") and rule.endswith("日高"):
        return {"高點": 100.0, "收盤": 78.0, "開盤": 70.0, "實K中關": 62.0,
                "全K中關": 58.0, "低點": 52.0}.get(part, 55.0)
    if rule.startswith("截至當日") and rule.endswith("日低"):
        return {"低點": 100.0, "收盤": 78.0, "開盤": 70.0, "實K中關": 62.0,
                "全K中關": 58.0, "高點": 52.0}.get(part, 55.0)
    return {"高點": 88.0, "低點": 88.0, "開盤": 80.0, "收盤": 80.0,
            "實K中關": 64.0, "全K中關": 58.0,
            "實體上緣": 82.0, "實體下緣": 82.0}.get(part, 55.0)


def build_raw_taiex_levels(
    taiex: pd.DataFrame, cutoff: pd.Timestamp, config: dict[str, Any]
) -> pd.DataFrame:
    """產生原始候選；不因排名較低而刪除，且每根K只看形成日以前特徵。"""
    date_col = config["date_column"]
    cmap = config["taiex"]["column_map"]
    formation = stage1.build_formation_features(taiex, cutoff, config)
    formation = formation.tail(int(config["candidate"]["lookback_trading_rows"])).copy()
    rows: list[dict[str, Any]] = []
    for _, bar in formation.iterrows():
        source_date = pd.Timestamp(bar[date_col]).normalize()
        values = _part_values(bar, cmap)
        profile = _candle_profile(bar, cmap)
        for rule in _formation_hits(bar, config):
            base_score = _rule_score(bar, rule)
            for part in _parts_for_rule(rule):
                level = values[part]
                if not np.isfinite(level):
                    continue
                rows.append({
                    "raw_level_id": _stable_id("TL", source_date.date(), rule, part, round(level, 4)),
                    "market": "TAIEX", "source_category": "STATIC_K_LEVEL",
                    "source_date": source_date,
                    "source_rule": rule, "source_part": part, "level_value": level,
                    "original_role": _original_role(part),
                    "formation_score": base_score, "source_atr": _number(bar.get("ATR"), np.nan),
                    **profile,
                    "point_relevance_score": _point_relevance_score(rule, part),
                    "source_open": values["開盤"], "source_high": values["高點"],
                    "source_low": values["低點"], "source_close": values["收盤"],
                })
    rows.extend(_gap_levels(formation, cutoff, config))
    rows.extend(_dynamic_levels(formation, cutoff, config))
    rows.extend(_external_round_levels(formation, cutoff, config))
    raw = pd.DataFrame(rows)
    if raw.empty:
        return raw
    raw = raw.drop_duplicates("raw_level_id").sort_values(
        ["source_date", "level_value", "source_rule", "source_part"]
    ).reset_index(drop=True)
    return add_level_validity(raw, formation, cutoff, config)


def _rule_score(row: pd.Series, rule: str) -> float:
    if rule == "大量K":
        return max(_number(row.get("大量K分數")), 0.0)
    if rule.startswith("截至當日") and rule.endswith("日高"):
        return max(_number(row.get("波段高分數")), 0.0)
    if rule.startswith("截至當日") and rule.endswith("日低"):
        return max(_number(row.get("波段低分數")), 0.0)
    if rule in {"長紅K", "長黑K"}:
        return max(_number(row.get("長實體分數")), 0.0)
    if rule == "長上影":
        return max(_number(row.get("長上影分數")), 0.0)
    if rule == "長下影":
        return max(_number(row.get("長下影分數")), 0.0)
    return 1.0


def _gap_levels(formation: pd.DataFrame, cutoff: pd.Timestamp, config: dict[str, Any]) -> list[dict[str, Any]]:
    cmap = config["taiex"]["column_map"]
    date_col = config["date_column"]
    c = config["candidate"]
    result: list[dict[str, Any]] = []
    previous: pd.Series | None = None
    for _, current in formation.sort_values(date_col).iterrows():
        if previous is None:
            previous = current
            continue
        prev_close = _number(previous[cmap["close"]], np.nan)
        current_open = _number(current[cmap["open"]], np.nan)
        atr = _number(current.get("ATR"), np.nan)
        gap = current_open - prev_close
        threshold = max(float(c["minimum_gap_points"]), (atr * float(c["minimum_gap_atr"])) if np.isfinite(atr) else 0)
        if np.isfinite(gap) and abs(gap) >= threshold:
            source_date = pd.Timestamp(current[date_col]).normalize()
            direction = "向上跳空" if gap > 0 else "向下跳空"
            score = abs(gap) / atr if np.isfinite(atr) and atr else 1.0
            for part, level in (("昨收缺口緣", prev_close), ("今開缺口緣", current_open)):
                result.append({
                    "raw_level_id": _stable_id("TL", source_date.date(), direction, part, round(level, 4)),
                    "market": "TAIEX", "source_category": "STATIC_K_LEVEL",
                    "source_date": source_date,
                    "source_rule": direction, "source_part": part, "level_value": level,
                    "original_role": "BOTH",
                    "formation_score": score, "source_atr": atr,
                    "source_open": _number(current[cmap["open"]], np.nan),
                    "source_high": _number(current[cmap["high"]], np.nan),
                    "source_low": _number(current[cmap["low"]], np.nan),
                    "source_close": _number(current[cmap["close"]], np.nan),
                })
        previous = current
    return result


def _dynamic_levels(
    formation: pd.DataFrame, cutoff: pd.Timestamp, config: dict[str, Any]
) -> list[dict[str, Any]]:
    """動態指標只保存截止日當下值，不冒充歷史固定K棒點。"""
    if formation.empty:
        return []
    current = formation.iloc[-1]
    source_date = pd.Timestamp(current[config["date_column"]]).normalize()
    atr = _number(current.get("ATR"), np.nan)
    result = []
    for spec in config["taiex"].get("dynamic_fields", []):
        value = _number(current.get(spec["field"]), np.nan)
        if not np.isfinite(value):
            continue
        part = str(spec["kind"])
        result.append({
            "raw_level_id": _stable_id("TL", source_date.date(), "DYNAMIC", part, round(value, 4)),
            "market": "TAIEX", "source_category": "DYNAMIC_INDICATOR",
            "source_date": source_date, "source_rule": "當日動態指標",
            "source_part": part, "level_value": value,
            "original_role": _original_role(part), "formation_score": 1.0,
            "source_atr": atr, "source_open": _number(current.get(config["taiex"]["column_map"]["open"]), np.nan),
            "source_high": _number(current.get(config["taiex"]["column_map"]["high"]), np.nan),
            "source_low": _number(current.get(config["taiex"]["column_map"]["low"]), np.nan),
            "source_close": _number(current.get(config["taiex"]["column_map"]["close"]), np.nan),
        })
        for ratio in spec.get("deviation_ratios", []):
            ratio = float(ratio)
            derived = value * (1.0 + ratio)
            ratio_text = f"{ratio * 100:+g}%"
            derived_part = f"{part}{ratio_text}乖離點"
            result.append({
                "raw_level_id": _stable_id(
                    "TL", source_date.date(), "DYNAMIC_DEVIATION", derived_part, round(derived, 4)
                ),
                "market": "TAIEX", "source_category": "DYNAMIC_INDICATOR",
                "source_date": source_date, "source_rule": "均線乖離率推算",
                "source_part": derived_part, "level_value": derived,
                "original_role": "BOTH", "formation_score": 1.0,
                "source_atr": atr,
                "source_open": _number(current.get(config["taiex"]["column_map"]["open"]), np.nan),
                "source_high": _number(current.get(config["taiex"]["column_map"]["high"]), np.nan),
                "source_low": _number(current.get(config["taiex"]["column_map"]["low"]), np.nan),
                "source_close": _number(current.get(config["taiex"]["column_map"]["close"]), np.nan),
            })
    return result


def _external_round_levels(
    formation: pd.DataFrame, cutoff: pd.Timestamp, config: dict[str, Any]
) -> list[dict[str, Any]]:
    """另列選擇權常見整數關卡，不和K棒形成規則混為一談。"""
    if formation.empty:
        return []
    current = formation.iloc[-1]
    cmap = config["taiex"]["column_map"]
    close = _number(current.get(cmap["close"]), np.nan)
    if not np.isfinite(close):
        return []
    source_date = pd.Timestamp(current[config["date_column"]]).normalize()
    step = int(config["candidate"].get("external_round_step", 500))
    each_side = int(config["candidate"].get("external_round_levels_each_side", 3))
    center = round(close / step) * step
    rows = []
    for offset in range(-each_side, each_side + 1):
        level = float(center + offset * step)
        rows.append({
            "raw_level_id": _stable_id("TL", source_date.date(), "ROUND", level),
            "market": "TAIEX", "source_category": "EXTERNAL_MARKET",
            "source_date": source_date, "source_rule": "選擇權整數關卡",
            "source_part": f"{step}點整數關卡", "level_value": level,
            "original_role": "BOTH", "formation_score": 1.0,
            "source_atr": _number(current.get("ATR"), np.nan),
            "source_open": _number(current.get(cmap["open"]), np.nan),
            "source_high": _number(current.get(cmap["high"]), np.nan),
            "source_low": _number(current.get(cmap["low"]), np.nan),
            "source_close": close,
        })
    return rows


def add_level_validity(
    raw: pd.DataFrame, market: pd.DataFrame, cutoff: pd.Timestamp, config: dict[str, Any]
) -> pd.DataFrame:
    """逐點計算觸碰、穿越、突破及截至cutoff的有效度。"""
    if raw.empty:
        return raw
    date_col = config["date_column"]
    cmap = config["taiex"]["column_map"]
    frame = market.loc[pd.to_datetime(market[date_col]) <= cutoff].copy().sort_values(date_col)
    current_close = _number(frame.iloc[-1][cmap["close"]])
    current_atr = _number(frame.iloc[-1].get("ATR"), 1.0) or 1.0
    c = config["candidate"]
    out = []
    for item in raw.to_dict("records"):
        level = float(item["level_value"])
        atr = item.get("source_atr")
        atr = float(atr) if atr is not None and np.isfinite(atr) and atr else current_atr
        later = frame.loc[frame[date_col] > item["source_date"]]
        high = pd.to_numeric(later[cmap["high"]], errors="coerce")
        low = pd.to_numeric(later[cmap["low"]], errors="coerce")
        close = pd.to_numeric(later[cmap["close"]], errors="coerce")
        touch_buffer = atr * float(c["touch_buffer_atr"])
        break_buffer = atr * float(c["break_buffer_atr"])
        touched = (low <= level + touch_buffer) & (high >= level - touch_buffer)
        side = np.sign(close - level).replace(0, np.nan).ffill().fillna(0)
        crosses = int(side.diff().abs().eq(2).sum())
        above = int((close > level + break_buffer).sum())
        below = int((close < level - break_buffer).sum())
        recent = close.tail(int(c["confirmed_break_closes"]))
        confirmed = int(c["confirmed_break_closes"])
        above_now = len(recent) >= confirmed and (recent > level + break_buffer).all()
        below_now = len(recent) >= confirmed and (recent < level - break_buffer).all()
        role = str(item.get("original_role", "BOTH"))
        # 形成後若跨越過多次，代表該價格已成為雜訊區；優先標為過度測試。
        if crosses >= int(c.get("overtested_cross_count", 5)):
            status = "OVERTESTED"
        elif role == "RESISTANCE" and above_now:
            after_break_touch = bool((touched & (close > level - touch_buffer)).tail(max(5, confirmed)).any())
            status = "FLIPPED_TO_SUPPORT" if after_break_touch else "BROKEN_UP"
        elif role == "SUPPORT" and below_now:
            after_break_touch = bool((touched & (close < level + touch_buffer)).tail(max(5, confirmed)).any())
            status = "FLIPPED_TO_RESISTANCE" if after_break_touch else "BROKEN_DOWN"
        elif above_now:
            status = "ACTIVE_SUPPORT"
        elif below_now:
            status = "ACTIVE_RESISTANCE"
        elif current_close >= level:
            status = "ACTIVE_SUPPORT"
        else:
            status = "ACTIVE_RESISTANCE"
        age = int((frame[date_col] > item["source_date"]).sum())
        recency = math.pow(0.5, age / float(config["scoring"]["validity"]["age_half_life_rows"]))
        test_penalty = math.exp(-max(0, int(touched.sum()) - 1) * 0.12)
        cross_penalty = math.exp(-crosses * float(config["scoring"]["validity"]["cross_penalty"]))
        validity = (0.55 + 0.25 * recency + 0.20 * math.exp(-abs(current_close - level) / (8 * current_atr)))
        validity *= test_penalty * cross_penalty
        if status == "OVERTESTED":
            validity *= 0.2
        item.update({
            "as_of_date": cutoff, "age_trading_rows": age,
            "distance_from_current": level - current_close,
            "distance_from_current_atr": abs(level - current_close) / current_atr,
            "touch_count": int(touched.sum()), "cross_count": crosses,
            "close_above_count": above, "close_below_count": below,
            "current_status": status, "validity_score": validity,
        })
        out.append(item)
    result = pd.DataFrame(out)
    return result.reset_index(drop=True)


def map_levels_to_tx(raw: pd.DataFrame, futures: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """依同日、同OHLC部位映射到TX；缺口邊緣以同日價差平移。"""
    if raw.empty or futures.empty:
        return pd.DataFrame()
    date_col = config["date_column"]
    fmap = {x["kind"]: x["field"] for x in config["futures"]["mapped_fields"]}
    ti_map = {"開盤": "open", "高點": "high", "低點": "low", "收盤": "close"}
    tc = config["taiex"]["column_map"]
    tx = futures.drop_duplicates(date_col, keep="last").set_index(date_col)
    rows = []
    for item in raw.to_dict("records"):
        source_date = pd.Timestamp(item["source_date"])
        if source_date not in tx.index:
            continue
        source_part = item["source_part"]
        if source_part in ti_map and source_part in fmap:
            value = _number(tx.loc[source_date].get(fmap[source_part]), np.nan)
            method = "同日同部位TX全日K"
        else:
            tx_close = _number(tx.loc[source_date].get(fmap.get("收盤")), np.nan)
            taiex_close = _number(item.get("source_close"), np.nan)
            value = item["level_value"] + tx_close - taiex_close
            method = "同日收盤價差平移"
        if not np.isfinite(value):
            continue
        mapped = dict(item)
        mapped.update({
            "raw_level_id": _stable_id("XL", item["raw_level_id"], round(value, 4)),
            "parent_raw_level_id": item["raw_level_id"], "market": "TX",
            "level_value": float(value), "mapping_method": method,
        })
        rows.append(mapped)
    return pd.DataFrame(rows)


def cluster_levels(raw: pd.DataFrame, current_atr: float, config: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """中心距離＋最大寬度合併，避免單連結把數百點串成超大區域。"""
    if raw.empty:
        return pd.DataFrame(), pd.DataFrame()
    tolerance = max(
        float(config["candidate"]["cluster_tolerance_points"]),
        current_atr * float(config["candidate"]["cluster_tolerance_atr"]),
    )
    max_width = max(
        float(config["candidate"].get("cluster_max_width_points", 35)),
        current_atr * float(config["candidate"].get("cluster_max_width_atr", .1)),
    )
    zones, links = [], []
    for (market, category), group in raw.groupby(["market", "source_category"], sort=True):
        ordered = group.sort_values("level_value").reset_index(drop=True)
        buckets: list[list[dict[str, Any]]] = []
        for item in ordered.to_dict("records"):
            if not buckets:
                buckets.append([item])
                continue
            bucket = buckets[-1]
            center = float(np.median([x["level_value"] for x in bucket]))
            proposed_width = item["level_value"] - bucket[0]["level_value"]
            if abs(item["level_value"] - center) <= tolerance and proposed_width <= max_width:
                bucket.append(item)
            else:
                buckets.append([item])
        for bucket in buckets:
            table = pd.DataFrame(bucket)
            for column, default in (("shape_prior_score", 50.0), ("point_relevance_score", 60.0),
                                    ("direction_strength", 0.0), ("candle_shape", "其他"),
                                    ("direction_bias", "中性")):
                if column not in table:
                    table[column] = default
                table[column] = table[column].fillna(default)
            formation_norm = min(100.0, max(0.0, float(table["formation_score"].max())) / 3.0 * 100.0)
            validity_norm = min(100.0, max(0.0, float(table["validity_score"].max())) * 100.0)
            overlap_score = min(100.0, 25.0 + max(0, len(table) - 1) * 12.0
                                + max(0, table["source_rule"].nunique() - 1) * 15.0)
            scoring = config.get("importance_scoring", {})
            weights = scoring.get("weights", {
                "shape": .25, "point": .20, "formation": .20, "validity": .20, "overlap": .15,
            })
            importance = (
                float(table["shape_prior_score"].max()) * float(weights["shape"])
                + float(table["point_relevance_score"].max()) * float(weights["point"])
                + formation_norm * float(weights["formation"])
                + validity_norm * float(weights["validity"])
                + overlap_score * float(weights["overlap"])
            )
            thresholds = scoring.get("grade_thresholds", {"S": 85, "A": 70, "B": 55, "C": 40})
            grade = next((name for name in ("S", "A", "B", "C")
                          if importance >= float(thresholds[name])), "D")
            strongest = table.sort_values(
                ["point_relevance_score", "formation_score", "validity_score", "source_date"],
                ascending=[False, False, False, False]
            ).iloc[0]
            lower, upper = float(table["level_value"].min()), float(table["level_value"].max())
            zone_id = _stable_id("Z", market, category, round(lower, 4), round(upper, 4), *sorted(table["raw_level_id"]))
            rules = sorted(table["source_rule"].astype(str).unique())
            statuses = sorted(table["current_status"].astype(str).unique())
            zones.append({
                "zone_id": zone_id, "market": market,
                "source_category": category,
                "zone_lower": lower, "zone_upper": upper,
                "representative_level": float(strongest["level_value"]),
                "representative_method": "點位部位權重、形成分數與有效度綜合最高之原始市場點",
                "source_count": len(table), "rule_count": len(rules),
                "source_rules": "|".join(rules),
                "source_dates": "|".join(f"{pd.Timestamp(x):%Y-%m-%d}" for x in sorted(table["source_date"].unique())),
                "formation_score": float(table["formation_score"].max()),
                "validity_score": float(table["validity_score"].max()),
                "candle_shapes": "|".join(sorted(set(table["candle_shape"].astype(str)))),
                "direction_bias": str(strongest["direction_bias"]),
                "direction_strength": float(strongest["direction_strength"]),
                "shape_prior_score": float(table["shape_prior_score"].max()),
                "point_relevance_score": float(table["point_relevance_score"].max()),
                "overlap_score": overlap_score,
                "importance_score": round(max(0.0, min(100.0, importance)), 2),
                "importance_grade": grade,
                "current_status": str(strongest["current_status"]),
                "component_statuses": "|".join(statuses),
                "distance_from_current": float(strongest["distance_from_current"]),
                "zone_width": upper - lower,
            })
            links.extend({"zone_id": zone_id, "raw_level_id": x} for x in table["raw_level_id"])
    zone_frame = pd.DataFrame(zones).sort_values(
        ["market", "validity_score", "formation_score"], ascending=[True, False, False]
    ).reset_index(drop=True)
    zone_frame.insert(0, "zone_rank", zone_frame.groupby("market").cumcount() + 1)
    zone_frame.attrs["current_atr"] = current_atr
    return zone_frame, pd.DataFrame(links)


def _rank_static_candidates(zones: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """排序K棒候選；限制重複來源加分，並加重距離與有效性。"""
    if zones.empty:
        return zones.copy()
    settings = config["review"]
    selected = zones.loc[
        zones["market"].eq("TAIEX")
        & zones["source_category"].eq("STATIC_K_LEVEL")
        & ~zones["current_status"].isin(settings["excluded_statuses"])
        & zones["distance_from_current"].abs().le(
            float(settings["maximum_distance_atr"])
            * max(1.0, float(zones.attrs.get("current_atr", 1.0)))
        )
    ].copy()
    if selected.empty:
        return selected
    distance_atr = selected["distance_from_current"].abs() / max(1.0, float(zones.attrs.get("current_atr", 1.0)))
    selected["review_score"] = (
        np.log1p(selected["source_count"]).clip(upper=1.8) * 0.8
        + np.log1p(selected["rule_count"]).clip(upper=1.5) * 1.0
        + selected["formation_score"].clip(upper=3) * 0.8
        + selected["validity_score"].clip(upper=1.5) * 3.0
        + np.exp(-distance_atr / 2) * 3.0
        - distance_atr * 0.35
    )
    selected = selected.sort_values(
        ["review_score", "validity_score", "formation_score"], ascending=[False, False, False]
    ).reset_index(drop=True)
    return selected


def select_review_zones(
    zones: pd.DataFrame, config: dict[str, Any], limit: int | None = None
) -> pd.DataFrame:
    """選取K棒候選點位，現價上下先各取一半，再依總分補足。"""
    selected = _rank_static_candidates(zones, config)
    if selected.empty:
        return selected
    maximum = int(limit if limit is not None else config["review"]["maximum_zones"])
    half = maximum // 2
    below = selected.loc[selected["distance_from_current"].le(0)].head(half)
    above = selected.loc[selected["distance_from_current"].gt(0)].head(half)
    chosen_ids = set(below["zone_id"]) | set(above["zone_id"])
    remainder = selected.loc[~selected["zone_id"].isin(chosen_ids)].head(maximum - len(chosen_ids))
    selected = pd.concat([below, above, remainder], ignore_index=True).sort_values(
        ["review_score", "validity_score"], ascending=[False, False]
    ).head(maximum).reset_index(drop=True)
    selected["review_rank"] = np.arange(1, len(selected) + 1)
    return selected


def select_reference_levels(zones: pd.DataFrame, category: str, config: dict[str, Any]) -> pd.DataFrame:
    """均線／外部參考點另列，不與K棒候選競爭30個名額。"""
    if zones.empty:
        return zones.copy()
    result = zones.loc[
        zones["market"].eq("TAIEX")
        & zones["source_category"].eq(category)
        & ~zones["current_status"].isin(config["review"]["excluded_statuses"])
    ].copy()
    return result.sort_values("representative_level").reset_index(drop=True)


def build_review_template(zones: pd.DataFrame, cutoff: pd.Timestamp) -> pd.DataFrame:
    review = zones.copy()
    review.insert(0, "as_of_date", cutoff)
    review["is_key_level"] = ""
    review["is_trade_candidate"] = ""
    review["review_action"] = ""
    review["reason_codes"] = ""
    review["corrected_level"] = ""
    review["corrected_role"] = ""
    review["reviewer"] = "Yu"
    review["note"] = ""
    review["allowed_review_actions"] = REVIEW_ACTIONS
    review["allowed_reason_codes"] = REASON_CODES
    return _friendly_columns(review)


def _friendly_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """輸出檔改用直白欄名；內部與資料庫欄位仍保持相容。"""
    return frame.rename(columns={
        "zone_id": "相近點位組_id",
        "zone_lower": "相近點位最低值",
        "zone_upper": "相近點位最高值",
        "representative_level": "代表點位",
        "zone_width": "相近點位跨度",
        "source_count": "原始點位來源數",
        "rule_count": "形成規則數",
        "source_rules": "形成原因",
        "source_dates": "形成日期",
        "current_status": "目前狀態",
        "distance_from_current": "距目前收盤點數",
        "review_score": "審核排序分數",
        "review_rank": "審核順序",
    })


def build_missed_template(cutoff: pd.Timestamp) -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "as_of_date", "market", "missed_level", "role", "source_date",
        "source_rule", "reason_code", "note", "reviewer",
    ]).astype({"as_of_date": "object"}).assign(as_of_date=pd.Series(dtype="object"))


def _known_category(point: dict[str, Any]) -> str:
    cleaned_category = str(point.get("已整理點位分類", "")).strip()
    category_map = {
        "K棒固定點位": "STATIC_K_LEVEL",
        "均線或指標點位": "DYNAMIC_INDICATOR",
        "外部參考點位": "EXTERNAL_MARKET",
    }
    if cleaned_category in category_map:
        return category_map[cleaned_category]
    # 同一段原文常同時列收盤、MA與選擇權；優先使用這一個點自己的子句。
    text = str(point.get("Yu點位子句", "")).strip()
    if not text or text.lower() == "nan":
        text = str(point.get("明示點位類型", "")).strip()
    if not text or text.lower() == "nan":
        text = str(point.get("Yu原始依據", "")).strip()
    if any(token in text for token in ["MA", "均線", "乖離", "布林"]):
        return "DYNAMIC_INDICATOR"
    if any(token in text for token in ["選擇權", "整數關卡", "履約"]):
        return "EXTERNAL_MARKET"
    return "STATIC_K_LEVEL"


def _best_zone_match(zones: pd.DataFrame, point: float) -> tuple[str, float]:
    if zones.empty:
        return "", np.nan
    candidates = zones.copy()
    candidates["point_error"] = np.minimum(
        (candidates["zone_lower"] - point).abs(),
        (candidates["zone_upper"] - point).abs(),
    )
    inside = point >= candidates["zone_lower"]
    inside &= point <= candidates["zone_upper"]
    candidates.loc[inside, "point_error"] = 0.0
    best = candidates.sort_values("point_error").iloc[0]
    return str(best["zone_id"]), float(best["point_error"])


def validate_known_levels(
    taiex: pd.DataFrame, label_paths: list[str | Path], cutoff: pd.Timestamp,
    config: dict[str, Any]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """逐分析日重建候選，任何舊答案都不會看到其分析日後的K棒。"""
    if not label_paths:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    start = cutoff - pd.Timedelta(days=int(config["features"]["load_calendar_days"]))
    known = discovery.normalize_taiex_labels(label_paths, start, cutoff)
    if known.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    rows = []
    coverage_k_rows = []
    coverage_ks = [int(x) for x in config["review"].get("coverage_at_k", [10, 20, 30, 40, 50])]
    tolerance = float(config["known_label_validation"]["match_tolerance_points"])
    grouped_days = list(known.groupby("分析日期", sort=True))
    for day_number, (analysis_date, daily) in enumerate(grouped_days, start=1):
        analysis_date = pd.Timestamp(analysis_date).normalize()
        if day_number == 1 or day_number % 20 == 0 or day_number == len(grouped_days):
            print(f"[walk-forward] {day_number}/{len(grouped_days)}：重建 {analysis_date:%Y-%m-%d} 候選")
        market = taiex.loc[pd.to_datetime(taiex[config["date_column"]]) <= analysis_date].copy()
        raw = build_raw_taiex_levels(market, analysis_date, config)
        formation = stage1.build_formation_features(market, analysis_date, config)
        current_atr = _number(formation.iloc[-1].get("ATR"), 1.0) if not formation.empty else 1.0
        all_zones, _ = cluster_levels(raw, current_atr, config)
        review_zones = select_review_zones(all_zones, config)
        dynamic_refs = select_reference_levels(all_zones, "DYNAMIC_INDICATOR", config)
        external_refs = select_reference_levels(all_zones, "EXTERNAL_MARKET", config)
        static_daily = []
        for point in daily.to_dict("records"):
            category = _known_category(point)
            eligible_all = all_zones.loc[
                all_zones["market"].eq("TAIEX") & all_zones["source_category"].eq(category)
            ]
            if category == "DYNAMIC_INDICATOR":
                eligible_review = dynamic_refs
            elif category == "EXTERNAL_MARKET":
                eligible_review = external_refs
            else:
                eligible_review = review_zones
            all_id, all_error = _best_zone_match(eligible_all, float(point["Yu加權點位"]))
            review_id, review_error = _best_zone_match(eligible_review, float(point["Yu加權點位"]))
            rows.append({
                **point, "expected_source_category": category,
                "candidate_count_that_day": len(eligible_all),
                "review_count_that_day": len(eligible_review),
                "all_candidate_zone_id": all_id, "all_candidate_error": all_error,
                "all_candidate_covered": int(np.isfinite(all_error) and all_error <= tolerance),
                "review_zone_id": review_id, "review_error": review_error,
                "review_covered": int(np.isfinite(review_error) and review_error <= tolerance),
                "validation_cutoff": analysis_date,
            })
            if category == "STATIC_K_LEVEL":
                static_daily.append(point)
        for k in coverage_ks:
            selected_k = select_review_zones(all_zones, config, limit=k)
            covered = 0
            for point in static_daily:
                _, error = _best_zone_match(selected_k, float(point["Yu加權點位"]))
                covered += int(np.isfinite(error) and error <= tolerance)
            coverage_k_rows.append({
                "analysis_date": analysis_date, "k": k,
                "known_static_level_count": len(static_daily),
                "covered_count": covered,
            })
    detail = pd.DataFrame(rows)
    summary_rows = []
    for category, group in [("ALL", detail), *list(detail.groupby("expected_source_category"))]:
        summary_rows.append({
            "as_of_date": cutoff, "source_category": category,
            "known_level_count": len(group),
            "all_candidate_covered_count": int(group["all_candidate_covered"].sum()),
            "all_candidate_coverage_pct": round(100 * group["all_candidate_covered"].mean(), 2),
            "review_covered_count": int(group["review_covered"].sum()),
            "review_coverage_pct": round(100 * group["review_covered"].mean(), 2),
            "average_review_zones_per_day": round(float(group["review_count_that_day"].mean()), 2),
            "tolerance_points": tolerance,
            "validation_mode": "WALK_FORWARD_AS_OF_EACH_ANALYSIS_DATE",
            "important_note": "舊清單是部分高可信正例；未列入者不可視為負例",
        })
    summary = pd.DataFrame(summary_rows)
    coverage_at_k = pd.DataFrame(coverage_k_rows)
    if not coverage_at_k.empty:
        coverage_at_k = coverage_at_k.groupby("k", as_index=False).agg(
            known_static_level_count=("known_static_level_count", "sum"),
            covered_count=("covered_count", "sum"),
            analysis_day_count=("analysis_date", "nunique"),
        )
        coverage_at_k["coverage_pct"] = (
            100 * coverage_at_k["covered_count"] / coverage_at_k["known_static_level_count"].replace(0, np.nan)
        ).round(2)
        coverage_at_k.insert(0, "as_of_date", cutoff)
        coverage_at_k["important_note"] = "只計算K棒形成點位；用來選擇每天應給Yu審核幾個候選"
    return detail, summary, coverage_at_k


def _json_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if pd.isna(value):
        return None
    return value


def save_to_database(
    run_id: uuid.UUID, cutoff: pd.Timestamp, config: dict[str, Any],
    raw: pd.DataFrame, zones: pd.DataFrame, links: pd.DataFrame
) -> None:
    """寫入前須先執行20260923_create_yu_candidate_review.sql。"""
    schema = config.get("database", {}).get("schema", "research")
    if schema != "research":
        raise ValueError("V1只允許database.schema=research，避免動態SQL注入")
    generator = config.get("database", {}).get("generator_code", "YU_CANDIDATE_REVIEW_V1")
    with db.get_connection() as conn:
        existing = conn.execute(
            "SELECT run_id FROM research.yu_candidate_runs WHERE generator_code=%s AND generator_version=%s AND as_of_date=%s",
            (generator, int(config.get("version", 1)), cutoff.date()),
        ).fetchone()
        if existing:
            raise ValueError(f"此版本與截止日已入庫，run_id={existing[0]}；請勿重複寫入")
        conn.execute(
            "INSERT INTO research.yu_candidate_runs "
            "(run_id,generator_code,generator_version,as_of_date,source_data_max_date,config,raw_level_count,zone_count) "
            "VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s)",
            (run_id, generator, int(config.get("version", 1)), cutoff.date(), cutoff.date(),
             json.dumps(config, ensure_ascii=False), len(raw), len(zones)),
        )
        for row in raw.to_dict("records"):
            attrs = {k: _json_value(v) for k, v in row.items() if k not in {
                "raw_level_id", "market", "source_date", "source_rule", "source_part",
                "source_category", "original_role", "level_value", "formation_score",
                "validity_score", "current_status"
            }}
            conn.execute(
                "INSERT INTO research.yu_candidate_raw_levels "
                "(run_id,raw_level_id,market,source_date,source_rule,source_part,source_category,original_role,level_value,formation_score,validity_score,current_status,attributes) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
                (run_id, row["raw_level_id"], row["market"], pd.Timestamp(row["source_date"]).date(),
                 row["source_rule"], row["source_part"], row["source_category"], row["original_role"],
                 row["level_value"], row["formation_score"],
                 row["validity_score"], row["current_status"], json.dumps(attrs, ensure_ascii=False)),
            )
        for row in zones.to_dict("records"):
            conn.execute(
                "INSERT INTO research.yu_candidate_zones "
                "(run_id,zone_id,market,source_category,zone_lower,zone_upper,representative_level,source_count,rule_count,formation_score,validity_score,current_status,review_score,selected_for_review) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (run_id, row["zone_id"], row["market"], row["source_category"],
                 row["zone_lower"], row["zone_upper"],
                 row["representative_level"], row["source_count"], row["rule_count"],
                 row["formation_score"], row["validity_score"], row["current_status"],
                 _json_value(row.get("review_score")), bool(row.get("selected_for_review", False))),
            )
        for row in links.to_dict("records"):
            conn.execute(
                "INSERT INTO research.yu_candidate_zone_sources (run_id,zone_id,raw_level_id) VALUES (%s,%s,%s)",
                (run_id, row["zone_id"], row["raw_level_id"]),
            )
        conn.commit()


def analyze_as_of(
    taiex: pd.DataFrame, futures: pd.DataFrame, cutoff: pd.Timestamp, config: dict[str, Any]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """純函式入口；即使呼叫端傳入未來列，也會先裁切。"""
    date_col = config["date_column"]
    cutoff = pd.Timestamp(cutoff).normalize()
    taiex = taiex.loc[pd.to_datetime(taiex[date_col]) <= cutoff].copy()
    futures = futures.loc[pd.to_datetime(futures[date_col]) <= cutoff].copy()
    raw_taiex = build_raw_taiex_levels(taiex, cutoff, config)
    raw_tx = map_levels_to_tx(raw_taiex, futures, config)
    raw = pd.concat([raw_taiex, raw_tx], ignore_index=True, sort=False)
    formation = stage1.build_formation_features(taiex, cutoff, config)
    current_atr = _number(formation.iloc[-1].get("ATR"), 1.0) if not formation.empty else 1.0
    zones, links = cluster_levels(raw, current_atr, config)
    review_zones = select_review_zones(zones, config)
    selected_ids = set(review_zones["zone_id"]) if not review_zones.empty else set()
    zones["selected_for_review"] = zones["zone_id"].isin(selected_ids)
    score_map = review_zones.set_index("zone_id")["review_score"] if not review_zones.empty else pd.Series(dtype=float)
    zones["review_score"] = zones["zone_id"].map(score_map)
    return raw, zones, links, review_zones


def run_generate(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    cutoff = pd.Timestamp(args.date).normalize()
    taiex, futures = stage1.load_reports(config, cutoff)
    raw, zones, links, review_zones = analyze_as_of(taiex, futures, cutoff, config)
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    stamp = f"{cutoff:%Y%m%d}"
    review = build_review_template(review_zones, cutoff)
    dynamic_references = _friendly_columns(
        select_reference_levels(zones, "DYNAMIC_INDICATOR", config)
    )
    external_references = _friendly_columns(
        select_reference_levels(zones, "EXTERNAL_MARKET", config)
    )
    missed = build_missed_template(cutoff)
    known_detail, known_summary, coverage_at_k = validate_known_levels(
        taiex, args.known_labels or [], cutoff, config
    )
    files = {
        f"yu_candidate_raw_levels_{stamp}.csv": raw,
        f"yu_all_candidate_groups_{stamp}.csv": _friendly_columns(zones),
        f"yu_candidate_group_sources_{stamp}.csv": links.rename(columns={"zone_id": "相近點位組_id"}),
        f"yu_kbar_candidate_review_{stamp}.csv": review,
        f"yu_indicator_reference_levels_{stamp}.csv": dynamic_references,
        f"yu_external_reference_levels_{stamp}.csv": external_references,
        f"yu_candidate_missed_levels_{stamp}.csv": missed,
    }
    if not known_detail.empty:
        files[f"yu_candidate_known_coverage_detail_{stamp}.csv"] = known_detail
        files[f"yu_candidate_known_coverage_summary_{stamp}.csv"] = known_summary
        files[f"yu_static_coverage_at_k_{stamp}.csv"] = coverage_at_k
    for name, frame in files.items():
        v1._write_csv(frame, output / name)
    run_id = uuid.uuid4()
    if args.save_db:
        save_to_database(run_id, cutoff, config, raw, zones, links)
    manifest = {
        "run_id": str(run_id), "generator": config["name"], "generator_version": config.get("version", 1),
        "as_of_date": f"{cutoff:%Y-%m-%d}", "source_data_max_date": f"{cutoff:%Y-%m-%d}",
        "future_data_policy": "所有行情、形成、有效性與映射都先裁切至as_of_date",
        "label_policy": "舊Yu清單只作部分高可信正例覆蓋率；未出現候選不是負例",
        "raw_level_count": len(raw), "candidate_group_count": len(zones),
        "kbar_review_count": len(review_zones),
        "indicator_reference_count": len(dynamic_references),
        "external_reference_count": len(external_references),
        "taiex_zone_count": int(zones["market"].eq("TAIEX").sum()) if not zones.empty else 0,
        "tx_zone_count": int(zones["market"].eq("TX").sum()) if not zones.empty else 0,
        "known_coverage": known_summary.to_dict("records") if not known_summary.empty else None,
        "saved_to_database": bool(args.save_db), "config": config,
    }
    (output / f"yu_candidate_run_manifest_{stamp}.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    print(
        f"截止日：{cutoff:%Y-%m-%d}；原始候選 {len(raw)}；"
        f"相近點位組 {len(zones)}；K棒審核點位組 {len(review_zones)}；"
        f"均線/指標參考 {len(dynamic_references)}；外部參考 {len(external_references)}"
    )
    if not known_summary.empty:
        print(known_summary.to_string(index=False))
    if args.save_db:
        print(f"資料庫 run_id：{run_id}")
    print(f"輸出位置：{output.resolve()}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Yu關鍵點位候選產生與人工審核V3")
    parser.add_argument("--config", required=True)
    parser.add_argument("--date", required=True, help="資料截止日；禁止使用此日之後資料")
    parser.add_argument("--known-labels", nargs="*", help="選填；既有Yu加權分析CSV，用於部分正例覆蓋率")
    parser.add_argument("--output-dir", default="../output/machine_learning/yu_candidate_review")
    parser.add_argument("--save-db", action="store_true", help="選填；先執行配套SQL後才可入庫")
    parser.set_defaults(func=run_generate)
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
