"""Yu 關鍵 K 第一階段：嚴格依指定截止日產生加權關鍵 K 與 TX 映射點位。

本程式不使用截止日後資料，也不使用 Yu 進場答案。第一階段只建立可稽核的
特徵快照與規則基準，供下一階段由 Yu 確認關鍵 K 後訓練監督式模型。
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from common import db
from machine_learning import yu_strategy_model as v1


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    required = {"date_column", "taiex", "futures", "features", "scoring", "output"}
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError(f"關鍵K第一階段設定缺少欄位：{missing}")
    return config


def _unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _load_relation(
    relation: str,
    columns: list[str],
    date_column: str,
    start: date,
    cutoff: date,
    filters: dict[str, Any] | None = None,
) -> pd.DataFrame:
    where = [f"{v1.quote_column(date_column)} BETWEEN ? AND ?"]
    params: list[Any] = [start, cutoff]
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
        raise RuntimeError(f"{relation} 截至 {cutoff} 查無資料")
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise RuntimeError(f"{relation} 缺少欄位：{missing}")
    frame[date_column] = pd.to_datetime(frame[date_column], errors="coerce").dt.normalize()
    return (
        frame.dropna(subset=[date_column])
        .loc[lambda x: x[date_column] <= pd.Timestamp(cutoff)]
        .drop_duplicates(date_column, keep="last")
        .sort_values(date_column)
        .reset_index(drop=True)
    )


def load_reports(config: dict[str, Any], cutoff: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame]:
    date_column = config["date_column"]
    history_days = int(config["features"]["load_calendar_days"])
    start = (cutoff - pd.Timedelta(days=history_days)).date()
    taiex_fields = _unique([date_column, *config["taiex"]["fields"]])
    futures_fields = _unique([
        date_column, *config["futures"].get("entity_filter", {}).keys(),
        *[item["field"] for item in config["futures"]["mapped_fields"]],
    ])
    taiex = _load_relation(
        config["taiex"]["relation"], taiex_fields, date_column, start, cutoff.date()
    )
    futures = _load_relation(
        config["futures"]["relation"], futures_fields, date_column, start, cutoff.date(),
        config["futures"].get("entity_filter"),
    )
    return taiex, futures


def _numeric(frame: pd.DataFrame, columns: Iterable[str]) -> None:
    for column in columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")


def _rolling_percentile(series: pd.Series, window: int, minimum: int) -> pd.Series:
    return series.rolling(window, min_periods=minimum).apply(
        lambda values: float(np.mean(values <= values[-1])) if np.isfinite(values[-1]) else np.nan,
        raw=True,
    )


def build_formation_features(
    taiex: pd.DataFrame,
    cutoff: pd.Timestamp,
    config: dict[str, Any],
) -> pd.DataFrame:
    """只使用每根 K 當日及更早資料，計算形成當時特徵。"""
    date_column = config["date_column"]
    fields = config["taiex"]["column_map"]
    frame = taiex.loc[taiex[date_column] <= cutoff].copy().sort_values(date_column).reset_index(drop=True)
    if frame.empty:
        return frame
    required = [fields[x] for x in ("open", "high", "low", "close", "volume")]
    _numeric(frame, required)
    open_ = frame[fields["open"]]
    high = frame[fields["high"]]
    low = frame[fields["low"]]
    close = frame[fields["close"]]
    volume = frame[fields["volume"]]
    previous_close = close.shift(1)
    true_range = pd.concat([
        high - low, (high - previous_close).abs(), (low - previous_close).abs()
    ], axis=1).max(axis=1)
    atr_window = int(config["features"]["atr_window"])
    atr = true_range.shift(1).rolling(atr_window, min_periods=max(3, atr_window // 4)).mean()
    safe_range = (high - low).replace(0, np.nan)
    body = close - open_
    body_abs = body.abs()
    upper_shadow = high - pd.concat([open_, close], axis=1).max(axis=1)
    lower_shadow = pd.concat([open_, close], axis=1).min(axis=1) - low

    frame["實體點數"] = body
    frame["實體絕對點數"] = body_abs
    frame["上影點數"] = upper_shadow
    frame["下影點數"] = lower_shadow
    frame["實體占振幅"] = body_abs / safe_range
    frame["上影占振幅"] = upper_shadow / safe_range
    frame["下影占振幅"] = lower_shadow / safe_range
    frame["實體_ATR倍數"] = body_abs / atr.replace(0, np.nan)
    frame["上影_ATR倍數"] = upper_shadow / atr.replace(0, np.nan)
    frame["下影_ATR倍數"] = lower_shadow / atr.replace(0, np.nan)
    frame["收盤位置"] = (close - low) / safe_range
    frame["ATR"] = atr
    frame["紅黑方向"] = np.select([body > 0, body < 0], ["紅K", "黑K"], default="十字")

    windows = [int(x) for x in config["features"]["windows"]]
    for window in windows:
        minimum = max(3, window // 3)
        prior_volume_mean = volume.shift(1).rolling(window, min_periods=minimum).mean()
        frame[f"量比_{window}"] = volume / prior_volume_mean.replace(0, np.nan)
        frame[f"成交量百分位_{window}"] = _rolling_percentile(volume, window, minimum)
        frame[f"是否截至當日{window}日新高"] = (
            high >= high.rolling(window, min_periods=minimum).max()
        ).astype(int)
        frame[f"是否截至當日{window}日新低"] = (
            low <= low.rolling(window, min_periods=minimum).min()
        ).astype(int)
        prior_high = high.shift(1).rolling(window, min_periods=minimum).max()
        prior_low = low.shift(1).rolling(window, min_periods=minimum).min()
        frame[f"高點突破前{window}日點數"] = high - prior_high
        frame[f"低點跌破前{window}日點數"] = low - prior_low
        frame[f"實體百分位_{window}"] = _rolling_percentile(body_abs, window, minimum)
        frame[f"上影百分位_{window}"] = _rolling_percentile(upper_shadow, window, minimum)
        frame[f"下影百分位_{window}"] = _rolling_percentile(lower_shadow, window, minimum)

    score = config["scoring"]
    volume_ratio = frame[f"量比_{int(score['volume_window'])}"].fillna(0)
    volume_percentile = frame[f"成交量百分位_{int(score['volume_window'])}"].fillna(0)
    pivot_windows = [int(x) for x in score["pivot_windows"]]
    pivot_high = sum(frame[f"是否截至當日{x}日新高"] for x in pivot_windows) / len(pivot_windows)
    pivot_low = sum(frame[f"是否截至當日{x}日新低"] for x in pivot_windows) / len(pivot_windows)
    body_strength = np.maximum(
        frame["實體_ATR倍數"].fillna(0) / float(score["long_body_atr"]),
        frame[f"實體百分位_{int(score['shape_percentile_window'])}"].fillna(0),
    ).clip(0, 1.5)
    upper_strength = np.maximum(
        frame["上影_ATR倍數"].fillna(0) / float(score["long_shadow_atr"]),
        frame[f"上影百分位_{int(score['shape_percentile_window'])}"].fillna(0),
    ).clip(0, 1.5)
    lower_strength = np.maximum(
        frame["下影_ATR倍數"].fillna(0) / float(score["long_shadow_atr"]),
        frame[f"下影百分位_{int(score['shape_percentile_window'])}"].fillna(0),
    ).clip(0, 1.5)
    frame["大量K分數"] = (
        (volume_ratio / float(score["large_volume_ratio"])).clip(0, 1.5) * 0.6
        + volume_percentile.clip(0, 1) * 0.4
    )
    frame["波段高分數"] = pivot_high
    frame["波段低分數"] = pivot_low
    frame["長實體分數"] = body_strength
    frame["長上影分數"] = upper_strength
    frame["長下影分數"] = lower_strength
    weights = score["formation_weights"]
    frame["形成重要度"] = (
        frame["大量K分數"] * float(weights["volume"])
        + np.maximum(frame["波段高分數"], frame["波段低分數"]) * float(weights["pivot"])
        + frame["長實體分數"] * float(weights["body"])
        + np.maximum(frame["長上影分數"], frame["長下影分數"]) * float(weights["shadow"])
    )
    frame["形成原因"] = frame.apply(lambda row: _formation_reasons(row, config), axis=1)
    return frame


def _formation_reasons(row: pd.Series, config: dict[str, Any]) -> str:
    score = config["scoring"]
    reasons = []
    volume_window = int(score["volume_window"])
    if row.get(f"量比_{volume_window}", 0) >= float(score["large_volume_ratio"]):
        reasons.append("大量K")
    longest = max(int(x) for x in score["pivot_windows"])
    if row.get(f"是否截至當日{longest}日新高", 0) == 1:
        reasons.append(f"截至當日{longest}日高")
    if row.get(f"是否截至當日{longest}日新低", 0) == 1:
        reasons.append(f"截至當日{longest}日低")
    if row.get("長實體分數", 0) >= 1:
        reasons.append("長紅棒" if row.get("實體點數", 0) > 0 else "長黑棒")
    if row.get("長上影分數", 0) >= 1:
        reasons.append("長上影")
    if row.get("長下影分數", 0) >= 1:
        reasons.append("長下影")
    return "＋".join(reasons) if reasons else "一般K"


def add_asof_validity(
    formation: pd.DataFrame,
    cutoff: pd.Timestamp,
    config: dict[str, Any],
) -> pd.DataFrame:
    """使用 K 形成日至截止日的已知資料，計算目前狀態與有效度。"""
    date_column = config["date_column"]
    fields = config["taiex"]["column_map"]
    frame = formation.loc[formation[date_column] <= cutoff].copy().reset_index(drop=True)
    if frame.empty:
        return frame
    current = frame.iloc[-1]
    current_close = float(current[fields["close"]])
    current_atr = float(current["ATR"]) if pd.notna(current["ATR"]) and current["ATR"] else 1.0
    validity = config["scoring"]["validity"]
    rows = []
    for index, row in frame.iterrows():
        later = frame.iloc[index + 1:]
        high_level = float(row[fields["high"]])
        low_level = float(row[fields["low"]])
        center = (high_level + low_level) / 2
        candidate_atr = float(row["ATR"]) if pd.notna(row["ATR"]) and row["ATR"] else current_atr
        buffer = candidate_atr * float(validity["break_buffer_atr"])
        later_close = pd.to_numeric(later[fields["close"]], errors="coerce")
        later_high = pd.to_numeric(later[fields["high"]], errors="coerce")
        later_low = pd.to_numeric(later[fields["low"]], errors="coerce")
        broke_up_count = int((later_close > high_level + buffer).sum())
        broke_down_count = int((later_close < low_level - buffer).sum())
        touch_buffer = candidate_atr * float(validity["touch_buffer_atr"])
        high_touches = (later_low <= high_level + touch_buffer) & (later_high >= high_level - touch_buffer)
        low_touches = (later_low <= low_level + touch_buffer) & (later_high >= low_level - touch_buffer)
        touches = high_touches | low_touches
        touch_indices = np.flatnonzero(touches.to_numpy())
        last_touch_age = int(len(later) - 1 - touch_indices[-1]) if len(touch_indices) else np.nan
        side = np.sign(later_close - center).replace(0, np.nan).ffill().fillna(0)
        crosses = int((side.diff().abs() == 2).sum())
        if current_close > high_level + buffer:
            status = "向上突破"
        elif current_close < low_level - buffer:
            status = "向下跌破"
        elif broke_up_count and broke_down_count:
            status = "雙向突破後回區間"
        elif broke_up_count:
            status = "曾向上突破後回區間"
        elif broke_down_count:
            status = "曾向下跌破後回區間"
        else:
            status = "仍在K棒區間"
        age = len(frame) - 1 - index
        distance_atr = abs(current_close - center) / current_atr
        decay = math.pow(0.5, age / float(validity["age_half_life_rows"]))
        proximity = math.exp(-distance_atr / float(validity["distance_decay_atr"]))
        break_penalty = math.exp(
            -(broke_up_count + broke_down_count) * float(validity["break_penalty"])
        )
        cross_penalty = math.exp(-crosses * float(validity["cross_penalty"]))
        effective = float(row["形成重要度"]) * (
            float(validity["formation_share"])
            + float(validity["recency_share"]) * decay
            + float(validity["proximity_share"]) * proximity
        ) * break_penalty * cross_penalty
        item = row.to_dict()
        item.update({
            "資料截止日": cutoff, "距今交易日數": age,
            "距目前收盤點數": center - current_close,
            "距目前收盤_ATR": distance_atr,
            "形成後向上突破收盤數": broke_up_count,
            "形成後向下跌破收盤數": broke_down_count,
            "形成後觸碰次數": int(touches.sum()),
            "距最近觸碰交易日": last_touch_age,
            "形成後中關穿越次數": crosses,
            "目前狀態": status,
            "目前有效度": effective,
        })
        rows.append(item)
    return pd.DataFrame(rows)


def select_key_k_candidates(features: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    settings = config["output"]
    eligible = features.loc[
        (features["形成重要度"] >= float(settings["minimum_formation_score"]))
        | (features["目前有效度"] >= float(settings["minimum_validity_score"]))
    ].copy()
    if eligible.empty:
        eligible = features.nlargest(int(settings["top_key_k"]), "目前有效度").copy()
    eligible = eligible.sort_values(
        ["目前有效度", "形成重要度", config["date_column"]], ascending=[False, False, False]
    ).head(int(settings["top_key_k"])).reset_index(drop=True)
    eligible.insert(0, "關鍵K排名", np.arange(1, len(eligible) + 1))
    return eligible


def map_key_k_to_tx(
    key_k: pd.DataFrame,
    futures: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    date_column = config["date_column"]
    tx_index = futures.set_index(date_column)
    rows = []
    for _, key in key_k.iterrows():
        source_date = key[date_column]
        if source_date not in tx_index.index:
            continue
        tx = tx_index.loc[source_date]
        sessions: dict[str, dict[str, float]] = {}
        for spec in config["futures"]["mapped_fields"]:
            value = pd.to_numeric(pd.Series([tx.get(spec["field"])]), errors="coerce").iloc[0]
            if pd.isna(value):
                continue
            sessions.setdefault(spec["session"], {})[spec["kind"]] = float(value)
            rows.append(_level_row(key, spec["session"], spec["kind"], spec["field"], value))
        for session, points in sessions.items():
            if {"開盤", "收盤"}.issubset(points):
                rows.append(_level_row(
                    key, session, "實K中關", f"{session}開收中值",
                    (points["開盤"] + points["收盤"]) / 2,
                ))
            if {"高點", "低點"}.issubset(points):
                rows.append(_level_row(
                    key, session, "全K中關", f"{session}高低中值",
                    (points["高點"] + points["低點"]) / 2,
                ))
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result["點位角色"] = result.apply(_level_role, axis=1)
    return result.sort_values(["關鍵K排名", "盤別", "點位類型"]).reset_index(drop=True)


def _level_row(key: pd.Series, session: str, kind: str, source_field: str, value: float) -> dict[str, Any]:
    return {
        "資料截止日": key["資料截止日"], "關鍵K排名": key["關鍵K排名"],
        "加權關鍵K日期": key["日期"], "關鍵K形成重要度": key["形成重要度"],
        "關鍵K目前有效度": key["目前有效度"], "關鍵K類型": key["形成原因"],
        "關鍵K目前狀態": key["目前狀態"], "盤別": session, "點位類型": kind,
        "TX來源欄位": source_field, "TX關鍵點位": float(value),
    }


def _level_role(row: pd.Series) -> str:
    kind = str(row["點位類型"])
    reason = str(row["關鍵K類型"])
    if kind == "高點":
        return "壓力候選"
    if kind == "低點":
        return "支撐候選"
    high_context = "日高" in reason or "長上影" in reason
    low_context = "日低" in reason or "長下影" in reason
    if high_context and not low_context:
        return "壓力候選"
    if low_context and not high_context:
        return "支撐候選"
    return "雙向關卡"


def analyze_as_of(
    taiex: pd.DataFrame,
    futures: pd.DataFrame,
    cutoff: pd.Timestamp,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """公開純函式入口；即使傳入未來列，也先裁切到 cutoff。"""
    date_column = config["date_column"]
    cutoff = pd.Timestamp(cutoff).normalize()
    taiex_cut = taiex.loc[pd.to_datetime(taiex[date_column]) <= cutoff].copy()
    futures_cut = futures.loc[pd.to_datetime(futures[date_column]) <= cutoff].copy()
    formation = build_formation_features(taiex_cut, cutoff, config)
    snapshot = add_asof_validity(formation, cutoff, config)
    key_k = select_key_k_candidates(snapshot, config)
    levels = map_key_k_to_tx(key_k, futures_cut, config)
    return snapshot, key_k, levels


def run(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    cutoff = pd.Timestamp(args.date).normalize()
    taiex, futures = load_reports(config, cutoff)
    snapshot, key_k, levels = analyze_as_of(taiex, futures, cutoff, config)
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    stamp = f"{cutoff:%Y%m%d}"
    v1._write_csv(snapshot, output / f"yu_key_k_feature_snapshot_{stamp}.csv")
    v1._write_csv(key_k, output / f"yu_key_k_candidates_{stamp}.csv")
    v1._write_csv(levels, output / f"yu_key_levels_{stamp}.csv")
    display = [
        "關鍵K排名", config["date_column"], "形成原因", "形成重要度", "目前有效度",
        "目前狀態", "距今交易日數", "距目前收盤點數",
    ]
    print(key_k[display].to_string(index=False))
    print(f"輸出位置：{output.resolve()}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Yu 關鍵K第一階段")
    parser.add_argument("--config", required=True)
    parser.add_argument("--date", required=True, help="資料截止日；程式禁止讀取此日之後資料")
    parser.add_argument("--output-dir", default="../output/machine_learningyu_key_k_stage1")
    parser.set_defaults(func=run)
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
