"""Yu 台指期策略的候選點位比對與多空邏輯逆向分析。

從 code 目錄執行：

    python -m machine_learning.yu_strategy_model analyze ^
      --config machine_learning/configs/yu_strategy_v1.json ^
      --labels yu_futures_analysis_2026.xlsx ^
      --start 2026-01-02 --end 2026-08-28

本程式不以事後 1 分 K 猜 Yu 的事前決策。分析日期 D 只使用 D 日收盤後已存在
的日報資料；walk-forward 預測 D 時，方向模型只用 D 以前的 Yu 標籤訓練。
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
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.tree import DecisionTreeClassifier, export_text

from common import db


SAFE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DIRECTION_PATTERN = re.compile(r"(多|空|無)")
DATE_HINT_PATTERN = re.compile(r"(?<!\d)(\d{1,2})\s*[/.-]\s*(\d{1,2})(?!\d)")
CONTINUATION_PATTERN = re.compile(r"抱單|持倉|移動停利|續抱")


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        config = json.load(fh)
    required = {"report_relation", "date_column", "direction_features", "candidate"}
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError(f"設定檔缺少欄位：{missing}")
    return config


def quote_relation(name: str) -> str:
    parts = name.split(".")
    if not 1 <= len(parts) <= 2 or any(not SAFE_NAME.fullmatch(p) for p in parts):
        raise ValueError(f"不安全的 relation 名稱：{name}")
    return ".".join(f'"{part}"' for part in parts)


def quote_column(name: str) -> str:
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError(f"不合法的欄位名稱：{name!r}")
    return '"' + name.replace('"', '""') + '"'


def normalize_direction(value: Any, *, missing_label: str = "無") -> str:
    if value is None or pd.isna(value):
        return missing_label
    match = DIRECTION_PATTERN.search(str(value).strip())
    return match.group(1) if match else missing_label


def _as_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def load_yu_labels(path: str | Path, sheet_name: str | None = None) -> pd.DataFrame:
    frame = pd.read_excel(path, sheet_name=sheet_name or 0)
    required = {"分析日期", "分析方向", "交易方向", "進場點", "進場依據"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Yu Excel 缺少欄位：{missing}")
    frame["分析日期"] = pd.to_datetime(frame["分析日期"], errors="coerce").dt.normalize()
    frame = frame.dropna(subset=["分析日期"]).copy()
    frame["分析方向_標籤"] = frame["分析方向"].map(normalize_direction)
    frame["交易方向_標籤"] = frame["交易方向"].map(normalize_direction)
    frame["樣本型態"] = np.where(
        frame["進場依據"].fillna("").astype(str).str.contains(CONTINUATION_PATTERN),
        "既有持倉", "新交易計畫",
    )
    frame["進場點"] = pd.to_numeric(frame["進場點"], errors="coerce")
    if "方案序號" not in frame:
        frame["方案序號"] = frame.groupby("分析日期").cumcount() + 1
    return frame.sort_values(["分析日期", "方案序號"]).reset_index(drop=True)


def _required_report_columns(config: dict[str, Any]) -> list[str]:
    candidate = config["candidate"]
    columns = [config["date_column"], *config["direction_features"]]
    columns.extend(item["field"] for item in candidate["raw_fields"])
    columns.extend(item["field"] for item in candidate["current_fields"])
    columns.extend([candidate["reference_price_field"], candidate["volume_field"]])
    columns.extend(config.get("entity_filter", {}).keys())
    return list(dict.fromkeys(columns))


def load_report(config: dict[str, Any], start: date, end: date) -> pd.DataFrame:
    date_column = config["date_column"]
    columns = _required_report_columns(config)
    where = [f"{quote_column(date_column)} BETWEEN ? AND ?"]
    params: list[Any] = [start, end]
    for column, value in config.get("entity_filter", {}).items():
        where.append(f"{quote_column(column)} = ?")
        params.append(value)
    statement = (
        f"SELECT {', '.join(quote_column(c) for c in columns)} "
        f"FROM {quote_relation(config['report_relation'])} "
        f"WHERE {' AND '.join(where)} ORDER BY {quote_column(date_column)}"
    )
    frame = db.query_to_df(statement, params)
    if frame.empty:
        raise RuntimeError("期貨日報 view 查無資料")
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise RuntimeError(f"期貨日報 view 缺少設定欄位：{missing}")
    frame[date_column] = pd.to_datetime(frame[date_column], errors="coerce").dt.normalize()
    return frame.dropna(subset=[date_column]).drop_duplicates(date_column, keep="last").reset_index(drop=True)


def _candidate_row(
    analysis_date: pd.Timestamp,
    source_date: pd.Timestamp,
    price: Any,
    session: str,
    kind: str,
    source_field: str,
    source_kind: str,
    age_rows: int,
    reference_price: float,
    volume_ratio: float | None,
) -> dict[str, Any] | None:
    numeric = _as_number(price)
    if numeric is None or numeric <= 0:
        return None
    return {
        "分析日期": analysis_date,
        "候選點位": numeric,
        "盤別": session,
        "點位類型": kind,
        "來源欄位": source_field,
        "來源日期": source_date,
        "來源種類": source_kind,
        "距今交易列數": age_rows,
        "距參考收盤點數": numeric - reference_price,
        "距參考收盤絕對點數": abs(numeric - reference_price),
        "來源量比": volume_ratio,
    }


def generate_candidates(report: pd.DataFrame, analysis_date: pd.Timestamp, config: dict[str, Any]) -> pd.DataFrame:
    """以分析日當下可見的日報歷史產生候選點位，不使用未來行情。"""
    date_column = config["date_column"]
    settings = config["candidate"]
    history = report.loc[report[date_column] <= analysis_date].tail(int(settings["lookback_rows"])).copy()
    if history.empty or history.iloc[-1][date_column] != analysis_date:
        return pd.DataFrame()
    current = history.iloc[-1]
    reference = _as_number(current[settings["reference_price_field"]])
    if reference is None:
        return pd.DataFrame()
    current_volume = _as_number(current[settings["volume_field"]])
    rows: list[dict[str, Any]] = []

    for age, (_, source) in enumerate(history.iloc[::-1].iterrows()):
        source_volume = _as_number(source[settings["volume_field"]])
        ratio = source_volume / current_volume if source_volume is not None and current_volume else None
        by_session: dict[str, dict[str, float]] = {}
        for spec in settings["raw_fields"]:
            value = _as_number(source[spec["field"]])
            row = _candidate_row(
                analysis_date, source[date_column], value, spec["session"], spec["kind"],
                spec["field"], "歷史OHLC", age, reference, ratio,
            )
            if row:
                rows.append(row)
                by_session.setdefault(spec["session"], {})[spec["kind"]] = value
        for session, points in by_session.items():
            if all(k in points for k in ("開盤", "收盤")):
                rows.append(_candidate_row(
                    analysis_date, source[date_column], (points["開盤"] + points["收盤"]) / 2,
                    session, "實K中關", f"{session}開收中值", "衍生中關", age, reference, ratio,
                ))
            if all(k in points for k in ("高點", "低點")):
                rows.append(_candidate_row(
                    analysis_date, source[date_column], (points["高點"] + points["低點"]) / 2,
                    session, "全K中關", f"{session}高低中值", "衍生中關", age, reference, ratio,
                ))

    for spec in settings["current_fields"]:
        row = _candidate_row(
            analysis_date, analysis_date, current[spec["field"]], spec["session"], spec["kind"],
            spec["field"], "當日指標", 0, reference, 1.0,
        )
        if row:
            rows.append(row)

    for step in settings.get("round_steps", []):
        center = round(reference / step) * step
        for offset in range(-2, 3):
            price = center + offset * step
            rows.append(_candidate_row(
                analysis_date, analysis_date, price, "通用", f"{step}點整數關卡",
                f"round_{step}", "整數關卡", 0, reference, None,
            ))

    candidates = pd.DataFrame(row for row in rows if row is not None)
    if candidates.empty:
        return candidates
    candidates = candidates.drop_duplicates(
        ["候選點位", "盤別", "點位類型", "來源日期", "來源欄位"], keep="first"
    ).reset_index(drop=True)
    tolerance = float(settings.get("cluster_tolerance_points", 15))
    prices = candidates["候選點位"].to_numpy(float)
    candidates["附近重疊數"] = [int(np.sum(np.abs(prices - price) <= tolerance)) for price in prices]
    return candidates


def _reason_score(reason: str, candidate: pd.Series) -> int:
    score = 0
    kind = str(candidate["點位類型"])
    session = str(candidate["盤別"])
    aliases = {
        "開盤": ["開盤", "開"], "收盤": ["收盤", "收"], "高點": ["高點", "高"],
        "低點": ["低點", "低"], "實K中關": ["中關", "中值"], "全K中關": ["中關", "中值"],
        "5日均": ["5日均", "五日均"], "10日均": ["10日均", "十日均"],
        "5日均扣抵": ["扣抵"], "10日均扣抵": ["扣抵"],
    }
    if any(token in reason for token in aliases.get(kind, [kind])):
        score += 2
    if session in reason or (session == "日盤" and "日盤" in reason) or (session == "夜盤" and "夜盤" in reason):
        score += 1
    hints = DATE_HINT_PATTERN.findall(reason)
    source_date = pd.Timestamp(candidate["來源日期"])
    if any(source_date.month == int(month) and source_date.day == int(day) for month, day in hints):
        score += 3
    return score


def match_entry_to_candidate(plan: pd.Series, candidates: pd.DataFrame, config: dict[str, Any]) -> dict[str, Any]:
    base = {
        "分析日期": plan["分析日期"], "方案序號": plan.get("方案序號"),
        "分析方向": plan["分析方向_標籤"], "交易方向": plan["交易方向_標籤"],
        "樣本型態": plan["樣本型態"], "Yu進場點": plan["進場點"],
        "Yu進場依據": plan.get("進場依據"), "資料品質狀態": plan.get("資料品質狀態"),
    }
    if plan["樣本型態"] == "既有持倉":
        return {**base, "比對狀態": "排除：既有持倉，不是新選點"}
    entry = _as_number(plan["進場點"])
    if entry is None or candidates.empty:
        return {**base, "比對狀態": "無固定進場點或無候選"}
    work = candidates.copy()
    work["差距點數"] = (work["候選點位"] - entry).abs()
    reason = "" if pd.isna(plan.get("進場依據")) else str(plan.get("進場依據"))
    work["原文符合分數"] = work.apply(lambda row: _reason_score(reason, row), axis=1)
    work["比對排序分"] = work["差距點數"] - work["原文符合分數"] * 4 - np.minimum(work["附近重疊數"], 8)
    best = work.sort_values(["比對排序分", "差距點數", "距今交易列數"]).iloc[0]
    tolerance = float(config["candidate"]["match_tolerance_points"])
    status = "命中" if best["差距點數"] <= tolerance else "未命中：超過容許點數"
    return {
        **base, "比對狀態": status, "候選點位": best["候選點位"],
        "差距點數": best["差距點數"], "盤別": best["盤別"], "點位類型": best["點位類型"],
        "來源欄位": best["來源欄位"], "來源日期": best["來源日期"], "來源種類": best["來源種類"],
        "距今交易列數": best["距今交易列數"], "來源量比": best["來源量比"],
        "附近重疊數": best["附近重疊數"], "原文符合分數": best["原文符合分數"],
    }


def build_candidate_matches(labels: pd.DataFrame, report: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    result = []
    cache: dict[pd.Timestamp, pd.DataFrame] = {}
    for _, plan in labels.iterrows():
        analysis_date = plan["分析日期"]
        candidates = cache.setdefault(analysis_date, generate_candidates(report, analysis_date, config))
        result.append(match_entry_to_candidate(plan, candidates, config))
    return pd.DataFrame(result)


def summarize_candidate_matches(matches: pd.DataFrame) -> pd.DataFrame:
    hit = matches.loc[matches["比對狀態"] == "命中"].copy()
    if hit.empty:
        return pd.DataFrame(columns=["交易方向", "點位類型", "盤別", "命中筆數", "平均差距點數", "占該方向_pct"])
    summary = hit.groupby(["交易方向", "點位類型", "盤別"], dropna=False).agg(
        命中筆數=("Yu進場點", "size"), 平均差距點數=("差距點數", "mean"),
    ).reset_index()
    totals = hit.groupby("交易方向").size()
    summary["占該方向_pct"] = summary.apply(
        lambda row: round(row["命中筆數"] / totals[row["交易方向"]] * 100, 2), axis=1
    )
    return summary.sort_values(["交易方向", "命中筆數"], ascending=[True, False]).reset_index(drop=True)


def _normalize_features(train: pd.DataFrame, predict: pd.DataFrame, features: list[str]):
    train_out, predict_out = train.copy(), predict.copy()
    numeric, categorical = [], []
    for column in features:
        converted = pd.to_numeric(train_out[column], errors="coerce")
        observed = int(train_out[column].notna().sum())
        if observed and int(converted.notna().sum()) == observed:
            numeric.append(column)
            train_out[column] = converted.astype(float)
            predict_out[column] = pd.to_numeric(predict_out[column], errors="coerce").astype(float)
        else:
            categorical.append(column)
            # 全空欄位經 map 後 pandas 可能仍推斷成 float；明確轉 object，
            # 才能由類別補值器安全填入「缺值」。
            train_out[column] = train_out[column].map(
                lambda v: np.nan if pd.isna(v) else str(v)
            ).astype(object)
            predict_out[column] = predict_out[column].map(
                lambda v: np.nan if pd.isna(v) else str(v)
            ).astype(object)
    return train_out, predict_out, numeric, categorical


def _build_direction_model(train: pd.DataFrame, predict: pd.DataFrame, features: list[str], config: dict[str, Any]):
    train, predict, numeric, categorical = _normalize_features(train, predict, features)
    transforms = []
    if numeric:
        transforms.append(("數值", SimpleImputer(strategy="median", keep_empty_features=True), numeric))
    if categorical:
        transforms.append(("類別", Pipeline([
            # constant 可保留整段訓練期間完全沒有值的欄位，不會出現
            # "Skipping features without any observed values" 警告。
            ("補值", SimpleImputer(strategy="constant", fill_value="缺值", keep_empty_features=True)),
            ("編碼", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]), categorical))
    settings = config["direction_model"]
    model = Pipeline([
        ("prepare", ColumnTransformer(transforms, remainder="drop", verbose_feature_names_out=False)),
        ("tree", DecisionTreeClassifier(
            max_depth=int(settings["max_depth"]), min_samples_leaf=int(settings["min_samples_leaf"]),
            class_weight="balanced", random_state=int(settings.get("random_seed", 42)),
        )),
    ])
    return model, train, predict


def _decision_path_text(model: Pipeline, row: pd.DataFrame) -> str:
    prepare = model.named_steps["prepare"]
    tree = model.named_steps["tree"]
    transformed = prepare.transform(row)
    names = prepare.get_feature_names_out()
    nodes = tree.decision_path(transformed).indices
    parts = []
    for node in nodes:
        feature_index = tree.tree_.feature[node]
        if feature_index < 0:
            continue
        value = float(transformed[0, feature_index])
        threshold = float(tree.tree_.threshold[node])
        operator = "≤" if value <= threshold else ">"
        parts.append(f"{names[feature_index]} {operator} {threshold:.3g}（值 {value:.3g}）")
    return "；".join(parts)


def build_direction_samples(labels: pd.DataFrame, report: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    date_column = config["date_column"]
    # 同日多方案只保留方向一致的一個標籤；同日多空皆有則標成「無」，避免任意挑一邊。
    daily_rows = []
    for analysis_date, group in labels.groupby("分析日期"):
        row = {"分析日期": analysis_date}
        for target in ("分析方向_標籤", "交易方向_標籤"):
            directions = set(group[target].dropna()) - {"無"}
            row[target] = next(iter(directions)) if len(directions) == 1 else "無"
        daily_rows.append(row)
    daily = pd.DataFrame(daily_rows)
    merged = daily.merge(report, left_on="分析日期", right_on=date_column, how="inner")
    return merged.sort_values("分析日期").reset_index(drop=True)


def walk_forward_direction(samples: pd.DataFrame, config: dict[str, Any]) -> tuple[pd.DataFrame, str]:
    features = config["direction_features"]
    minimum = int(config["direction_model"]["minimum_training_rows"])
    output: list[dict[str, Any]] = []
    for index in range(len(samples)):
        current = samples.iloc[[index]]
        train_base = samples.iloc[:index]
        result: dict[str, Any] = {"分析日期": current.iloc[0]["分析日期"]}
        for target, prefix in (("分析方向_標籤", "看盤"), ("交易方向_標籤", "交易")):
            train = train_base.loc[train_base[target].isin(["多", "空", "無"])].copy()
            result[f"{prefix}實際"] = current.iloc[0][target]
            if len(train) < minimum or train[target].nunique() < 2:
                result[f"{prefix}預測"] = None
                result[f"{prefix}信心_pct"] = None
                result[f"{prefix}判斷路徑"] = f"訓練資料不足（{len(train)}/{minimum}）"
                continue
            model, normalized_train, normalized_current = _build_direction_model(train, current, features, config)
            model.fit(normalized_train[features], normalized_train[target])
            probabilities = model.predict_proba(normalized_current[features])[0]
            best = int(np.argmax(probabilities))
            result[f"{prefix}預測"] = str(model.classes_[best])
            result[f"{prefix}信心_pct"] = round(float(probabilities[best]) * 100, 2)
            result[f"{prefix}判斷路徑"] = _decision_path_text(model, normalized_current[features])
        output.append(result)

    rules = []
    for target, title in (("分析方向_標籤", "Yu 看盤方向"), ("交易方向_標籤", "Yu 交易方向")):
        train = samples.loc[samples[target].isin(["多", "空", "無"])].copy()
        if train[target].nunique() < 2:
            rules.append(f"[{title}] 標籤類別不足，無法建立規則。")
            continue
        model, normalized, _ = _build_direction_model(train, train.iloc[[-1]], features, config)
        model.fit(normalized[features], normalized[target])
        names = list(model.named_steps["prepare"].get_feature_names_out())
        rules.append(f"[{title}]\n" + export_text(model.named_steps["tree"], feature_names=names, decimals=3))
    return pd.DataFrame(output), "\n\n".join(rules)


def direction_metrics(results: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for prefix in ("看盤", "交易"):
        valid = results.dropna(subset=[f"{prefix}預測"])
        correct = int((valid[f"{prefix}預測"] == valid[f"{prefix}實際"]).sum())
        rows.append({
            "目標": prefix, "有效回測日": len(valid), "命中日": correct,
            "命中率_pct": round(correct / len(valid) * 100, 2) if len(valid) else None,
            "回測方法": "逐日 walk-forward；預測當日只使用更早的 Yu 標籤",
        })
    return pd.DataFrame(rows)


def predict_direction_for_date(
    samples: pd.DataFrame,
    report_row: pd.DataFrame,
    target_date: pd.Timestamp,
    config: dict[str, Any],
) -> pd.DataFrame:
    """用 target_date 以前的 Yu 標籤，判斷指定分析日的看盤及交易方向。"""
    features = config["direction_features"]
    minimum = int(config["direction_model"]["minimum_training_rows"])
    row: dict[str, Any] = {"分析日期": target_date}
    for target, prefix in (("分析方向_標籤", "看盤"), ("交易方向_標籤", "交易")):
        train = samples.loc[
            (samples["分析日期"] < target_date) & samples[target].isin(["多", "空", "無"])
        ].copy()
        if len(train) < minimum or train[target].nunique() < 2:
            row[f"{prefix}預測"] = None
            row[f"{prefix}信心_pct"] = None
            row[f"{prefix}判斷路徑"] = f"訓練資料不足（{len(train)}/{minimum}）"
            continue
        pipeline, normalized_train, normalized_row = _build_direction_model(train, report_row, features, config)
        pipeline.fit(normalized_train[features], normalized_train[target])
        probabilities = pipeline.predict_proba(normalized_row[features])[0]
        best = int(np.argmax(probabilities))
        row[f"{prefix}預測"] = str(pipeline.classes_[best])
        row[f"{prefix}信心_pct"] = round(float(probabilities[best]) * 100, 2)
        row[f"{prefix}判斷路徑"] = _decision_path_text(pipeline, normalized_row[features])
    return pd.DataFrame([row])


def candidate_rankings_for_date(
    target_date: pd.Timestamp, candidates: pd.DataFrame, historical_matches: pd.DataFrame, top_n: int = 30,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    prior = historical_matches.loc[
        (historical_matches["分析日期"] < target_date) & (historical_matches["比對狀態"] == "命中")
    ]
    frequency = prior.groupby(["交易方向", "點位類型", "盤別"]).size().rename("歷史選用次數")
    blocks = []
    for direction in ("多", "空"):
        block = candidates.copy()
        block["預測交易方向"] = direction
        if direction in frequency.index.get_level_values(0):
            direction_frequency = frequency.loc[direction].rename("歷史選用次數")
            block = block.join(direction_frequency, on=["點位類型", "盤別"])
        else:
            block["歷史選用次數"] = 0
        block["歷史選用次數"] = block["歷史選用次數"].fillna(0).astype(int)
        # 可解釋的 V1 排序：過往 Yu 常用來源、重疊支撐/壓力、離收盤距離與新鮮度。
        block["候選分數"] = (
            block["歷史選用次數"] * 8 + np.minimum(block["附近重疊數"], 10) * 2
            - block["距參考收盤絕對點數"] / 100 - block["距今交易列數"] / 10
        )
        signed = block["距參考收盤點數"]
        block["位置角色"] = np.where(
            signed < 0, "收盤下方支撐候選", np.where(signed > 0, "收盤上方壓力候選", "收盤同點")
        )
        blocks.append(block.nlargest(top_n, "候選分數"))
    return pd.concat(blocks, ignore_index=True).sort_values(["預測交易方向", "候選分數"], ascending=[True, False])


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    serializable = frame.copy()
    for column in serializable.columns:
        if pd.api.types.is_datetime64_any_dtype(serializable[column]):
            serializable[column] = serializable[column].dt.strftime("%Y-%m-%d")
    serializable.to_csv(path, index=False, encoding="utf-8-sig")


def run_analysis(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    labels = load_yu_labels(args.labels, config.get("labels_sheet"))
    start = pd.Timestamp(args.start).normalize()
    end = pd.Timestamp(args.end).normalize()
    labels = labels.loc[labels["分析日期"].between(start, end)].copy()
    if labels.empty:
        raise RuntimeError("指定期間沒有 Yu 分析資料")
    lookback_days = int(config["candidate"]["lookback_rows"]) * 3
    report = load_report(config, (start - pd.Timedelta(days=lookback_days)).date(), end.date())
    report[config["date_column"]] = pd.to_datetime(report[config["date_column"]]).dt.normalize()

    matches = build_candidate_matches(labels, report, config)
    summary = summarize_candidate_matches(matches)
    samples = build_direction_samples(labels, report, config)
    directions, rules = walk_forward_direction(samples, config)
    metrics = direction_metrics(directions)

    target_candidates = generate_candidates(report, end, config)
    rankings = candidate_rankings_for_date(end, target_candidates, matches, args.top)

    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    _write_csv(matches, output / "yu_candidate_matches.csv")
    _write_csv(summary, output / "yu_candidate_source_summary.csv")
    _write_csv(directions, output / "yu_direction_walk_forward.csv")
    _write_csv(metrics, output / "yu_direction_metrics.csv")
    _write_csv(rankings, output / f"yu_candidate_rankings_{end:%Y%m%d}.csv")
    (output / "yu_direction_rules.txt").write_text(rules, encoding="utf-8")

    hit_count = int((matches["比對狀態"] == "命中").sum())
    eligible = int(((matches["Yu進場點"].notna()) & (matches["樣本型態"] != "既有持倉")).sum())
    print(f"候選點位：{hit_count}/{max(eligible, 0)} 筆在容許範圍內找到來源")
    print(metrics.to_string(index=False))
    print(f"輸出位置：{output.resolve()}")
    if end not in set(report[config["date_column"]]):
        print(f"提醒：{end.date()} 沒有日報資料，因此該日候選排名為空。")


def run_predict(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    labels = load_yu_labels(args.labels, config.get("labels_sheet"))
    target = pd.Timestamp(args.date).normalize()
    first_label = labels["分析日期"].min()
    lookback_days = int(config["candidate"]["lookback_rows"]) * 3
    report = load_report(
        config, (first_label - pd.Timedelta(days=lookback_days)).date(), target.date()
    )
    report[config["date_column"]] = pd.to_datetime(report[config["date_column"]]).dt.normalize()
    report_row = report.loc[report[config["date_column"]] == target]
    if report_row.empty:
        raise RuntimeError(f"{target.date()} 沒有期貨日報資料，無法判斷")

    prior_labels = labels.loc[labels["分析日期"] < target].copy()
    historical_matches = build_candidate_matches(prior_labels, report, config)
    samples = build_direction_samples(prior_labels, report, config)
    prediction = predict_direction_for_date(samples, report_row, target, config)
    candidates = generate_candidates(report, target, config)
    rankings = candidate_rankings_for_date(target, candidates, historical_matches, args.top)

    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    _write_csv(prediction, output / f"yu_direction_prediction_{target:%Y%m%d}.csv")
    _write_csv(rankings, output / f"yu_candidate_rankings_{target:%Y%m%d}.csv")
    print(prediction.to_string(index=False))
    print(f"候選點位已輸出：{output.resolve()}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Yu 台指期候選點位與多空邏輯逆向分析")
    sub = parser.add_subparsers(dest="command", required=True)
    analyze = sub.add_parser("analyze", help="比對歷史候選點位並做多空 walk-forward 回測")
    analyze.add_argument("--config", required=True)
    analyze.add_argument("--labels", required=True, help="yu_futures_analysis_2026.xlsx 路徑")
    analyze.add_argument("--start", required=True)
    analyze.add_argument("--end", required=True)
    analyze.add_argument("--output-dir", default="../output/machine_learningyu_strategy_v1")
    analyze.add_argument("--top", type=int, default=30, help="每個方向輸出的候選點位數")
    analyze.set_defaults(func=run_analysis)
    predict = sub.add_parser("predict", help="用過往 Yu 紀錄判斷指定分析日的多空與候選點位")
    predict.add_argument("--config", required=True)
    predict.add_argument("--labels", required=True)
    predict.add_argument("--date", required=True)
    predict.add_argument("--output-dir", default="../output/machine_learningyu_strategy_v1")
    predict.add_argument("--top", type=int, default=30)
    predict.set_defaults(func=run_predict)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        args.func(args)
        return 0
    except Exception as exc:  # CLI 要提供短而明確的錯誤，詳細追蹤可由開發測試取得。
        print(f"[錯誤] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
