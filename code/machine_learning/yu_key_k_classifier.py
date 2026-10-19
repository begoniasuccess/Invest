"""以 Yu 歷史明示日期為標籤，訓練「關鍵加權日 K」分類器。

從 code 目錄執行：

    python -m machine_learning.yu_key_k_classifier compare `
      --config machine_learning/configs/yu_key_k_classifier.json `
      --training ../output/machine_learningyu_key_k_formation/yu_k_formation_training_20260102_20260828.csv

評估嚴格依日期向前展開；某一測試 K 的模型只會接觸更早的 K 棒特徵。
標籤是回顧性標籤（截至 Yu 標註檔結束日是否曾被引用），輸出會明示此限制。
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
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    balanced_accuracy_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

SAFE_RELATION_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TARGET = "Yu曾標記關鍵K"


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    required = {"date_column", "database_sources", "feature_sets", "models", "validation"}
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError(f"關鍵K分類器設定缺少欄位：{missing}")
    return config


def quote_column(name: str) -> str:
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError(f"不合法欄位名稱：{name!r}")
    return '"' + name.replace('"', '""') + '"'


def quote_relation(name: str) -> str:
    parts = name.split(".")
    if not 1 <= len(parts) <= 2 or any(not SAFE_RELATION_PART.fullmatch(x) for x in parts):
        raise ValueError(f"不安全 relation：{name}")
    return ".".join(quote_column(x) for x in parts)


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig")


def load_training(path: str | Path, date_column: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    required = {date_column, TARGET}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"訓練 CSV 缺少欄位：{missing}")
    frame[date_column] = pd.to_datetime(frame[date_column], errors="coerce").dt.normalize()
    frame[TARGET] = pd.to_numeric(frame[TARGET], errors="coerce")
    frame = frame.dropna(subset=[date_column, TARGET]).copy()
    frame[TARGET] = frame[TARGET].astype(int)
    if not set(frame[TARGET].unique()).issubset({0, 1}):
        raise ValueError(f"{TARGET} 只能是 0/1")
    return frame.sort_values(date_column).drop_duplicates(date_column, keep="last").reset_index(drop=True)


def load_database_features(
    config: dict[str, Any], start: pd.Timestamp, end: pd.Timestamp
) -> pd.DataFrame:
    """只查形成日同日已知的日報欄位，再以日期合併。"""
    # 延遲載入，讓純 CSV 的模型測試不必先安裝 PostgreSQL 驅動。
    from common import db

    date_column = config["date_column"]
    merged: pd.DataFrame | None = None
    for source_name, source in config["database_sources"].items():
        fields = list(dict.fromkeys([date_column, *source["fields"]]))
        where = [f"{quote_column(date_column)} BETWEEN ? AND ?"]
        params: list[Any] = [start.date(), end.date()]
        for column, value in source.get("filters", {}).items():
            where.append(f"{quote_column(column)} = ?")
            params.append(value)
        statement = (
            f"SELECT {', '.join(quote_column(x) for x in fields)} "
            f"FROM {quote_relation(source['relation'])} WHERE {' AND '.join(where)} "
            f"ORDER BY {quote_column(date_column)}"
        )
        part = db.query_to_df(statement, params)
        if part.empty:
            print(f"[警告] {source_name} 查無資料，相關欄位將保持缺值")
            continue
        part[date_column] = pd.to_datetime(part[date_column], errors="coerce").dt.normalize()
        part = part.dropna(subset=[date_column]).drop_duplicates(date_column, keep="last")
        rename = {field: f"{source_name}_{field}" for field in source["fields"]}
        part = part.rename(columns=rename)[[date_column, *rename.values()]]
        merged = part if merged is None else merged.merge(part, on=date_column, how="outer")
    return pd.DataFrame({date_column: pd.date_range(start, end)}) if merged is None else merged


def build_dataset(training: pd.DataFrame, database: pd.DataFrame, date_column: str) -> pd.DataFrame:
    data = training.merge(database, on=date_column, how="left")
    # 日期本身不可當成連續點數輸入；只保留可泛化的日曆特徵。
    data["日曆_月份"] = data[date_column].dt.month.astype(str)
    data["日曆_星期"] = data[date_column].dt.dayofweek.astype(str)
    return data.sort_values(date_column).reset_index(drop=True)


def resolve_feature_sets(data: pd.DataFrame, config: dict[str, Any]) -> dict[str, list[str]]:
    resolved: dict[str, list[str]] = {}
    for name, spec in config["feature_sets"].items():
        columns: list[str] = []
        for group in spec["groups"]:
            if group == "formation":
                columns.extend(config["formation_features"])
            elif group == "calendar":
                columns.extend(["日曆_月份", "日曆_星期"])
            elif group in config["database_sources"]:
                columns.extend(f"{group}_{x}" for x in config["database_sources"][group]["fields"])
            else:
                raise ValueError(f"未知特徵群組：{group}")
        columns = list(dict.fromkeys(columns))
        missing = [x for x in columns if x not in data.columns]
        if missing:
            raise ValueError(f"特徵組 {name} 缺少欄位：{missing}")
        usable = [x for x in columns if data[x].notna().any()]
        dropped = sorted(set(columns) - set(usable))
        if dropped:
            print(f"[警告] {name} 排除全空欄位：{dropped}")
        resolved[name] = usable
    return resolved


def make_pipeline(data: pd.DataFrame, features: list[str], model_name: str, config: dict[str, Any]) -> Pipeline:
    numeric = [x for x in features if pd.api.types.is_numeric_dtype(data[x])]
    categorical = [x for x in features if x not in numeric]
    numeric_pipe = Pipeline([
        ("impute", SimpleImputer(strategy="median", keep_empty_features=True)),
        ("scale", StandardScaler()),
    ])
    categorical_pipe = Pipeline([
        ("impute", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ])
    preprocess = ColumnTransformer([
        ("number", numeric_pipe, numeric),
        ("category", categorical_pipe, categorical),
    ], remainder="drop")
    settings = config["models"][model_name]
    if settings["type"] == "logistic":
        estimator = LogisticRegression(
            C=float(settings.get("C", 1.0)), class_weight="balanced",
            max_iter=int(settings.get("max_iter", 3000)), random_state=42,
        )
    elif settings["type"] == "random_forest":
        estimator = RandomForestClassifier(
            n_estimators=int(settings.get("n_estimators", 500)),
            min_samples_leaf=int(settings.get("min_samples_leaf", 5)),
            max_depth=settings.get("max_depth"), class_weight="balanced_subsample",
            max_features=settings.get("max_features", "sqrt"), n_jobs=-1, random_state=42,
        )
    else:
        raise ValueError(f"不支援模型型態：{settings['type']}")
    return Pipeline([("preprocess", preprocess), ("model", estimator)])


def expanding_predictions(
    data: pd.DataFrame, features: list[str], model_name: str, config: dict[str, Any]
) -> pd.DataFrame:
    validation = config["validation"]
    minimum = int(validation["minimum_train_rows"])
    block = int(validation["test_block_rows"])
    rows: list[pd.DataFrame] = []
    for test_start in range(minimum, len(data), block):
        test_end = min(test_start + block, len(data))
        train, test = data.iloc[:test_start], data.iloc[test_start:test_end]
        if train[TARGET].nunique() < 2:
            continue
        pipeline = make_pipeline(train, features, model_name, config)
        pipeline.fit(train[features], train[TARGET])
        probability = pipeline.predict_proba(test[features])[:, 1]
        fold = test[[config["date_column"], TARGET]].copy()
        fold["預測為關鍵K機率"] = probability
        fold["預測標籤"] = (probability >= float(validation["classification_threshold"])).astype(int)
        fold["訓練截止日"] = train.iloc[-1][config["date_column"]]
        fold["特徵組"] = ""
        fold["模型"] = model_name
        rows.append(fold)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def safe_auc(y: pd.Series, probability: pd.Series) -> float:
    return float(roc_auc_score(y, probability)) if y.nunique() == 2 else math.nan


def summarize_predictions(predictions: pd.DataFrame, top_ks: list[int]) -> dict[str, Any]:
    y, pred, probability = predictions[TARGET], predictions["預測標籤"], predictions["預測為關鍵K機率"]
    row: dict[str, Any] = {
        "OOS樣本數": len(predictions), "OOS正例數": int(y.sum()),
        "ROC_AUC": safe_auc(y, probability),
        "Average_Precision": float(average_precision_score(y, probability)),
        "Accuracy": float(accuracy_score(y, pred)),
        "Balanced_Accuracy": float(balanced_accuracy_score(y, pred)),
        "Precision": float(precision_score(y, pred, zero_division=0)),
        "Recall": float(recall_score(y, pred, zero_division=0)),
    }
    ranked = predictions.sort_values("預測為關鍵K機率", ascending=False)
    for k in top_ks:
        sample = ranked.head(min(int(k), len(ranked)))
        row[f"Top{k}_命中率"] = float(sample[TARGET].mean()) if len(sample) else math.nan
        row[f"Top{k}_命中數"] = int(sample[TARGET].sum()) if len(sample) else 0
    return row


def permutation_importance_table(
    data: pd.DataFrame, features: list[str], model_name: str, config: dict[str, Any]
) -> pd.DataFrame:
    """在最後一段保留資料上做原始欄位 permutation importance。"""
    holdout = int(config["validation"]["importance_holdout_rows"])
    train, test = data.iloc[:-holdout], data.iloc[-holdout:]
    if train.empty or test[TARGET].nunique() < 2:
        return pd.DataFrame()
    pipeline = make_pipeline(train, features, model_name, config)
    pipeline.fit(train[features], train[TARGET])
    baseline = safe_auc(test[TARGET], pd.Series(pipeline.predict_proba(test[features])[:, 1]))
    rng = np.random.default_rng(42)
    rows = []
    for feature in features:
        losses = []
        for _ in range(int(config["validation"]["importance_repeats"])):
            shuffled = test[features].copy()
            shuffled[feature] = rng.permutation(shuffled[feature].to_numpy())
            score = safe_auc(test[TARGET], pd.Series(pipeline.predict_proba(shuffled)[:, 1]))
            losses.append(baseline - score)
        rows.append({"欄位": feature, "重要度_AUC下降": float(np.mean(losses)), "保留段基準AUC": baseline})
    return pd.DataFrame(rows).sort_values("重要度_AUC下降", ascending=False).reset_index(drop=True)


def compare(data: pd.DataFrame, feature_sets: dict[str, list[str]], config: dict[str, Any]):
    summaries, predictions = [], []
    for set_name, features in feature_sets.items():
        for model_name in config["models"]:
            result = expanding_predictions(data, features, model_name, config)
            if result.empty:
                continue
            result["特徵組"] = set_name
            row = summarize_predictions(result, config["validation"]["top_k"])
            row.update({"特徵組": set_name, "模型": model_name, "輸入欄位數": len(features)})
            summaries.append(row); predictions.append(result)
    summary = pd.DataFrame(summaries)
    if summary.empty:
        raise RuntimeError("沒有可評估結果；請降低 minimum_train_rows 或增加訓練資料")
    summary = summary.sort_values(["Average_Precision", "ROC_AUC"], ascending=False).reset_index(drop=True)
    all_predictions = pd.concat(predictions, ignore_index=True)
    best = summary.iloc[0]
    importance = permutation_importance_table(
        data, feature_sets[best["特徵組"]], best["模型"], config
    )
    importance.insert(0, "特徵組", best["特徵組"]); importance.insert(1, "模型", best["模型"])
    return summary, all_predictions, importance


def run_compare(args: argparse.Namespace) -> None:
    config = load_config(args.config); date_column = config["date_column"]
    training = load_training(args.training, date_column)
    database = load_database_features(config, training[date_column].min(), training[date_column].max())
    data = build_dataset(training, database, date_column)
    feature_sets = resolve_feature_sets(data, config)
    summary, predictions, importance = compare(data, feature_sets, config)
    output = Path(args.output_dir); output.mkdir(parents=True, exist_ok=True)
    stamp = f"{training[date_column].min():%Y%m%d}_{training[date_column].max():%Y%m%d}"
    _write_csv(summary, output / f"yu_key_k_model_comparison_{stamp}.csv")
    _write_csv(predictions, output / f"yu_key_k_oos_predictions_{stamp}.csv")
    _write_csv(importance, output / f"yu_key_k_best_feature_importance_{stamp}.csv")
    manifest = {
        "target": TARGET, "date_start": f"{training[date_column].min():%Y-%m-%d}",
        "date_end": f"{training[date_column].max():%Y-%m-%d}",
        "label_definition": "截至 Yu 標註檔結束日，該日K是否曾被Yu明示引用",
        "validation": "依日期 expanding walk-forward；特徵不使用測試日以後資料",
        "best_feature_set": summary.iloc[0]["特徵組"], "best_model": summary.iloc[0]["模型"],
        "warning": "期間末端未被標記的K可能仍在未來被Yu引用，暫定負例存在右設限。",
    }
    (output / f"yu_key_k_run_manifest_{stamp}.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(summary.to_string(index=False)); print(f"輸出位置：{output.resolve()}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Yu關鍵加權日K分類器")
    sub = parser.add_subparsers(dest="command", required=True)
    compare_parser = sub.add_parser("compare", help="比較多組input及模型的時間序列樣本外表現")
    compare_parser.add_argument("--config", required=True)
    compare_parser.add_argument("--training", required=True)
    compare_parser.add_argument("--output-dir", default="../output/machine_learningyu_key_k_classifier")
    compare_parser.set_defaults(func=run_compare)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv); args.func(args); return 0
    except Exception as exc:
        print(f"[錯誤] {exc}"); return 1


if __name__ == "__main__":
    raise SystemExit(main())
