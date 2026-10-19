"""通用的期貨收盤方向預測、走勢回測與 PostgreSQL 留存框架。

先執行 machine_learning/sql/create_ml_prediction_schema.sql，再由 code 目錄執行：

    python -m machine_learning.futures_direction_model register --config machine_learning/configs/tx_close_direction_v1.json
    python -m machine_learning.futures_direction_model predict  --config ... --date 2026-08-28
    python -m machine_learning.futures_direction_model backtest --config ... --start 2025-01-01 --end 2026-08-28
    python -m machine_learning.futures_direction_model performance --model-key tx_close_direction

日期 D 的特徵固定取 D 前一筆交易日資料，訓練答案只允許 target_date < D，
因此不會把 D 日收盤或任何更晚資料洩漏進模型。

長區間回測按日曆月 checkpoint：每月成功後立即 commit；相同模型版本、
run_type 與日期區間再次執行時，自動沿用原 run_id 並跳過已保存日期。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import traceback
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from psycopg.types.json import Jsonb
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from common import db


SAFE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _json_default(value: Any):
    if isinstance(value, (pd.Timestamp, date)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Decimal):
        return float(value)
    if pd.isna(value):
        return None
    raise TypeError(f"無法轉成 JSON：{type(value)!r}")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        config = json.load(fh)
    required = {
        "model_key", "model_name", "target_market", "target_name", "version_no",
        "algorithm", "source_relation", "date_column", "entity_filter",
        "feature_columns", "target_config", "model_params", "training_config",
    }
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError(f"模型設定缺少欄位：{missing}")
    if not config["feature_columns"]:
        raise ValueError("feature_columns 不可為空")
    if config["algorithm"] != "random_forest":
        raise ValueError("第一版支援的 algorithm 為 random_forest")
    return config


def quote_relation(name: str) -> str:
    parts = name.split(".")
    if not 1 <= len(parts) <= 2 or any(not SAFE_NAME.fullmatch(part) for part in parts):
        raise ValueError(f"不安全的 relation 名稱：{name}")
    return ".".join(f'"{part}"' for part in parts)


def quote_column(name: str) -> str:
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError(f"不合法的欄位名稱：{name!r}")
    return '"' + name.replace('"', '""') + '"'


def register_model(config: dict[str, Any]) -> str:
    snapshot = canonical_json(config)
    config_hash = hashlib.sha256(snapshot.encode("utf-8")).hexdigest()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ml_model
                    (model_key, model_name, description, target_market, target_name)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (model_key) DO UPDATE SET
                    model_name = EXCLUDED.model_name,
                    description = EXCLUDED.description,
                    target_market = EXCLUDED.target_market,
                    target_name = EXCLUDED.target_name,
                    updated_at = now()
                RETURNING model_id
                """,
                (config["model_key"], config["model_name"], config.get("description"),
                 config["target_market"], config["target_name"]),
            )
            model_id = cur.fetchone()[0]
            cur.execute(
                "SELECT model_version_id, config_hash FROM ml_model_version WHERE model_id=%s AND version_no=%s",
                (model_id, config["version_no"]),
            )
            old = cur.fetchone()
            if old and old[1] != config_hash:
                raise ValueError(
                    f"版本 {config['version_no']} 已存在但設定不同；請增加 version_no，勿覆蓋歷史版本"
                )
            if old:
                version_id = old[0]
            else:
                cur.execute(
                    """
                    INSERT INTO ml_model_version
                        (model_id, version_no, algorithm, source_relation, entity_filter,
                         feature_columns, target_config, model_params, training_config,
                         config_snapshot, config_hash, code_version)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING model_version_id
                    """,
                    (model_id, config["version_no"], config["algorithm"], config["source_relation"],
                     Jsonb(config["entity_filter"]), Jsonb(config["feature_columns"]),
                     Jsonb(config["target_config"]), Jsonb(config["model_params"]),
                     Jsonb(config["training_config"]), Jsonb(config), config_hash,
                     config.get("code_version")),
                )
                version_id = cur.fetchone()[0]
        conn.commit()
    return str(version_id)


def load_source(config: dict[str, Any], through_date: date) -> pd.DataFrame:
    date_col = config["date_column"]
    price_col = config["target_config"]["price_column"]
    columns = list(dict.fromkeys([date_col, price_col, *config["feature_columns"]]))
    where = [f"{quote_column(date_col)} <= ?"]
    params: list[Any] = [through_date]
    for col, value in config["entity_filter"].items():
        where.append(f"{quote_column(col)} = ?")
        params.append(value)
    sql = (
        f"SELECT {', '.join(quote_column(c) for c in columns)} "
        f"FROM {quote_relation(config['source_relation'])} "
        f"WHERE {' AND '.join(where)} ORDER BY {quote_column(date_col)}"
    )
    frame = db.query_to_df(sql, params)
    if frame.empty:
        raise RuntimeError("來源 view 查無資料")
    absent = sorted(set(columns) - set(frame.columns))
    if absent:
        raise RuntimeError(f"來源 view 缺少設定欄位：{absent}")
    frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce")
    frame = frame.dropna(subset=[date_col]).sort_values(date_col).drop_duplicates(date_col, keep="last")
    frame[price_col] = pd.to_numeric(frame[price_col], errors="coerce")
    return frame.reset_index(drop=True)


def make_samples(raw: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    """每列特徵預測下一筆交易日；答案是下一日收盤相對本日收盤。"""
    date_col = config["date_column"]
    price_col = config["target_config"]["price_column"]
    threshold = float(config["target_config"].get("flat_threshold_pct", 0.0))
    out = raw.copy()
    out["feature_date"] = out[date_col]
    out["target_date"] = out[date_col].shift(-1)
    out["target_price"] = out[price_col].shift(-1)
    out["actual_return_pct"] = (out["target_price"] / out[price_col] - 1.0) * 100.0
    out["actual_class"] = np.select(
        [out["actual_return_pct"] > threshold, out["actual_return_pct"] < -threshold],
        [1, -1], default=0,
    ).astype("int8")
    out.loc[out["target_date"].isna() | out["target_price"].isna() | out[price_col].isna(), "actual_class"] = np.nan
    return out


def normalize_feature_frames(
    train_frame: pd.DataFrame,
    predict_frame: pd.DataFrame,
    features: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """統一 PostgreSQL numeric/NULL 與文字欄位型別。

    psycopg 會將 PostgreSQL numeric 讀成 decimal.Decimal；含 NULL 時 pandas
    常把整欄判成 object。若直接送進類別前處理器，Decimal 與 None 排序便會
    出現 ``'<' not supported``。欄位型別只以訓練集判斷，避免偷看未來值。
    """
    train_out = train_frame.copy()
    predict_out = predict_frame.copy()
    for col in features:
        original = train_out[col]
        converted = pd.to_numeric(original, errors="coerce")
        non_null_count = int(original.notna().sum())
        numeric_count = int(converted.notna().sum())
        # 非空訓練值全都可轉數字時，視為真正的數值欄；NULL 保留為 NaN。
        is_numeric = non_null_count > 0 and numeric_count == non_null_count
        if is_numeric:
            train_out[col] = converted.astype(float)
            predict_out[col] = pd.to_numeric(predict_out[col], errors="coerce").astype(float)
        else:
            # 真正的類別欄統一成字串，缺值必須是 np.nan，不可混入 None。
            train_out[col] = original.map(lambda value: np.nan if pd.isna(value) else str(value))
            predict_out[col] = predict_out[col].map(
                lambda value: np.nan if pd.isna(value) else str(value)
            )
    return train_out, predict_out


def _build_pipeline(frame: pd.DataFrame, features: list[str], config: dict[str, Any]) -> Pipeline:
    numeric = [c for c in features if pd.api.types.is_numeric_dtype(frame[c])]
    categorical = [c for c in features if c not in numeric]
    transformers = []
    if numeric:
        transformers.append(("numeric", SimpleImputer(strategy="median"), numeric))
    if categorical:
        transformers.append(("categorical", Pipeline([
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]), categorical))
    params = dict(config["model_params"])
    params["random_state"] = int(config["training_config"].get("random_seed", 42))
    return Pipeline([
        ("prepare", ColumnTransformer(transformers, remainder="drop")),
        ("model", RandomForestClassifier(**params)),
    ])


def _feature_snapshot(row: pd.Series, features: list[str]) -> dict[str, Any]:
    result = {}
    for col in features:
        value = row[col]
        if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
            result[col] = None
        elif isinstance(value, (pd.Timestamp, date)):
            result[col] = value.isoformat()
        elif isinstance(value, np.generic):
            result[col] = value.item()
        elif isinstance(value, Decimal):
            result[col] = float(value)
        else:
            result[col] = value
    return result


@dataclass
class Prediction:
    target_date: date
    feature_date: date
    trained_through_date: date
    predicted_class: int
    probabilities: dict[int, float]
    feature_values: dict[str, Any]
    actual_class: int | None
    actual_return_pct: float | None


def predict_dates(config: dict[str, Any], requested_dates: list[date]) -> tuple[list[Prediction], date | None, date | None]:
    if not requested_dates:
        return [], None, None
    raw = load_source(config, max(requested_dates))
    samples = make_samples(raw, config)
    date_col = config["date_column"]
    features = config["feature_columns"]
    labels = config["target_config"].get("labels", {"-1": "跌", "0": "平", "1": "漲"})
    minimum = int(config["training_config"].get("minimum_training_rows", 300))
    maximum = config["training_config"].get("maximum_training_rows")
    predictions: list[Prediction] = []

    for target in sorted(set(requested_dates)):
        target_ts = pd.Timestamp(target)
        train = samples[(samples["target_date"] < target_ts) & samples["actual_class"].notna()].copy()
        if maximum:
            train = train.tail(int(maximum))
        if len(train) < minimum:
            raise RuntimeError(f"{target} 僅有 {len(train)} 筆已揭曉訓練資料，少於 minimum_training_rows={minimum}")

        realized = samples[samples["target_date"] == target_ts]
        if not realized.empty:
            feature_row = realized.iloc[-1]
            actual_class = int(feature_row["actual_class"]) if pd.notna(feature_row["actual_class"]) else None
            actual_return = float(feature_row["actual_return_pct"]) if pd.notna(feature_row["actual_return_pct"]) else None
        else:
            eligible = raw[raw[date_col] < target_ts]
            if eligible.empty:
                raise RuntimeError(f"{target} 之前沒有可用特徵資料")
            feature_row = eligible.iloc[-1]
            actual_class = None
            actual_return = None

        prediction_frame = pd.DataFrame([feature_row[features]])
        train_features, prediction_features = normalize_feature_frames(
            train[features], prediction_frame, features
        )
        model = _build_pipeline(train_features, features, config)
        model.fit(train_features, train["actual_class"].astype(int))
        predicted = int(model.predict(prediction_features)[0])
        model_classes = model.named_steps["model"].classes_
        probability_values = model.predict_proba(prediction_features)[0]
        probabilities = {int(k): float(v) for k, v in zip(model_classes, probability_values)}
        trained_through = pd.Timestamp(train["target_date"].max()).date()
        feature_date = pd.Timestamp(feature_row.get("feature_date", feature_row[date_col])).date()
        predictions.append(Prediction(
            target, feature_date, trained_through, predicted, probabilities,
            _feature_snapshot(feature_row, features), actual_class, actual_return,
        ))
        print(
            f"{target}：預測{labels[str(predicted)]}，信心 {probabilities.get(predicted, 0):.1%}；"
            f"特徵日 {feature_date}，訓練至 {trained_through}"
        )
    first_train = pd.Timestamp(samples[samples["target_date"] < pd.Timestamp(min(requested_dates))]["target_date"].min()).date()
    last_train = predictions[-1].trained_through_date if predictions else None
    return predictions, first_train, last_train


def begin_or_resume_run(
    config: dict[str, Any], version_id: str, run_type: str,
    requested_start: date, requested_end: date,
) -> str:
    """建立批次；相同模型版本、類型與日期區間則接續原批次。"""
    seed = int(config["training_config"].get("random_seed", 42))
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT run_id FROM ml_prediction_run
                   WHERE model_version_id=%s AND run_type=%s
                     AND requested_start_date=%s AND requested_end_date=%s
                   ORDER BY started_at DESC LIMIT 1""",
                (version_id, run_type, requested_start, requested_end),
            )
            row = cur.fetchone()
            if row:
                run_id = row[0]
                cur.execute(
                    """UPDATE ml_prediction_run
                       SET status='running', error_message=NULL, finished_at=NULL
                       WHERE run_id=%s""", (run_id,),
                )
            else:
                cur.execute(
                    """INSERT INTO ml_prediction_run
                       (model_version_id, run_type, requested_start_date, requested_end_date,
                        random_seed, runtime_params, status)
                       VALUES (%s,%s,%s,%s,%s,%s,'running') RETURNING run_id""",
                    (version_id, run_type, requested_start, requested_end, seed,
                     Jsonb({"checkpoint_unit": "month"})),
                )
                run_id = cur.fetchone()[0]
        conn.commit()
    return str(run_id)


def saved_target_dates(run_id: str) -> set[date]:
    frame = db.query_to_df(
        "SELECT target_date FROM ml_prediction WHERE run_id=? ORDER BY target_date",
        (run_id,),
    )
    if frame.empty:
        return set()
    return {pd.Timestamp(value).date() for value in frame["target_date"]}


def group_dates_by_month(dates: list[date]) -> list[list[date]]:
    """依年月分批，保留日期順序。"""
    groups: list[list[date]] = []
    for target in sorted(set(dates)):
        if not groups or (groups[-1][0].year, groups[-1][0].month) != (target.year, target.month):
            groups.append([])
        groups[-1].append(target)
    return groups


def missing_target_dates(dates: list[date], saved: set[date]) -> list[date]:
    """只回傳尚未保存的日期；即使某月僅存一部分也能精確續跑。"""
    return [target for target in dates if target not in saved]


def save_month_checkpoint(
    config: dict[str, Any], version_id: str, run_id: str,
    predictions: list[Prediction], train_start: date | None, train_end: date | None,
) -> int:
    """以一個交易儲存一整月；成功 commit 後即使後續中斷也會保留。"""
    labels = config["target_config"].get("labels", {"-1": "跌", "0": "平", "1": "漲"})
    with db.get_connection() as conn:
        try:
            with conn.cursor() as cur:
                for p in predictions:
                    cur.execute(
                        """INSERT INTO ml_prediction
                           (run_id, model_version_id, target_date, feature_date, trained_through_date,
                            predicted_class, predicted_label, probability_down, probability_flat,
                            probability_up, confidence, feature_values, actual_class, actual_label,
                            actual_return_pct, actual_updated_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                   CASE WHEN %s IS NULL THEN NULL ELSE now() END)
                           ON CONFLICT (run_id, target_date) DO NOTHING""",
                        (run_id, version_id, p.target_date, p.feature_date, p.trained_through_date,
                         p.predicted_class, labels[str(p.predicted_class)], p.probabilities.get(-1),
                         p.probabilities.get(0), p.probabilities.get(1), p.probabilities.get(p.predicted_class),
                         Jsonb(p.feature_values), p.actual_class,
                         labels[str(p.actual_class)] if p.actual_class is not None else None,
                         p.actual_return_pct, p.actual_class),
                    )
                cur.execute(
                    """UPDATE ml_prediction_run r SET
                           train_start_date = CASE
                               WHEN r.train_start_date IS NULL THEN %s
                               WHEN %s IS NULL THEN r.train_start_date
                               ELSE LEAST(r.train_start_date, %s) END,
                           train_end_date = CASE
                               WHEN r.train_end_date IS NULL THEN %s
                               WHEN %s IS NULL THEN r.train_end_date
                               ELSE GREATEST(r.train_end_date, %s) END,
                           row_count = x.row_count,
                           metrics = jsonb_build_object(
                               'realized_count', x.realized_count,
                               'correct_count', x.correct_count,
                               'accuracy', CASE WHEN x.realized_count=0 THEN NULL
                                                ELSE x.correct_count::double precision/x.realized_count END
                           ),
                           status='partial', error_message=NULL
                       FROM (
                           SELECT count(*)::integer AS row_count,
                                  count(actual_class)::integer AS realized_count,
                                  count(*) FILTER (WHERE is_correct)::integer AS correct_count
                           FROM ml_prediction WHERE run_id=%s
                       ) x
                       WHERE r.run_id=%s""",
                    (train_start, train_start, train_start, train_end, train_end, train_end,
                     run_id, run_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return len(predictions)


def mark_run_error(run_id: str, message: str) -> None:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ml_prediction_run SET status='partial', error_message=%s WHERE run_id=%s",
                (message[:4000], run_id),
            )
        conn.commit()


def finalize_run(run_id: str) -> dict[str, Any]:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """WITH x AS (
                       SELECT count(*)::integer AS row_count,
                              count(actual_class)::integer AS realized_count,
                              count(*) FILTER (WHERE is_correct)::integer AS correct_count
                       FROM ml_prediction WHERE run_id=%s
                   )
                   UPDATE ml_prediction_run r SET
                       row_count=x.row_count,
                       metrics=jsonb_build_object(
                           'realized_count', x.realized_count,
                           'correct_count', x.correct_count,
                           'accuracy', CASE WHEN x.realized_count=0 THEN NULL
                                            ELSE x.correct_count::double precision/x.realized_count END),
                       status='completed', error_message=NULL, finished_at=now()
                   FROM x WHERE r.run_id=%s
                   RETURNING r.metrics""",
                (run_id, run_id),
            )
            metrics = cur.fetchone()[0]
        conn.commit()
    return metrics


def available_target_dates(config: dict[str, Any], start: date, end: date) -> list[date]:
    raw = load_source(config, end)
    samples = make_samples(raw, config)
    mask = samples["target_date"].between(pd.Timestamp(start), pd.Timestamp(end)) & samples["actual_class"].notna()
    return [pd.Timestamp(x).date() for x in samples.loc[mask, "target_date"]]


def refresh_actuals(model_key: str) -> int:
    """用目前已入庫的日報資料回填先前 live/single 預測的實際結果。"""
    versions = db.query_to_df(
        """SELECT v.model_version_id, v.config_snapshot
           FROM ml_model_version v JOIN ml_model m USING (model_id)
           WHERE m.model_key=?""", (model_key,),
    )
    if versions.empty:
        raise RuntimeError(f"找不到 model_key={model_key}")
    updated = 0
    for _, version in versions.iterrows():
        version_id = version["model_version_id"]
        config = version["config_snapshot"]
        if isinstance(config, str):
            config = json.loads(config)
        pending = db.query_to_df(
            "SELECT DISTINCT target_date FROM ml_prediction WHERE model_version_id=? AND actual_class IS NULL ORDER BY target_date",
            (version_id,),
        )
        if pending.empty:
            continue
        raw = load_source(config, pd.Timestamp(pending["target_date"].max()).date())
        samples = make_samples(raw, config).dropna(subset=["target_date", "actual_class"])
        actual_by_date = {pd.Timestamp(row["target_date"]).date(): row for _, row in samples.iterrows()}
        labels = config["target_config"].get("labels", {"-1": "跌", "0": "平", "1": "漲"})
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                for target in pending["target_date"]:
                    target = pd.Timestamp(target).date()
                    row = actual_by_date.get(target)
                    if row is None:
                        continue
                    actual_class = int(row["actual_class"])
                    cur.execute(
                        """UPDATE ml_prediction SET actual_class=%s, actual_label=%s,
                                  actual_return_pct=%s, actual_updated_at=now()
                           WHERE model_version_id=%s AND target_date=%s AND actual_class IS NULL""",
                        (actual_class, labels[str(actual_class)], float(row["actual_return_pct"]),
                         version_id, target),
                    )
                    updated += cur.rowcount
            conn.commit()
    print(f"已回填 {updated} 筆實際結果")
    return updated


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="台指期收盤方向模型與走勢回測")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("register", "predict", "backtest"):
        p = sub.add_parser(name)
        p.add_argument("--config", required=True)
        if name == "predict":
            p.add_argument("--date", required=True, type=parse_date)
        elif name == "backtest":
            p.add_argument("--start", required=True, type=parse_date)
            p.add_argument("--end", required=True, type=parse_date)
    p = sub.add_parser("performance")
    p.add_argument("--model-key", required=True)
    p = sub.add_parser("refresh-actuals")
    p.add_argument("--model-key", required=True)
    p = sub.add_parser("delete")
    p.add_argument("--model-key", required=True)
    p.add_argument("--confirm", action="store_true")
    args = parser.parse_args(argv)

    if args.command == "performance":
        result = db.query_to_df("SELECT * FROM v_ml_model_performance WHERE model_key=? ORDER BY version_no", (args.model_key,))
        print(result.to_string(index=False))
        return 0
    if args.command == "refresh-actuals":
        refresh_actuals(args.model_key)
        return 0
    if args.command == "delete":
        result = db.query_single_value("SELECT ml_delete_model(?, ?)", (args.model_key, args.confirm))
        print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))
        if not args.confirm:
            print("以上只是預覽；確認無誤後加 --confirm 才會連動刪除。")
        return 0

    config = load_config(args.config)
    version_id = register_model(config)
    if args.command == "register":
        print(f"模型版本已登錄：{version_id}")
        return 0
    if args.command == "predict":
        dates = [args.date]
        run_type = "single"
        start = end = args.date
    else:
        if args.end < args.start:
            parser.error("--end 不可早於 --start")
        dates = available_target_dates(config, args.start, args.end)
        if not dates:
            raise RuntimeError("指定區間沒有已揭曉的交易日")
        run_type, start, end = "backtest", args.start, args.end
    run_id = begin_or_resume_run(config, version_id, run_type, start, end)
    saved = saved_target_dates(run_id)
    pending_dates = missing_target_dates(dates, saved)
    if saved:
        print(f"續跑批次 {run_id}：已保存 {len(saved)} 日，將自動跳過。")
    try:
        for month_dates in group_dates_by_month(pending_dates):
            predictions, train_start, train_end = predict_dates(config, month_dates)
            save_month_checkpoint(
                config, version_id, run_id, predictions, train_start, train_end
            )
            print(
                f"[月度存檔] {month_dates[0]:%Y-%m} 已保存 {len(predictions)} 日；"
                f"下次中斷可由下一個缺漏日期續跑。"
            )
    except Exception as exc:
        try:
            mark_run_error(run_id, str(exc))
        except Exception as mark_exc:
            print(f"[警告] 回測錯誤後無法更新批次狀態：{mark_exc}", file=sys.stderr)
        raise
    metrics = finalize_run(run_id)
    accuracy = metrics.get("accuracy") if isinstance(metrics, dict) else None
    if accuracy is not None:
        print(
            f"回測已揭曉 {metrics['realized_count']} 日，正確 {metrics['correct_count']} 日，"
            f"正確率 {accuracy:.2%}"
        )
    print(f"資料庫 run_id：{run_id}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[錯誤] {exc}", file=sys.stderr)
        if "--debug" in sys.argv:
            traceback.print_exc()
        raise SystemExit(1)
