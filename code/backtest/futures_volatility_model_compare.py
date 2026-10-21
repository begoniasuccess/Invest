from __future__ import annotations

import argparse
import json
import math
import uuid
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import log_loss, mean_absolute_error, mean_squared_error, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from backtest.futures_raw_signal_strength_scan import Settings, _db_value, load_source, prepare_detail
from backtest.futures_volatility_strength_backtest import build_daily_score


ANALYSIS_NAME = "futures_volatility_model_compare"
METRIC_VERSION = "1.0.0"
FEATURES = (
    "布林低位強度", "散戶比偏高強度", "MA10斜率偏低強度", "主力淨多空OI偏低強度",
)
TARGETS = {
    "隔日日內振幅_pct": {"kind": "regression", "threshold": None},
    "振幅至少1.0pct": {"kind": "binary", "threshold": 1.0},
    "振幅至少1.5pct": {"kind": "binary", "threshold": 1.5},
}


def prepare_model_data(daily: pd.DataFrame) -> pd.DataFrame:
    data = pd.DataFrame({
        "訊號日": pd.to_datetime(daily["訊號日"]),
        "隔日日期": pd.to_datetime(daily["隔日日期"]),
        "波動分數": pd.to_numeric(daily["波動分數"], errors="coerce"),
        "布林低位強度": 100.0 - pd.to_numeric(daily["布林通道位置_歷史分位_pct"], errors="coerce"),
        "散戶比偏高強度": pd.to_numeric(daily["散戶多空比_歷史分位_pct"], errors="coerce"),
        "MA10斜率偏低強度": 100.0 - pd.to_numeric(daily["MA10斜率_歷史分位_pct"], errors="coerce"),
        "主力淨多空OI偏低強度": 100.0 - pd.to_numeric(daily["主力淨多空_OI比_歷史分位_pct"], errors="coerce"),
        "隔日日內振幅_pct": pd.to_numeric(daily["隔日日內振幅_pct"], errors="coerce"),
    })
    data["振幅至少1.0pct"] = (data["隔日日內振幅_pct"] >= 1.0).astype(int)
    data["振幅至少1.5pct"] = (data["隔日日內振幅_pct"] >= 1.5).astype(int)
    return data.dropna(subset=["波動分數", *FEATURES, "隔日日內振幅_pct"]).reset_index(drop=True)


def _score_lookup(train: pd.DataFrame, target: str) -> tuple[dict[int, float], float]:
    lookup = train.groupby("波動分數")[target].mean().to_dict()
    return {int(key): float(value) for key, value in lookup.items()}, float(train[target].mean())


def _lookup_predict(scores: pd.Series, lookup: dict[int, float], fallback: float) -> np.ndarray:
    return scores.map(lambda value: lookup.get(int(value), fallback)).to_numpy(dtype=float)


def walk_forward_predict(
    data: pd.DataFrame,
    first_test_year: int = 2021,
    minimum_train_size: int = 500,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    years = sorted(int(year) for year in data["訊號日"].dt.year.unique() if year >= first_test_year)
    predictions: list[pd.DataFrame] = []
    coefficients: list[dict[str, object]] = []
    for test_year in years:
        train = data[data["訊號日"].dt.year < test_year]
        test = data[data["訊號日"].dt.year == test_year]
        if len(train) < minimum_train_size or test.empty:
            continue
        result = test[["訊號日", "隔日日期", "波動分數", "隔日日內振幅_pct", "振幅至少1.0pct", "振幅至少1.5pct"]].copy()
        result["樣本外年度"] = test_year
        x_train = train[list(FEATURES)].to_numpy(dtype=float)
        x_test = test[list(FEATURES)].to_numpy(dtype=float)

        lookup, fallback = _score_lookup(train, "隔日日內振幅_pct")
        result["簡單分數_預測振幅_pct"] = _lookup_predict(test["波動分數"], lookup, fallback)
        ridge = make_pipeline(StandardScaler(), Ridge(alpha=1.0))
        ridge.fit(x_train, train["隔日日內振幅_pct"])
        result["Ridge_預測振幅_pct"] = ridge.predict(x_test)
        for feature, coefficient in zip(FEATURES, ridge[-1].coef_):
            coefficients.append({"樣本外年度": test_year, "目標": "隔日日內振幅_pct", "模型": "Ridge", "特徵": feature, "標準化係數": coefficient})

        for target in ("振幅至少1.0pct", "振幅至少1.5pct"):
            lookup, fallback = _score_lookup(train, target)
            result[f"簡單分數_{target}_機率"] = _lookup_predict(test["波動分數"], lookup, fallback)
            if train[target].nunique() < 2:
                result[f"Logistic_{target}_機率"] = fallback
                continue
            logistic = make_pipeline(StandardScaler(), LogisticRegression(C=1.0, max_iter=2000))
            logistic.fit(x_train, train[target])
            result[f"Logistic_{target}_機率"] = logistic.predict_proba(x_test)[:, 1]
            for feature, coefficient in zip(FEATURES, logistic[-1].coef_[0]):
                coefficients.append({"樣本外年度": test_year, "目標": target, "模型": "Logistic", "特徵": feature, "標準化係數": coefficient})
        predictions.append(result)
    if not predictions:
        raise ValueError("沒有足夠資料產生樣本外預測；請降低 first_test_year 或 minimum_train_size")
    return pd.concat(predictions, ignore_index=True), pd.DataFrame(coefficients)


def _top_lift(actual: pd.Series, predicted: pd.Series) -> tuple[float, float]:
    count = max(1, int(math.ceil(len(predicted) * 0.20)))
    top_index = predicted.nlargest(count).index
    top_mean = float(actual.loc[top_index].mean())
    overall = float(actual.mean())
    return top_mean, top_mean / overall if overall else math.nan


def build_metrics(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    periods: list[tuple[str, pd.DataFrame]] = [("全部樣本外", predictions)]
    periods.extend((str(year), group) for year, group in predictions.groupby("樣本外年度"))
    for period, group in periods:
        actual_range = group["隔日日內振幅_pct"]
        for model, column in (("簡單分數", "簡單分數_預測振幅_pct"), ("Ridge", "Ridge_預測振幅_pct")):
            predicted = group[column]
            rho = spearmanr(actual_range, predicted).statistic if predicted.nunique() > 1 else math.nan
            top_mean, lift = _top_lift(actual_range, predicted)
            rows.append({
                "評估期間": period, "目標": "隔日日內振幅_pct", "模型": model, "樣本數": len(group),
                "MAE": mean_absolute_error(actual_range, predicted),
                "RMSE": mean_squared_error(actual_range, predicted) ** 0.5,
                "Spearman": rho, "Brier": math.nan, "ROC_AUC": math.nan, "LogLoss": math.nan,
                "預測最高20pct實際值": top_mean, "最高20pct提升倍數": lift,
            })
        for target in ("振幅至少1.0pct", "振幅至少1.5pct"):
            actual = group[target]
            for model, column in (("簡單分數", f"簡單分數_{target}_機率"), ("Logistic", f"Logistic_{target}_機率")):
                predicted = group[column].clip(1e-9, 1 - 1e-9)
                top_mean, lift = _top_lift(actual, predicted)
                auc = roc_auc_score(actual, predicted) if actual.nunique() > 1 else math.nan
                rows.append({
                    "評估期間": period, "目標": target, "模型": model, "樣本數": len(group),
                    "MAE": math.nan, "RMSE": math.nan, "Spearman": math.nan,
                    "Brier": float(np.mean((actual - predicted) ** 2)), "ROC_AUC": auc,
                    "LogLoss": log_loss(actual, predicted, labels=[0, 1]),
                    "預測最高20pct實際值": top_mean, "最高20pct提升倍數": lift,
                })
    return pd.DataFrame(rows)


def _records(frame: pd.DataFrame, columns: list[str]) -> list[tuple[Any, ...]]:
    return [tuple(_db_value(value) for value in row) for row in frame[columns].itertuples(index=False, name=None)]


def _insert_rows(cursor: Any, table: str, columns: list[str], frame: pd.DataFrame) -> None:
    rows = _records(frame, columns)
    if rows:
        cursor.executemany(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})", rows)


def save_to_database(
    run_id: uuid.UUID,
    predictions: pd.DataFrame,
    metrics: pd.DataFrame,
    coefficients: pd.DataFrame,
    settings: Settings,
    first_test_year: int,
    source_rows: int,
) -> None:
    from common.db import get_connection

    prediction_db = predictions.rename(columns={
        "訊號日": "signal_date", "隔日日期": "result_date", "波動分數": "volatility_score",
        "隔日日內振幅_pct": "actual_range_pct", "振幅至少1.0pct": "actual_range_ge_100",
        "振幅至少1.5pct": "actual_range_ge_150", "樣本外年度": "test_year",
        "簡單分數_預測振幅_pct": "score_predicted_range_pct", "Ridge_預測振幅_pct": "ridge_predicted_range_pct",
        "簡單分數_振幅至少1.0pct_機率": "score_range_ge_100_probability",
        "Logistic_振幅至少1.0pct_機率": "logistic_range_ge_100_probability",
        "簡單分數_振幅至少1.5pct_機率": "score_range_ge_150_probability",
        "Logistic_振幅至少1.5pct_機率": "logistic_range_ge_150_probability",
    }).copy()
    prediction_db.insert(0, "run_id", run_id)
    metric_db = metrics.rename(columns={
        "評估期間": "evaluation_period", "目標": "target_name", "模型": "model_name", "樣本數": "sample_count",
        "MAE": "mae", "RMSE": "rmse", "Spearman": "spearman", "Brier": "brier",
        "ROC_AUC": "roc_auc", "LogLoss": "log_loss", "預測最高20pct實際值": "top20_actual",
        "最高20pct提升倍數": "top20_lift",
    }).copy()
    metric_db.insert(0, "run_id", run_id)
    coefficient_db = coefficients.rename(columns={
        "樣本外年度": "test_year", "目標": "target_name", "模型": "model_name",
        "特徵": "feature_name", "標準化係數": "standardized_coefficient",
    }).copy()
    coefficient_db.insert(0, "run_id", run_id)
    tables = (
        "research.futures_volatility_model_prediction",
        "research.futures_volatility_model_metric",
        "research.futures_volatility_model_coefficient",
    )
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for table in tables:
                cursor.execute("SELECT to_regclass(%s)", (table,))
                if cursor.fetchone()[0] is None:
                    raise RuntimeError("尚未建立波動模型研究表；請先執行 db_migrate/20261020_add_futures_volatility_model.sql")
            cursor.execute(
                """INSERT INTO research.analysis_run (
                    run_id, analysis_name, futures_id, start_date, end_date, bucket_edges,
                    metric_version, source_relation, status, source_rows, valid_rows,
                    excluded_rows, exclusion_summary
                ) VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,%s,'running',%s,%s,%s,%s::jsonb)""",
                (run_id, ANALYSIS_NAME, settings.futures_id, settings.start, settings.end or predictions["訊號日"].max().date(),
                 json.dumps({"features": FEATURES, "targets": TARGETS, "first_test_year": first_test_year}, ensure_ascii=False),
                 METRIC_VERSION, "research.futures_volatility_strength_daily", source_rows, len(predictions),
                 max(source_rows-len(predictions), 0), json.dumps({"prediction_rows": len(predictions)}, ensure_ascii=False)),
            )
            _insert_rows(cursor, tables[0], list(prediction_db.columns), prediction_db)
            _insert_rows(cursor, tables[1], list(metric_db.columns), metric_db)
            _insert_rows(cursor, tables[2], list(coefficient_db.columns), coefficient_db)
            cursor.execute("UPDATE research.analysis_run SET status='completed', finished_at=CURRENT_TIMESTAMP WHERE run_id=%s", (run_id,))
        conn.commit()


def export_results(run_id: uuid.UUID, predictions: pd.DataFrame, metrics: pd.DataFrame, coefficients: pd.DataFrame, settings: Settings, first_test_year: int, output_dir: Path) -> tuple[Path, ...]:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{settings.start:%Y%m%d}_{settings.end:%Y%m%d}" if settings.end else f"{settings.start:%Y%m%d}_latest"
    paths = (
        output_dir/f"volatility_model_predictions_{suffix}.csv",
        output_dir/f"volatility_model_metrics_{suffix}.csv",
        output_dir/f"volatility_model_coefficients_{suffix}.csv",
        output_dir/f"volatility_model_manifest_{suffix}.json",
    )
    predictions.to_csv(paths[0],index=False,encoding="utf-8-sig")
    metrics.to_csv(paths[1],index=False,encoding="utf-8-sig")
    coefficients.to_csv(paths[2],index=False,encoding="utf-8-sig")
    paths[3].write_text(json.dumps({
        "run_id":str(run_id),"analysis_name":ANALYSIS_NAME,"metric_version":METRIC_VERSION,
        "start":settings.start.isoformat(),"end":settings.end.isoformat() if settings.end else None,
        "first_test_year":first_test_year,"features":FEATURES,"targets":TARGETS,
        "method":"每年只使用此前年度資料訓練，再預測該年度；Ridge alpha=1，Logistic C=1",
        "warning":"特徵由先前全期探索選出，因此逐年預測為樣本外，但整體研究仍非完全未觀察樣本外",
    },ensure_ascii=False,indent=2),encoding="utf-8")
    return paths


def parse_args() -> argparse.Namespace:
    parser=argparse.ArgumentParser(description="波動強度簡單分數與Ridge/Logistic樣本外比較")
    parser.add_argument("--start",type=date.fromisoformat,default=date(2018,1,1)); parser.add_argument("--end",type=date.fromisoformat)
    parser.add_argument("--split-date",type=date.fromisoformat,default=date(2023,1,1)); parser.add_argument("--futures-id",default="TX")
    parser.add_argument("--history-window",type=int,default=252); parser.add_argument("--minimum-history",type=int,default=60)
    parser.add_argument("--minimum-group-size",type=int,default=30); parser.add_argument("--minimum-year-size",type=int,default=10)
    parser.add_argument("--alpha",type=float,default=0.05); parser.add_argument("--winsor-tail",type=float,default=0.005)
    parser.add_argument("--first-test-year",type=int,default=2021); parser.add_argument("--minimum-train-size",type=int,default=500)
    parser.add_argument("--output-dir",type=Path,default=Path("backtest_output/futures_volatility_model")); parser.add_argument("--save-db",action="store_true")
    return parser.parse_args()


def main() -> None:
    args=parse_args(); settings=Settings(start=args.start,end=args.end,futures_id=args.futures_id,split_date=args.split_date,
        history_window=args.history_window,minimum_history=args.minimum_history,minimum_group_size=args.minimum_group_size,
        minimum_year_size=args.minimum_year_size,alpha=args.alpha,winsor_tail=args.winsor_tail)
    source=load_source(settings); detail,_=prepare_detail(source,settings); daily=build_daily_score(detail); data=prepare_model_data(daily)
    predictions,coefficients=walk_forward_predict(data,args.first_test_year,args.minimum_train_size); metrics=build_metrics(predictions)
    run_id=uuid.uuid4(); paths=export_results(run_id,predictions,metrics,coefficients,settings,args.first_test_year,args.output_dir)
    if args.save_db: save_to_database(run_id,predictions,metrics,coefficients,settings,args.first_test_year,len(data))
    print(f"樣本外預測：{len(predictions):,} 日"); print(f"研究 run_id：{run_id}"); print(f"資料庫保存：{'完成' if args.save_db else '未啟用'}")
    print("\n輸出："); [print(path.resolve()) for path in paths]


if __name__ == "__main__": main()
