"""Yu 自動選點模型 V3。

把每個交易日的合法候選點位視為同一個 ranking group，逐日只使用更早的
Yu 選點紀錄訓練，並比較不同輸入欄位組合的 Top-1/Top-3/Top-5 表現。

時間定義：日期 D 的候選清單使用資料庫中日期 <= D 的日報，適合在 D 收盤後
產生下一交易時段的清單；模型訓練只使用 Yu 分析日期 < D 的標籤。
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from common import db
from machine_learning import yu_strategy_model as v1
from machine_learning import yu_strategy_model_v2 as v2


TARGET_LEAK_COLUMNS = {
    "進場點", "進場依據", "進場條件", "進場附註", "操作結果", "今日操作心得",
    "停利1_點位", "停利2_點位", "停損1_點位", "停損2_點位", "原始區段",
}


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    required = {"date_column", "labels_sheet", "futures", "taiex", "candidate", "selector"}
    missing = sorted(required - config.keys())
    if missing:
        raise ValueError(f"V3 設定檔缺少欄位：{missing}")
    names = [item["name"] for item in config["selector"]["feature_sets"]]
    if len(names) != len(set(names)):
        raise ValueError("feature_sets 的 name 不可重複")
    leaked = TARGET_LEAK_COLUMNS.intersection(config["selector"].get("yu_context_fields", []))
    if leaked:
        raise ValueError(f"Yu context 含答案洩漏欄位：{sorted(leaked)}")
    return config


def _unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(values))


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
    sql = (
        f"SELECT {', '.join(v1.quote_column(c) for c in columns)} "
        f"FROM {v1.quote_relation(relation)} WHERE {' AND '.join(where)} "
        f"ORDER BY {v1.quote_column(date_column)}"
    )
    frame = db.query_to_df(sql, params)
    if frame.empty:
        raise RuntimeError(f"{relation} 在指定期間查無資料")
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise RuntimeError(f"{relation} 缺少設定欄位：{missing}")
    frame[date_column] = pd.to_datetime(frame[date_column], errors="coerce").dt.normalize()
    return frame.dropna(subset=[date_column]).drop_duplicates(date_column, keep="last").reset_index(drop=True)


def load_market_reports(config: dict[str, Any], start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    date_column = config["date_column"]
    futures = config["futures"]
    mapped = [item["field"] for item in futures["mapped_fields"] + futures["current_fields"]]
    futures_columns = _unique([
        date_column, *futures.get("entity_filter", {}).keys(), *mapped,
        futures["reference_price_field"], futures["range_field"], futures["volume_field"],
        *config["selector"]["tx_report_fields"],
    ])
    taiex_columns = _unique([
        date_column, *config["taiex"]["key_fields"],
        *config["selector"]["taiex_report_fields"],
    ])
    taiex = _load_relation(config["taiex"]["relation"], taiex_columns, date_column, start, end)
    tx = _load_relation(
        futures["relation"], futures_columns, date_column, start, end,
        futures.get("entity_filter"),
    )
    return taiex, tx


def _distance_to_zone(point: float, row: pd.Series) -> float:
    if point < float(row["區間下緣"]):
        return float(row["區間下緣"]) - point
    if point > float(row["區間上緣"]):
        return point - float(row["區間上緣"])
    return 0.0


def _history_key(row: pd.Series, scenario: str) -> tuple[str, str, str]:
    return scenario, str(row["主要類型"]), str(row["主要盤別"])


def _market_features(
    row: pd.Series,
    prefix: str,
    fields: list[str],
) -> dict[str, Any]:
    return {f"{prefix}__{field}": row.get(field) for field in fields}


def _candidate_feature_row(
    cluster: pd.Series,
    plan: pd.Series,
    scenario: str,
    tx_row: pd.Series,
    taiex_row: pd.Series,
    history_counts: Counter,
    history_scenario_totals: Counter,
    config: dict[str, Any],
) -> dict[str, Any]:
    key = _history_key(cluster, scenario)
    type_key = (scenario, str(cluster["主要類型"]))
    total = int(history_scenario_totals[scenario])
    family_count = int(history_counts[key])
    result: dict[str, Any] = {
        "分析日期": plan["分析日期"],
        "方案序號": int(plan.get("方案序號", 1)),
        "進場情境": scenario,
        "交易方向": v1.normalize_direction(plan.get("交易方向")),
        "點位群編號": int(cluster["點位群編號"]),
        "候選點位": float(cluster["代表點位"]),
        "區間下緣": float(cluster["區間下緣"]),
        "區間上緣": float(cluster["區間上緣"]),
        "主要盤別": str(cluster["主要盤別"]),
        "主要類型": str(cluster["主要類型"]),
        "主要加權選日原因": str(cluster["主要加權選日原因"]),
        "依據數": float(cluster["依據數"]),
        "來源日期數": float(cluster["來源日期數"]),
        "距參考收盤點數": float(cluster["距參考收盤點數"]),
        "距參考收盤絕對點數": float(cluster["距參考收盤絕對點數"]),
        "最近來源列數": float(cluster["最近來源列數"]),
        "最高加權關鍵K分數": float(cluster["最高加權關鍵K分數"]),
        "歷史同情境樣本數": total,
        "歷史同情境類型盤別選用次數": family_count,
        "歷史同情境類型盤別選用率": family_count / total if total else 0.0,
        "歷史同情境類型選用次數": int(history_counts[type_key]),
    }
    result.update(_market_features(tx_row, "tx", config["selector"]["tx_report_fields"]))
    result.update(_market_features(taiex_row, "taiex", config["selector"]["taiex_report_fields"]))
    for field in config["selector"].get("yu_context_fields", []):
        result[f"yu__{field}"] = plan.get(field)
    return result


def build_candidate_samples(
    labels: pd.DataFrame,
    futures: pd.DataFrame,
    taiex: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    """建立候選層級樣本；歷史選用統計只在處理完當日後才更新。"""
    date_column = config["date_column"]
    tx_index = futures.set_index(date_column)
    taiex_index = taiex.set_index(date_column)
    history_counts: Counter = Counter()
    history_scenario_totals: Counter = Counter()
    tolerance = float(config["candidate"]["match_tolerance_points"])
    rows: list[dict[str, Any]] = []
    cache: dict[pd.Timestamp, pd.DataFrame] = {}

    for analysis_date, plans in labels.sort_values(["分析日期", "方案序號"]).groupby("分析日期"):
        if analysis_date not in tx_index.index or analysis_date not in taiex_index.index:
            continue
        tx_row = tx_index.loc[analysis_date]
        taiex_row = taiex_index.loc[analysis_date]
        reference = v2._number(tx_row[config["futures"]["reference_price_field"]])
        if reference is None:
            continue
        clusters = cache.setdefault(
            analysis_date,
            v2.cluster_candidates(v2.generate_raw_candidates(futures, taiex, analysis_date, config), config),
        )
        daily_updates: list[tuple[str, str, str]] = []
        for _, plan in plans.iterrows():
            entry = v2._number(plan.get("進場點"))
            scenario = v2.infer_scenario(plan, reference)
            if entry is None or scenario is None or clusters.empty:
                continue
            compatible = clusters.loc[v2.scenario_compatible(clusters, scenario, config)].copy()
            if compatible.empty:
                continue
            plan_id = f"{analysis_date:%Y%m%d}-{int(plan.get('方案序號', 1))}"
            plan_rows: list[dict[str, Any]] = []
            for _, cluster in compatible.iterrows():
                item = _candidate_feature_row(
                    cluster, plan, scenario, tx_row, taiex_row,
                    history_counts, history_scenario_totals, config,
                )
                distance = _distance_to_zone(entry, cluster)
                item.update({
                    "計畫ID": plan_id, "Yu進場點": entry, "差距點數": distance,
                    "是否命中Yu點位": int(distance <= tolerance),
                })
                plan_rows.append(item)
            if not plan_rows:
                continue
            rows.extend(plan_rows)
            best = min(plan_rows, key=lambda x: (x["差距點數"], -x["依據數"], x["最近來源列數"]))
            daily_updates.append((scenario, best["主要類型"], best["主要盤別"]))
        # 同一天的多方案不可互相偷看，等全日樣本建立完成才供未來日期使用。
        for scenario, kind, session in daily_updates:
            history_counts[(scenario, kind, session)] += 1
            history_counts[(scenario, kind)] += 1
            history_scenario_totals[scenario] += 1
    return pd.DataFrame(rows)


def _feature_columns(frame: pd.DataFrame, groups: list[str], config: dict[str, Any]) -> list[str]:
    columns: list[str] = []
    definitions = config["selector"]["feature_groups"]
    for group in groups:
        if group == "tx_report":
            columns.extend(f"tx__{x}" for x in config["selector"]["tx_report_fields"])
        elif group == "taiex_report":
            columns.extend(f"taiex__{x}" for x in config["selector"]["taiex_report_fields"])
        elif group == "yu_context":
            columns.extend(f"yu__{x}" for x in config["selector"].get("yu_context_fields", []))
        else:
            columns.extend(definitions[group])
    columns = _unique(columns)
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise RuntimeError(f"特徵群缺少欄位：{missing}")
    return columns


def _python_scalar(value: Any) -> Any:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return np.nan
    if hasattr(value, "item"):
        value = value.item()
    return value


def _clean_features(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = frame[columns].copy()
    for column in result.columns:
        result[column] = result[column].map(_python_scalar)
        numeric = pd.to_numeric(result[column], errors="coerce")
        non_missing = result[column].notna().sum()
        if non_missing and numeric.notna().sum() / non_missing >= 0.9:
            result[column] = numeric.astype(float)
        else:
            result[column] = result[column].fillna("缺值").astype(str)
    return result


def fit_selector(frame: pd.DataFrame, columns: list[str], config: dict[str, Any]) -> Pipeline:
    x = _clean_features(frame, columns)
    numeric = list(x.select_dtypes(include=[np.number]).columns)
    categorical = [column for column in columns if column not in numeric]
    transformers = []
    if numeric:
        transformers.append(("num", Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
        ]), numeric))
    if categorical:
        transformers.append(("cat", Pipeline([
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]), categorical))
    model = LogisticRegression(
        max_iter=int(config["selector"]["model"].get("max_iter", 1000)),
        C=float(config["selector"]["model"].get("C", 1.0)),
        class_weight="balanced",
        random_state=int(config["selector"]["model"].get("random_seed", 42)),
    )
    pipeline = Pipeline([("features", ColumnTransformer(transformers)), ("model", model)])
    pipeline.fit(x, frame["是否命中Yu點位"].astype(int))
    return pipeline


def _positive_probability(model: Pipeline, frame: pd.DataFrame, columns: list[str]) -> np.ndarray:
    probabilities = model.predict_proba(_clean_features(frame, columns))
    classes = list(model.named_steps["model"].classes_)
    return probabilities[:, classes.index(1)] if 1 in classes else np.zeros(len(frame))


def _rank_result(block: pd.DataFrame, scores: np.ndarray, feature_set: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ranked = block.copy()
    ranked["模型分數"] = scores
    ranked = ranked.sort_values(
        ["模型分數", "依據數", "最近來源列數"], ascending=[False, False, True]
    ).reset_index(drop=True)
    ranked["模型排名"] = np.arange(1, len(ranked) + 1)
    ranked["特徵組合"] = feature_set
    hits = ranked["是否命中Yu點位"].astype(bool)
    hit_ranks = ranked.loc[hits, "模型排名"]
    best_distance = float(ranked.iloc[0]["差距點數"])
    actual_best = float(ranked["差距點數"].min())
    metric = {
        "特徵組合": feature_set,
        "分析日期": ranked.iloc[0]["分析日期"],
        "計畫ID": ranked.iloc[0]["計畫ID"],
        "進場情境": ranked.iloc[0]["進場情境"],
        "Yu進場點": ranked.iloc[0]["Yu進場點"],
        "候選數": len(ranked),
        "可命中候選存在": int(hits.any()),
        "Top1命中": int(hits.iloc[:1].any()),
        "Top3命中": int(hits.iloc[:3].any()),
        "Top5命中": int(hits.iloc[:5].any()),
        "首個命中排名": int(hit_ranks.min()) if not hit_ranks.empty else np.nan,
        "倒數排名MRR": 1.0 / float(hit_ranks.min()) if not hit_ranks.empty else 0.0,
        "Top1差距點數": best_distance,
        "候選最小差距點數": actual_best,
    }
    detail_columns = [
        "特徵組合", "分析日期", "計畫ID", "進場情境", "Yu進場點", "模型排名",
        "模型分數", "候選點位", "區間下緣", "區間上緣", "差距點數",
        "是否命中Yu點位", "主要盤別", "主要類型", "依據數", "來源日期數",
        "距參考收盤點數", "最近來源列數", "最高加權關鍵K分數",
    ]
    return ranked[detail_columns].to_dict("records"), metric


def walk_forward_experiments(
    samples: pd.DataFrame,
    config: dict[str, Any],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    details: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    minimum_plans = int(config["selector"]["minimum_training_plans"])
    evaluation = samples.loc[samples["分析日期"].between(start, end)]
    plan_dates = evaluation[["計畫ID", "分析日期"]].drop_duplicates().sort_values("分析日期")

    for experiment in config["selector"]["feature_sets"]:
        name = experiment["name"]
        columns = _feature_columns(samples, experiment["groups"], config)
        last_training_date: pd.Timestamp | None = None
        model: Pipeline | None = None
        for analysis_date, daily_plans in plan_dates.groupby("分析日期"):
            prior = samples.loc[samples["分析日期"] < analysis_date]
            prior_plan_count = prior["計畫ID"].nunique()
            if prior_plan_count < minimum_plans or prior["是否命中Yu點位"].nunique() < 2:
                continue
            # 每個交易日只訓練一次，同日多方案共用，仍然不會互相偷看答案。
            if last_training_date != analysis_date:
                model = fit_selector(prior, columns, config)
                last_training_date = analysis_date
            assert model is not None
            for plan_id in daily_plans["計畫ID"]:
                block = samples.loc[samples["計畫ID"] == plan_id]
                ranked_rows, metric = _rank_result(
                    block, _positive_probability(model, block, columns), name
                )
                metric["訓練計畫數"] = prior_plan_count
                metric["輸入欄位數"] = len(columns)
                metric["僅研究用途"] = bool(experiment.get("research_only", False))
                details.extend(ranked_rows)
                metrics.append(metric)
    return pd.DataFrame(details), pd.DataFrame(metrics)


def summarize_experiments(plan_metrics: pd.DataFrame) -> pd.DataFrame:
    if plan_metrics.empty:
        return pd.DataFrame()
    rows = []
    for name, group in plan_metrics.groupby("特徵組合", sort=False):
        eligible = group.loc[group["可命中候選存在"] == 1]
        rows.append({
            "特徵組合": name,
            "有效回測計畫": len(group),
            "候選可覆蓋計畫": len(eligible),
            "候選覆蓋率_pct": round(group["可命中候選存在"].mean() * 100, 2),
            "Top1命中率_pct": round(eligible["Top1命中"].mean() * 100, 2) if len(eligible) else np.nan,
            "Top3命中率_pct": round(eligible["Top3命中"].mean() * 100, 2) if len(eligible) else np.nan,
            "Top5命中率_pct": round(eligible["Top5命中"].mean() * 100, 2) if len(eligible) else np.nan,
            "MRR": round(eligible["倒數排名MRR"].mean(), 4) if len(eligible) else np.nan,
            "Top1平均差距點數": round(group["Top1差距點數"].mean(), 2),
            "Top1差距中位數": round(group["Top1差距點數"].median(), 2),
            "輸入欄位數": int(group["輸入欄位數"].max()),
            "僅研究用途": bool(group["僅研究用途"].max()),
        })
    return pd.DataFrame(rows).sort_values(
        ["僅研究用途", "Top3命中率_pct", "Top1命中率_pct", "MRR"],
        ascending=[True, False, False, False],
    ).reset_index(drop=True)


def coefficient_report(
    samples: pd.DataFrame,
    config: dict[str, Any],
    through_date: pd.Timestamp,
) -> pd.DataFrame:
    prior = samples.loc[samples["分析日期"] <= through_date]
    rows = []
    for experiment in config["selector"]["feature_sets"]:
        columns = _feature_columns(prior, experiment["groups"], config)
        if prior["是否命中Yu點位"].nunique() < 2:
            continue
        model = fit_selector(prior, columns, config)
        transformer = model.named_steps["features"]
        names = transformer.get_feature_names_out()
        coefficients = model.named_steps["model"].coef_[0]
        for feature, coefficient in sorted(zip(names, coefficients), key=lambda x: abs(x[1]), reverse=True):
            rows.append({
                "特徵組合": experiment["name"], "特徵": feature,
                "係數": float(coefficient), "影響方向": "提高入選" if coefficient > 0 else "降低入選",
            })
    return pd.DataFrame(rows)


def _load_inputs(args: argparse.Namespace, config: dict[str, Any]):
    labels = v1.load_yu_labels(args.labels, config.get("labels_sheet"))
    start = pd.Timestamp(getattr(args, "start", None) or labels["分析日期"].min()).normalize()
    end = pd.Timestamp(getattr(args, "end", None) or getattr(args, "date", None)).normalize()
    lookback_days = int(config["candidate"]["lookback_rows"]) * 3
    history_start = min(start, labels["分析日期"].min()) - pd.Timedelta(days=lookback_days)
    taiex, futures = load_market_reports(config, history_start.date(), end.date())
    return labels.loc[labels["分析日期"] <= end].copy(), taiex, futures, start, end


def run_analyze(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    labels, taiex, futures, start, end = _load_inputs(args, config)
    samples = build_candidate_samples(labels, futures, taiex, config)
    details, plan_metrics = walk_forward_experiments(samples, config, start, end)
    summary = summarize_experiments(plan_metrics)
    coefficients = coefficient_report(samples, config, end)
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    v1._write_csv(summary, output / "yu_v3_feature_set_comparison.csv")
    v1._write_csv(plan_metrics, output / "yu_v3_plan_metrics.csv")
    v1._write_csv(details, output / "yu_v3_walk_forward_rankings.csv")
    v1._write_csv(coefficients, output / "yu_v3_feature_coefficients.csv")
    print(summary.to_string(index=False))
    print(f"輸出位置：{output.resolve()}")


def _live_rows_for_scenario(
    clusters: pd.DataFrame,
    scenario: str,
    target: pd.Timestamp,
    tx_row: pd.Series,
    taiex_row: pd.Series,
    samples: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    compatible = clusters.loc[v2.scenario_compatible(clusters, scenario, config)].copy()
    if compatible.empty:
        return pd.DataFrame()
    prior = samples.loc[samples["分析日期"] < target]
    selected = (
        prior.sort_values(["計畫ID", "差距點數", "依據數"], ascending=[True, True, False])
        .drop_duplicates("計畫ID")
    )
    counts: Counter = Counter()
    totals: Counter = Counter()
    for _, row in selected.iterrows():
        key = (row["進場情境"], str(row["主要類型"]), str(row["主要盤別"]))
        counts[key] += 1
        counts[(row["進場情境"], str(row["主要類型"]))] += 1
        totals[row["進場情境"]] += 1
    dummy = pd.Series({"分析日期": target, "方案序號": 1, "交易方向": "多" if "多" in scenario else "空"})
    return pd.DataFrame([
        _candidate_feature_row(cluster, dummy, scenario, tx_row, taiex_row, counts, totals, config)
        for _, cluster in compatible.iterrows()
    ])


def run_candidates(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    labels, taiex, futures, _, target = _load_inputs(args, config)
    samples = build_candidate_samples(labels.loc[labels["分析日期"] < target], futures, taiex, config)
    date_column = config["date_column"]
    tx_today = futures.loc[futures[date_column] == target]
    taiex_today = taiex.loc[taiex[date_column] == target]
    if tx_today.empty or taiex_today.empty:
        raise RuntimeError(f"{target.date()} 缺少期貨或加權日報")
    raw = v2.generate_raw_candidates(futures, taiex, target, config)
    clusters = v2.cluster_candidates(raw, config)
    if clusters.empty:
        raise RuntimeError(f"{target.date()} 無法產生候選點位")
    requested = args.feature_set
    experiments = [x for x in config["selector"]["feature_sets"] if not x.get("research_only", False)]
    if requested:
        experiments = [x for x in experiments if x["name"] == requested]
        if not experiments:
            raise ValueError(f"找不到可即時使用的特徵組合：{requested}")
    output_rows = []
    for experiment in experiments:
        columns = _feature_columns(samples, experiment["groups"], config)
        model = fit_selector(samples, columns, config)
        for scenario in v2.SCENARIOS:
            live = _live_rows_for_scenario(
                clusters, scenario, target, tx_today.iloc[0], taiex_today.iloc[0], samples, config
            )
            if live.empty:
                continue
            live["模型分數"] = _positive_probability(model, live, columns)
            live = live.sort_values(["模型分數", "依據數"], ascending=[False, False]).head(
                int(config["candidate"]["top_per_scenario"])
            )
            live["排名"] = np.arange(1, len(live) + 1)
            live["特徵組合"] = experiment["name"]
            live["資料截止日"] = target
            output_rows.append(live)
    rankings = pd.concat(output_rows, ignore_index=True) if output_rows else pd.DataFrame()
    columns = [
        "資料截止日", "特徵組合", "進場情境", "排名", "候選點位", "區間下緣", "區間上緣",
        "模型分數", "主要盤別", "主要類型", "依據數", "來源日期數", "距參考收盤點數",
        "最近來源列數", "最高加權關鍵K分數", "主要加權選日原因",
    ]
    rankings = rankings[[c for c in columns if c in rankings.columns]]
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"yu_v3_candidate_rankings_{target:%Y%m%d}.csv"
    v1._write_csv(rankings, path)
    print(rankings.to_string(index=False))
    print(f"輸出位置：{path.resolve()}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Yu 自動選點模型 V3")
    commands = parser.add_subparsers(dest="command", required=True)
    analyze = commands.add_parser("analyze", help="逐日比較多組輸入欄位的選點效果")
    analyze.add_argument("--config", required=True)
    analyze.add_argument("--labels", required=True)
    analyze.add_argument("--start", required=True)
    analyze.add_argument("--end", required=True)
    analyze.add_argument("--output-dir", default="../output/machine_learningyu_point_selector_v3")
    analyze.set_defaults(func=run_analyze)
    candidates = commands.add_parser("candidates", help="產生指定資料截止日的四情境候選點位")
    candidates.add_argument("--config", required=True)
    candidates.add_argument("--labels", required=True)
    candidates.add_argument("--date", required=True)
    candidates.add_argument("--feature-set")
    candidates.add_argument("--output-dir", default="../output/machine_learningyu_point_selector_v3")
    candidates.set_defaults(func=run_candidates)
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
