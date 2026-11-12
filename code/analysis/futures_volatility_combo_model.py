from __future__ import annotations

import argparse
import itertools
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

try:
    from sklearn.linear_model import LogisticRegression, Ridge
    from sklearn.metrics import (
        average_precision_score,
        brier_score_loss,
        log_loss,
        mean_absolute_error,
        mean_squared_error,
        roc_auc_score,
    )
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
except ImportError as exc:
    raise RuntimeError(
        "此分析需要 scikit-learn。請在專案既有 Python 環境安裝 scikit-learn。"
    ) from exc

from backtest.futures_raw_signal_strength_scan import (
    SOURCE_COLUMNS,
    _safe_ratio,
    build_signal_features,
)


ANALYSIS_NAME = "futures_volatility_combo_model"
METRIC_VERSION = "1.0.0"
SOURCE_VIEW = "report.v_futures_report_daily_corrected_turning"
TARGET_VIEW = "research.v_futures_daily_amplitude_pattern"
STOCK_TABLE = "core.stock_report_daily"
MAINTENANCE_TABLE = "raw_finmind.fm_taiwan_total_exchange_margin_maintenance"

# 每個候選先轉為「分數越高，代表該假設中的波動風險條件越強」。
FEATURES = {
    "布林低位": "布林通道位置歷史分位反向（位置越低分數越高）",
    "散戶偏多": "散戶多空比歷史分位（比值越高分數越高）",
    "MA10偏弱": "MA10斜率歷史分位反向（斜率越低分數越高）",
    "主力淨空": "主力淨多空／OI歷史分位反向（比值越低分數越高）",
    "MA5低乖離": "MA5乖離率歷史分位反向（乖離越低分數越高）",
    "外資三日賣超": "外資三日累計買賣超／成交金額歷史分位反向（賣超越大分數越高）",
    "融資維持率三日急降": "三個交易日前維持率減當日維持率之歷史分位",
}
BASELINE_FEATURES = ("當日振幅倍數", "近5日平均振幅倍數", "近20日平均振幅倍數")
TARGETS = {
    "next_range_ratio": "次日日盤振幅／此前30日平均振幅",
    "劇烈以上": "既有振幅標籤為劇烈或極端",
}


@dataclass(frozen=True)
class Settings:
    start: date = date(2018, 1, 1)
    end: date | None = None
    futures_id: str = "TX"
    first_test_year: int = 2023
    history_window: int = 252
    minimum_history: int = 60
    minimum_train_size: int = 500
    ridge_alpha: float = 10.0
    logistic_c: float = 1.0
    output_dir: Path = Path("../output/analysis/futures_volatility_combo_model")

    def validate(self) -> None:
        if self.end and self.end < self.start:
            raise ValueError("end 不可早於 start")
        if self.first_test_year <= self.start.year:
            raise ValueError("first-test-year 必須晚於 start 年度")
        if self.history_window < 60:
            raise ValueError("history-window 至少為60")
        if not 20 <= self.minimum_history <= self.history_window:
            raise ValueError("minimum-history 必須介於20與history-window之間")
        if self.minimum_train_size < 100:
            raise ValueError("minimum-train-size 至少為100")
        if self.ridge_alpha <= 0 or self.logistic_c <= 0:
            raise ValueError("模型正規化參數必須大於0")


def load_inputs(settings: Settings) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from common import db

    settings.validate()
    query_start = settings.start - timedelta(days=max(500, settings.history_window * 3))
    query_end = (settings.end or date.today()) + timedelta(days=14)
    selected = ", ".join(f'f."{column}"' for column in SOURCE_COLUMNS)
    source = db.sql_to_df(
        f'''SELECT {selected},
                   stock."買超_外資_億" AS "外資買賣超_億",
                   stock."總成交金額_億" AS "大盤成交金額_億"
            FROM {SOURCE_VIEW} AS f
            LEFT JOIN {STOCK_TABLE} AS stock
              ON stock."日期" = f."日期"
             AND stock."股票代號" = 'TAIEX'
            WHERE f.futures_id = ? AND f."日期" BETWEEN ? AND ?
            ORDER BY f."日期"''',
        params=(settings.futures_id, query_start, query_end),
        parse_dates=["日期"],
    )
    targets = db.sql_to_df(
        f'''SELECT trading_date, range_ratio, amplitude_label
            FROM {TARGET_VIEW}
            WHERE futures_id = ? AND valid_for_research
              AND trading_date BETWEEN ? AND ?
            ORDER BY trading_date''',
        params=(settings.futures_id, query_start, query_end),
        parse_dates=["trading_date"],
    )
    maintenance = db.sql_to_df(
        f'''SELECT date, total_exchange_margin_maintenance AS margin_maintenance
            FROM {MAINTENANCE_TABLE}
            WHERE date BETWEEN ? AND ?
            ORDER BY date''',
        params=(query_start, query_end),
        parse_dates=["date"],
    )
    return source, targets, maintenance


def _rolling_percentile(values: pd.Series, window: int, minimum_history: int) -> pd.Series:
    # Kept local so this model's feature construction is explicit and auditable.
    array = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    result = np.full(len(array), np.nan, dtype=float)
    for i, value in enumerate(array):
        if not np.isfinite(value):
            continue
        history = array[max(0, i - window):i]
        history = history[np.isfinite(history)]
        if len(history) < minimum_history:
            continue
        result[i] = (
            np.count_nonzero(history < value)
            + 0.5 * np.count_nonzero(history == value)
        ) / len(history) * 100.0
    return pd.Series(result, index=values.index, dtype=float)


def prepare_model_frame(
    source: pd.DataFrame,
    targets: pd.DataFrame,
    maintenance: pd.DataFrame,
    settings: Settings,
) -> pd.DataFrame:
    raw = source.copy()
    raw["日期"] = pd.to_datetime(raw["日期"], errors="coerce").dt.normalize()
    raw = raw.dropna(subset=["日期"]).sort_values("日期").drop_duplicates("日期", keep="last").reset_index(drop=True)
    signal_features, _ = build_signal_features(raw)
    raw["日期"] = pd.to_datetime(raw["日期"]).dt.normalize()
    signal_features["日期"] = pd.to_datetime(signal_features["日期"]).dt.normalize()

    target = targets.copy()
    target["trading_date"] = pd.to_datetime(target["trading_date"], errors="coerce").dt.normalize()
    target["range_ratio"] = pd.to_numeric(target["range_ratio"], errors="coerce")
    target = target.dropna(subset=["trading_date"]).drop_duplicates("trading_date", keep="last")
    signal = raw.merge(
        target.rename(columns={
            "trading_date": "日期",
            "range_ratio": "當日振幅倍數",
            "amplitude_label": "當日振幅標籤",
        }),
        on="日期", how="left", validate="one_to_one",
    )
    signal = signal.merge(signal_features, on="日期", how="left", validate="one_to_one")

    maintenance = maintenance.copy()
    maintenance["date"] = pd.to_datetime(maintenance["date"], errors="coerce").dt.normalize()
    maintenance["margin_maintenance"] = pd.to_numeric(maintenance["margin_maintenance"], errors="coerce")
    maintenance = maintenance.dropna(subset=["date", "margin_maintenance"]).drop_duplicates("date", keep="last")
    signal = signal.merge(
        maintenance.rename(columns={"date": "日期"}),
        on="日期", how="left", validate="one_to_one",
    )

    dates = pd.to_datetime(signal["日期"])
    signal["結果日"] = dates.shift(-1)
    signal["當日振幅倍數"] = pd.to_numeric(signal["當日振幅倍數"], errors="coerce")
    signal["近5日平均振幅倍數"] = signal["當日振幅倍數"].rolling(5, min_periods=5).mean()
    signal["近20日平均振幅倍數"] = signal["當日振幅倍數"].rolling(20, min_periods=20).mean()

    foreign = pd.to_numeric(signal["外資買賣超_億"], errors="coerce")
    turnover = pd.to_numeric(signal["大盤成交金額_億"], errors="coerce")
    foreign_3d = foreign.rolling(3, min_periods=3).sum()
    turnover_3d = turnover.rolling(3, min_periods=3).sum()
    signal["外資三日買賣超率_pct"] = _safe_ratio(foreign_3d, turnover_3d) * 100.0
    signal["融資三日淨下降_pct點"] = signal["margin_maintenance"].shift(3) - signal["margin_maintenance"]

    raw_candidates = {
        "布林低位": signal["布林通道位置"],
        "散戶偏多": signal["散戶多空比"],
        "MA10偏弱": signal["MA10斜率"],
        "主力淨空": signal["主力淨多空_OI比"],
        "MA5低乖離": signal["MA5乖離"],
        "外資三日賣超": signal["外資三日買賣超率_pct"],
        "融資維持率三日急降": signal["融資三日淨下降_pct點"],
    }
    inverse_rank = {
        "布林低位", "MA10偏弱", "主力淨空", "MA5低乖離", "外資三日賣超",
    }
    for feature, values in raw_candidates.items():
        pct = _rolling_percentile(values, settings.history_window, settings.minimum_history)
        signal[feature] = 100.0 - pct if feature in inverse_rank else pct

    target_result = target.rename(columns={
        "trading_date": "結果日",
        "range_ratio": "next_range_ratio",
        "amplitude_label": "結果振幅標籤",
    })
    signal = signal.merge(
        target_result[["結果日", "next_range_ratio", "結果振幅標籤"]],
        on="結果日", how="left", validate="many_to_one",
    )
    signal["劇烈以上"] = signal["結果振幅標籤"].isin(["劇烈振幅", "極端振幅"]).astype(float)
    signal.loc[signal["結果振幅標籤"].isna(), "劇烈以上"] = np.nan

    end = settings.end or date.today()
    result_date = pd.to_datetime(signal["結果日"])
    signal = signal[
        (result_date >= pd.Timestamp(settings.start))
        & (result_date <= pd.Timestamp(end))
    ].copy()
    signal["next_range_ratio"] = pd.to_numeric(signal["next_range_ratio"], errors="coerce")
    model_columns = [*BASELINE_FEATURES, *FEATURES.keys(), "next_range_ratio", "劇烈以上"]
    for column in model_columns:
        signal[column] = pd.to_numeric(signal[column], errors="coerce")
    # All combinations use the exact same complete-case rows for fair comparison.
    signal = signal.dropna(subset=model_columns + ["結果日"]).copy()
    signal = signal[signal["next_range_ratio"] > 0]
    signal["結果年"] = pd.to_datetime(signal["結果日"]).dt.year
    return signal.reset_index(drop=True)


def _feature_subsets() -> list[tuple[str, ...]]:
    keys = tuple(FEATURES)
    return [
        subset
        for size in range(len(keys) + 1)
        for subset in itertools.combinations(keys, size)
    ]


def _model_name(features: tuple[str, ...]) -> str:
    return "基準模型" if not features else "基準＋" + "＋".join(features)


def _score_predictions(
    actual_ratio: np.ndarray,
    predicted_ratio: np.ndarray,
    actual_event: np.ndarray,
    predicted_probability: np.ndarray,
) -> dict[str, float]:
    top_n = max(1, int(math.ceil(len(actual_event) * 0.20)))
    top_idx = np.argsort(predicted_probability)[-top_n:]
    event_rate = float(np.mean(actual_event))
    try:
        rho = float(spearmanr(actual_ratio, predicted_ratio).statistic)
    except Exception:
        rho = math.nan
    auc = (
        float(roc_auc_score(actual_event, predicted_probability))
        if len(np.unique(actual_event)) == 2 else math.nan
    )
    ap = (
        float(average_precision_score(actual_event, predicted_probability))
        if len(np.unique(actual_event)) == 2 else math.nan
    )
    return {
        "樣本數": int(len(actual_ratio)),
        "MAE_range_ratio": float(mean_absolute_error(actual_ratio, predicted_ratio)),
        "RMSE_range_ratio": float(mean_squared_error(actual_ratio, predicted_ratio) ** 0.5),
        "Spearman_pred_actual": rho,
        "Brier_劇烈以上": float(brier_score_loss(actual_event, predicted_probability)),
        "LogLoss_劇烈以上": float(log_loss(actual_event, predicted_probability, labels=[0, 1])),
        "ROC_AUC_劇烈以上": auc,
        "PR_AUC_劇烈以上": ap,
        "母體劇烈率_pct": event_rate * 100.0,
        "預測風險前20pct劇烈率_pct": float(np.mean(actual_event[top_idx])) * 100.0,
        "前20pct劇烈率Lift": float(np.mean(actual_event[top_idx]) / event_rate) if event_rate else math.nan,
    }


def run_walk_forward(
    frame: pd.DataFrame,
    settings: Settings,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    coefficient_rows: list[dict[str, Any]] = []
    subsets = _feature_subsets()
    test_years = sorted(
        int(year) for year in frame["結果年"].unique()
        if int(year) >= settings.first_test_year
    )
    if not test_years:
        raise ValueError("沒有可用的測試年度；請確認資料日期或調整 --first-test-year。")

    date_values = pd.to_datetime(frame["結果日"])
    y_all = frame["next_range_ratio"].to_numpy(dtype=float)
    event_all = frame["劇烈以上"].to_numpy(dtype=int)
    for year in test_years:
        train_mask = date_values.dt.year < year
        test_mask = date_values.dt.year == year
        train = frame.loc[train_mask]
        test = frame.loc[test_mask]
        if len(train) < settings.minimum_train_size or test.empty:
            continue
        y_train = train["next_range_ratio"].to_numpy(dtype=float)
        y_test = test["next_range_ratio"].to_numpy(dtype=float)
        event_train = train["劇烈以上"].to_numpy(dtype=int)
        event_test = test["劇烈以上"].to_numpy(dtype=int)

        year_predictions: dict[str, tuple[np.ndarray, np.ndarray]] = {}
        for subset in subsets:
            name = _model_name(subset)
            columns = [*BASELINE_FEATURES, *subset]
            x_train = train[columns].to_numpy(dtype=float)
            x_test = test[columns].to_numpy(dtype=float)

            regression = make_pipeline(StandardScaler(), Ridge(alpha=settings.ridge_alpha))
            regression.fit(x_train, np.log(y_train))
            pred_ratio = np.exp(regression.predict(x_test))

            classifier = make_pipeline(
                StandardScaler(),
                LogisticRegression(C=settings.logistic_c, max_iter=2000, solver="lbfgs"),
            )
            if np.unique(event_train).size < 2:
                probability = np.full(len(test), float(np.mean(event_train)))
            else:
                classifier.fit(x_train, event_train)
                probability = classifier.predict_proba(x_test)[:, 1]
            year_predictions[name] = (pred_ratio, probability)

            metrics = _score_predictions(y_test, pred_ratio, event_test, probability)
            rows.append({"測試年": year, "模型": name, "特徵數": len(subset),
                         "特徵組合": "、".join(subset) if subset else "僅近期振幅基準",
                         "訓練樣本數": len(train), **metrics})

            if subset in (tuple(FEATURES),):
                reg = regression.named_steps["ridge"]
                for feature, coefficient in zip(columns, reg.coef_):
                    coefficient_rows.append({
                        "測試年": year, "模型": "基準＋全部7特徵",
                        "結果": "log_range_ratio", "特徵": feature,
                        "標準化係數": float(coefficient),
                    })
                if np.unique(event_train).size == 2:
                    clf = classifier.named_steps["logisticregression"]
                    for feature, coefficient in zip(columns, clf.coef_[0]):
                        coefficient_rows.append({
                            "測試年": year, "模型": "基準＋全部7特徵",
                            "結果": "劇烈以上_log_odds", "特徵": feature,
                            "標準化係數": float(coefficient),
                        })

        # Keep per-year comparisons anchored to the same baseline fold.
        baseline = next(row for row in rows if row["測試年"] == year and row["模型"] == "基準模型")
        for row in (r for r in rows if r["測試年"] == year):
            row["MAE改善_pct_vs基準"] = (
                (baseline["MAE_range_ratio"] - row["MAE_range_ratio"])
                / baseline["MAE_range_ratio"] * 100.0
                if baseline["MAE_range_ratio"] else math.nan
            )
            row["Brier改善_vs基準"] = baseline["Brier_劇烈以上"] - row["Brier_劇烈以上"]

    yearly = pd.DataFrame(rows)
    if yearly.empty:
        raise ValueError("沒有產生 walk-forward 結果；請檢查訓練樣本門檻及日期範圍。")
    summary = (
        yearly.groupby(["模型", "特徵數", "特徵組合"], as_index=False)
        .agg(
            測試年度數=("測試年", "nunique"),
            年平均MAE=("MAE_range_ratio", "mean"),
            年平均RMSE=("RMSE_range_ratio", "mean"),
            年平均Spearman=("Spearman_pred_actual", "mean"),
            年平均MAE改善_pct=("MAE改善_pct_vs基準", "mean"),
            MAE勝過基準年度數=("MAE改善_pct_vs基準", lambda s: int((s > 0).sum())),
            年平均Brier=("Brier_劇烈以上", "mean"),
            年平均Brier改善=("Brier改善_vs基準", "mean"),
            年平均ROC_AUC=("ROC_AUC_劇烈以上", "mean"),
            年平均PR_AUC=("PR_AUC_劇烈以上", "mean"),
            年平均前20pct劇烈率Lift=("前20pct劇烈率Lift", "mean"),
        )
    )
    summary["振幅排名"] = summary["年平均MAE"].rank(method="min", ascending=True).astype(int)
    summary["劇烈率排名"] = summary["年平均Brier"].rank(method="min", ascending=True).astype(int)

    full_name = _model_name(tuple(FEATURES))
    ablation_rows = []
    for feature in FEATURES:
        reduced = tuple(item for item in FEATURES if item != feature)
        reduced_name = _model_name(reduced)
        full_years = yearly[yearly["模型"] == full_name].set_index("測試年")
        reduced_years = yearly[yearly["模型"] == reduced_name].set_index("測試年")
        common = full_years.index.intersection(reduced_years.index)
        if not len(common):
            continue
        for year in common:
            full = full_years.loc[year]
            less = reduced_years.loc[year]
            ablation_rows.append({
                "移除特徵": feature,
                "測試年": int(year),
                "移除後_MAE減全模型_MAE": float(less["MAE_range_ratio"] - full["MAE_range_ratio"]),
                "移除後_Brier減全模型_Brier": float(less["Brier_劇烈以上"] - full["Brier_劇烈以上"]),
                "移除後AUC減全模型AUC": float(less["ROC_AUC_劇烈以上"] - full["ROC_AUC_劇烈以上"]),
            })
    ablation = pd.DataFrame(ablation_rows)
    if not ablation.empty:
        ablation = (
            ablation.groupby("移除特徵", as_index=False)
            .agg(
                測試年度數=("測試年", "nunique"),
                平均MAE變化=("移除後_MAE減全模型_MAE", "mean"),
                平均Brier變化=("移除後_Brier減全模型_Brier", "mean"),
                平均AUC變化=("移除後AUC減全模型AUC", "mean"),
                移除後MAE改善年度數=("移除後_MAE減全模型_MAE", lambda s: int((s < 0).sum())),
                移除後Brier改善年度數=("移除後_Brier減全模型_Brier", lambda s: int((s < 0).sum())),
            )
        )
        ablation["解讀"] = np.where(
            (ablation["平均MAE變化"] < 0) & (ablation["平均Brier變化"] < 0),
            "移除此特徵後兩項平均指標都改善，可能互相干擾或增加雜訊",
            np.where(
                (ablation["平均MAE變化"] > 0) & (ablation["平均Brier變化"] > 0),
                "保留此特徵有助於兩項平均指標",
                "對不同目標的影響不一致",
            ),
        )
    return yearly, summary, ablation, pd.DataFrame(coefficient_rows)


def build_feature_correlation(frame: pd.DataFrame) -> pd.DataFrame:
    corr = frame[list(FEATURES)].corr(method="spearman")
    rows = []
    names = list(FEATURES)
    for i, first in enumerate(names):
        for second in names[i + 1:]:
            rows.append({
                "特徵A": first,
                "特徵B": second,
                "Spearman相關": float(corr.loc[first, second]),
                "可能高度相關": abs(float(corr.loc[first, second])) >= 0.70,
            })
    return pd.DataFrame(rows).sort_values("Spearman相關", key=lambda s: s.abs(), ascending=False)


def export_results(
    frame: pd.DataFrame,
    yearly: pd.DataFrame,
    summary: pd.DataFrame,
    ablation: pd.DataFrame,
    coefficients: pd.DataFrame,
    correlation: pd.DataFrame,
    settings: Settings,
) -> tuple[Path, ...]:
    out = settings.output_dir
    out.mkdir(parents=True, exist_ok=True)
    end = settings.end or date.today()
    suffix = f"{settings.start:%Y%m%d}_{end:%Y%m%d}"
    parts = (
        ("volatility_model_combo_summary", summary),
        ("volatility_model_walk_forward_yearly", yearly),
        ("volatility_model_feature_ablation", ablation),
        ("volatility_model_feature_correlation", correlation),
        ("volatility_model_full_coefficients", coefficients),
        ("volatility_model_detail", frame),
    )
    paths: list[Path] = []
    for stem, data in parts:
        path = out / f"{stem}_{suffix}.csv"
        data.to_csv(path, index=False, encoding="utf-8-sig")
        paths.append(path)
    manifest_path = out / f"volatility_model_manifest_{suffix}.json"
    manifest_path.write_text(json.dumps({
        "analysis_name": ANALYSIS_NAME,
        "metric_version": METRIC_VERSION,
        "run_id": str(uuid.uuid4()),
        "settings": {**asdict(settings), "start": settings.start.isoformat(),
                     "end": settings.end.isoformat() if settings.end else None},
        "source_view": SOURCE_VIEW,
        "target_view": TARGET_VIEW,
        "baseline_features": list(BASELINE_FEATURES),
        "candidate_features": FEATURES,
        "targets": TARGETS,
        "combination_count_including_baseline": 2 ** len(FEATURES),
        "validation": {
            "method": "年度 expanding walk-forward；每年只用該結果年度以前樣本訓練",
            "first_test_year": settings.first_test_year,
            "regression": "StandardScaler + Ridge(alpha fixed); fit log(next_range_ratio)",
            "classification": "StandardScaler + LogisticRegression(L2, C fixed); predict 劇烈以上",
            "common_sample": "所有模型使用候選特徵、基準及目標皆完整的共同樣本",
            "ranking": "振幅以跨測試年度平均MAE排序；劇烈事件以跨年度平均Brier排序，另報AUC與前20%風險Lift",
        },
        "caveats": [
            "特徵組合排名是探索結果；同時比較127種組合會有選擇偏誤，排名第一不等於可部署",
            "既有指標發現來自同一歷史區間，walk-forward評估仍不能替代未來新資料的前瞻驗證",
            "高特徵相關只代表資訊重疊，不等同互相矛盾；需與移除特徵消融結果一起看",
            "維持率三日急降是三個交易日前減當日的淨變化，不要求連續三天下降",
            "本版預測振幅，不預測漲跌方向",
        ],
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    paths.append(manifest_path)
    return tuple(paths)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="七項候選特徵的隔日日盤波動 walk-forward 組合比較")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--first-test-year", type=int, default=2023)
    parser.add_argument("--history-window", type=int, default=252)
    parser.add_argument("--minimum-history", type=int, default=60)
    parser.add_argument("--minimum-train-size", type=int, default=500)
    parser.add_argument("--ridge-alpha", type=float, default=10.0)
    parser.add_argument("--logistic-c", type=float, default=1.0)
    parser.add_argument("--output-dir", type=Path, default=Path("../output/analysis/futures_volatility_combo_model"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = Settings(
        start=args.start, end=args.end, futures_id=args.futures_id,
        first_test_year=args.first_test_year, history_window=args.history_window,
        minimum_history=args.minimum_history, minimum_train_size=args.minimum_train_size,
        ridge_alpha=args.ridge_alpha, logistic_c=args.logistic_c, output_dir=args.output_dir,
    )
    settings.validate()
    source, targets, maintenance = load_inputs(settings)
    frame = prepare_model_frame(source, targets, maintenance, settings)
    if frame.empty:
        raise ValueError("共同完整樣本為空；請檢查候選欄位、維持率與外資資料涵蓋區間。")
    yearly, summary, ablation, coefficients = run_walk_forward(frame, settings)
    correlation = build_feature_correlation(frame)
    paths = export_results(frame, yearly, summary, ablation, coefficients, correlation, settings)
    print(f"共同完整樣本：{len(frame):,} 筆")
    print(f"測試年度：{', '.join(map(str, sorted(yearly['測試年'].unique())))}")
    print("振幅預測前10名（以年平均MAE排序）：")
    display = summary.sort_values("年平均MAE").head(10)
    print(display[["模型", "特徵組合", "年平均MAE", "年平均MAE改善_pct", "MAE勝過基準年度數"]].to_string(index=False))
    print("輸出檔案：")
    for path in paths:
        print(f"  {path}")


if __name__ == "__main__":
    main()
