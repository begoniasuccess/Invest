from __future__ import annotations

import argparse
import json
import math
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency, mannwhitneyu, spearmanr

try:
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (
        accuracy_score,
        balanced_accuracy_score,
        brier_score_loss,
        log_loss,
        roc_auc_score,
    )
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
except ImportError as exc:  # pragma: no cover - clear runtime diagnostic
    raise SystemExit("需要 scikit-learn，請先執行 pip install scikit-learn") from exc


SOURCE_VIEW = "report.v_futures_report_daily_corrected_turning"
SIGNALS = (
    "主力新建多單",
    "主力新建空單",
    "主力淨多空和_5日",
    "主力淨多空和_10日",
    "主力淨多空和_20日",
    "主力淨多空和_60日",
)
MIN_TEST_N = 30


def load_source(start: date, end: date | None, futures_id: str) -> pd.DataFrame:
    """載入訊號日欄位，並多取 end 之後資料以對齊下一個交易日。"""
    from common import db

    columns = ", ".join(f'"{column}"' for column in (
        "日期", *SIGNALS, "日盤開", "日盤高", "日盤低", "日盤收",
    ))
    sql = f"""SELECT futures_id, {columns}
              FROM {SOURCE_VIEW}
              WHERE futures_id = ? AND "日期" >= ?"""
    params: list[object] = [futures_id, start]
    if end:
        sql += ' AND "日期" <= ?'
        params.append(end + pd.Timedelta(days=14))
    sql += ' ORDER BY "日期"'
    return db.sql_to_df(sql, params=tuple(params), parse_dates=["日期"])


def _number(frame: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_numeric(frame[name], errors="coerce").astype(float)


def prepare_detail(source: pd.DataFrame, start: date, end: date | None) -> pd.DataFrame:
    frame = source.sort_values("日期").drop_duplicates("日期", keep="last").reset_index(drop=True).copy()
    for name in SIGNALS:
        if name not in frame:
            raise KeyError(f"期貨 View 缺少欄位：{name}")
        frame[name] = _number(frame, name)

    next_open = _number(frame, "日盤開").shift(-1)
    next_high = _number(frame, "日盤高").shift(-1)
    next_low = _number(frame, "日盤低").shift(-1)
    next_close = _number(frame, "日盤收").shift(-1)
    frame["隔日日期"] = pd.to_datetime(frame["日期"]).shift(-1)
    frame["隔日開收報酬_pct"] = (next_close - next_open).div(next_open.where(next_open != 0)) * 100
    frame["隔日方向"] = np.where(
        frame["隔日開收報酬_pct"].notna() & frame["隔日開收報酬_pct"].ne(0),
        (frame["隔日開收報酬_pct"] > 0).astype(float), np.nan,
    )
    frame["隔日方向標籤"] = np.select(
        [frame["隔日方向"].eq(1), frame["隔日方向"].eq(0)], ["上漲", "下跌"], default=None,
    )
    frame["隔日高低振幅點"] = next_high - next_low
    signal_dates = pd.to_datetime(frame["日期"]).dt.date
    keep = (signal_dates >= start) & frame["隔日日期"].notna()
    if end:
        keep &= signal_dates <= end
    return frame.loc[keep].reset_index(drop=True)


def _bh_adjust(p_values: pd.Series) -> pd.Series:
    values = pd.to_numeric(p_values, errors="coerce")
    result = pd.Series(np.nan, index=values.index, dtype=float)
    valid = values.notna()
    if not valid.any():
        return result
    p = values.loc[valid].to_numpy()
    order = np.argsort(p)
    ranked = p[order] * len(p) / np.arange(1, len(p) + 1)
    adjusted = np.minimum.accumulate(ranked[::-1])[::-1]
    adjusted = np.clip(adjusted, 0, 1)
    back = np.empty_like(adjusted)
    back[order] = adjusted
    result.loc[valid] = back
    return result


def _safe_spearman(x: pd.Series, y: pd.Series) -> tuple[float, float, int]:
    data = pd.concat([x, y], axis=1).apply(pd.to_numeric, errors="coerce").dropna()
    if len(data) < MIN_TEST_N or data.iloc[:, 0].nunique() < 2 or data.iloc[:, 1].nunique() < 2:
        return math.nan, math.nan, len(data)
    rho, p = spearmanr(data.iloc[:, 0], data.iloc[:, 1])
    return float(rho), float(p), len(data)


def _binary_difference(group_a: pd.Series, group_b: pd.Series) -> tuple[float, float, int, int, float, float]:
    """回傳 P(up|A)-P(up|B)、p、樣本數與兩組上漲率。"""
    a = group_a.dropna().astype(int)
    b = group_b.dropna().astype(int)
    if min(len(a), len(b)) < MIN_TEST_N:
        return math.nan, math.nan, len(a), len(b), math.nan, math.nan
    table = np.array([
        [(a == 1).sum(), (a == 0).sum()],
        [(b == 1).sum(), (b == 0).sum()],
    ])
    if (table.sum(axis=0) == 0).any() or (table.sum(axis=1) == 0).any():
        p = math.nan
    else:
        p = float(chi2_contingency(table, correction=True).pvalue)
    p_a = float(a.mean() * 100)
    p_b = float(b.mean() * 100)
    return p_a - p_b, p, len(a), len(b), p_a, p_b


def _make_variants(detail: pd.DataFrame) -> dict[str, pd.Series]:
    variants: dict[str, pd.Series] = {}
    for name in SIGNALS:
        x = detail[name]
        variants[f"{name}｜原始值"] = x
        variants[f"{name}｜絕對值"] = x.abs()
        variants[f"{name}｜正值部分"] = x.clip(lower=0)
        variants[f"{name}｜負值幅度"] = (-x).clip(lower=0)
        variants[f"{name}｜正負符號"] = np.sign(x)
    return variants


def scan_univariate(detail: pd.DataFrame, period_name: str) -> pd.DataFrame:
    """測連續方向、絕對幅度、單邊幅度與正負條件，不測波動大小。"""
    rows: list[dict[str, object]] = []
    target = detail["隔日開收報酬_pct"]
    up = detail["隔日方向"]
    for name in SIGNALS:
        x = detail[name]
        variants = {
            "原始值對隔日報酬": x,
            "絕對值對隔日報酬": x.abs(),
            "正值部分對隔日報酬": x.clip(lower=0),
            "負值幅度對隔日報酬": (-x).clip(lower=0),
            "正負符號對隔日報酬": np.sign(x),
        }
        for test_name, values in variants.items():
            effect, p, n = _safe_spearman(pd.Series(values, index=detail.index), target)
            rows.append({
                "期間": period_name, "欄位": name, "檢定": test_name,
                "樣本數": n, "效果值_Spearman_rho": effect,
                "隔日報酬方向": "偏多" if effect > 0 else ("偏空" if effect < 0 else "無"),
                "p值": p,
            })

        # 正值相對非正值、負值相對非負值：直接檢驗「只有單邊有作用」的想法。
        valid_signal = x.notna()
        pos = x.gt(0) & valid_signal
        neg = x.lt(0) & valid_signal
        nonpos = x.le(0) & valid_signal
        nonneg = x.ge(0) & valid_signal
        for label, mask_a, mask_b in (
            ("正值 vs 非正值", pos, nonpos),
            ("負值 vs 非負值", neg, nonneg),
        ):
            diff, p, n1, n0, up1, up0 = _binary_difference(up[mask_a], up[mask_b])
            rows.append({
                "期間": period_name, "欄位": name, "檢定": label,
                "樣本數": n1 + n0, "組別A_n": n1, "組別B_n": n0,
                "效果值_Spearman_rho": diff,
                "效果方向": "A組上漲率較高" if diff > 0 else ("B組上漲率較高" if diff < 0 else "無差異"),
                "A組隔日上漲率_pct": up1, "B組隔日上漲率_pct": up0, "p值": p,
            })

        # 分別檢查原始值與絕對值 Q1~Q5，能看非線性與極端值反轉。
        for transform, values in (("原始值", x), ("絕對值", x.abs())):
            valid = values.notna() & target.notna() & up.notna()
            if valid.sum() < MIN_TEST_N * 5:
                continue
            try:
                bins = pd.qcut(values[valid].rank(method="first"), 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"])
            except ValueError:
                continue
            grouped = pd.DataFrame({"bucket": bins, "return": target[valid], "up": up[valid]}).groupby("bucket", observed=True)
            q1 = pd.DataFrame({"bucket": bins, "return": target[valid], "up": up[valid]}).loc[bins == "Q1", "return"]
            q5 = pd.DataFrame({"bucket": bins, "return": target[valid], "up": up[valid]}).loc[bins == "Q5", "return"]
            if min(len(q1), len(q5)) >= MIN_TEST_N:
                mw_p = float(mannwhitneyu(q5, q1, alternative="two-sided").pvalue)
                rows.append({
                    "期間": period_name, "欄位": name, "檢定": f"{transform} Q5-Q1隔日報酬",
                    "樣本數": len(q1) + len(q5), "效果值_Spearman_rho": float(q5.mean() - q1.mean()),
                    "效果方向": "Q5報酬較高" if q5.mean() > q1.mean() else "Q1報酬較高",
                    "Q1平均隔日報酬_pct": float(q1.mean()), "Q5平均隔日報酬_pct": float(q5.mean()),
                    "p值": mw_p,
                })
    result = pd.DataFrame(rows)
    if not result.empty:
        result["FDR_q值"] = _bh_adjust(result["p值"])
        result["FDR_5pct顯著"] = result["FDR_q值"] < 0.05
    return result


def build_group_summary(detail: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for name in SIGNALS:
        x = detail[name]
        for label, mask in (("負值", x < 0), ("零值", x == 0), ("正值", x > 0)):
            g = detail.loc[mask & detail["隔日方向"].notna()]
            if g.empty:
                continue
            rows.append({
                "欄位": name, "分組方式": "正負號", "組別": label,
                "樣本數": len(g), "隔日上漲率_pct": g["隔日方向"].mean() * 100,
                "隔日開收報酬平均_pct": g["隔日開收報酬_pct"].mean(),
                "隔日開收報酬中位數_pct": g["隔日開收報酬_pct"].median(),
            })
        for transform, values in (("原始值分位", x), ("絕對值分位", x.abs())):
            valid = values.notna() & detail["隔日方向"].notna()
            if valid.sum() < MIN_TEST_N * 5:
                continue
            bins = pd.qcut(values[valid].rank(method="first"), 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"])
            temp = detail.loc[valid, ["隔日方向", "隔日開收報酬_pct"]].copy()
            temp["分組"] = bins.astype(str).to_numpy()
            for bucket, g in temp.groupby("分組", sort=True):
                rows.append({
                    "欄位": name, "分組方式": transform, "組別": bucket,
                    "樣本數": len(g), "隔日上漲率_pct": g["隔日方向"].mean() * 100,
                    "隔日開收報酬平均_pct": g["隔日開收報酬_pct"].mean(),
                    "隔日開收報酬中位數_pct": g["隔日開收報酬_pct"].median(),
                })
    return pd.DataFrame(rows)


def _feature_sets() -> dict[str, list[str]]:
    sets: dict[str, list[str]] = {}
    for name in SIGNALS:
        sets[f"單欄｜{name}｜原始"] = [f"raw::{name}"]
        sets[f"單欄｜{name}｜絕對值"] = [f"abs::{name}"]
        sets[f"單欄｜{name}｜正負拆分"] = [f"positive::{name}", f"negative::{name}"]
    sets["組合｜六欄原始值"] = [f"raw::{name}" for name in SIGNALS]
    sets["組合｜六欄絕對值"] = [f"abs::{name}" for name in SIGNALS]
    sets["組合｜六欄正負拆分"] = [f"{side}::{name}" for name in SIGNALS for side in ("positive", "negative")]
    sets["組合｜原始值加正負拆分"] = [
        *[f"raw::{name}" for name in SIGNALS],
        *[f"{side}::{name}" for name in SIGNALS for side in ("positive", "negative")],
    ]
    return sets


def _model_matrix(detail: pd.DataFrame) -> pd.DataFrame:
    data: dict[str, pd.Series] = {}
    for name in SIGNALS:
        x = detail[name]
        data[f"raw::{name}"] = x
        data[f"abs::{name}"] = x.abs()
        data[f"positive::{name}"] = x.clip(lower=0)
        data[f"negative::{name}"] = (-x).clip(lower=0)
    return pd.DataFrame(data, index=detail.index)


def evaluate_models(detail: pd.DataFrame, split_date: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    model_data = _model_matrix(detail)
    labels = detail["隔日方向"]
    dates = pd.to_datetime(detail["日期"]).dt.date
    train_mask = dates < split_date
    test_mask = (dates >= split_date) & labels.notna()
    train_mask &= labels.notna()
    rows: list[dict[str, object]] = []
    predictions: list[pd.DataFrame] = []
    if train_mask.sum() < 100 or test_mask.sum() < MIN_TEST_N:
        return pd.DataFrame([{"錯誤": "訓練期或驗證期有效樣本不足"}]), pd.DataFrame()

    y_train = labels.loc[train_mask].astype(int)
    y_test = labels.loc[test_mask].astype(int)
    base_probability = float(y_train.mean())
    base_prediction = np.full(len(y_test), int(base_probability >= 0.5))
    rows.append({
        "模型": "基準｜訓練期多數方向", "欄位數": 0,
        "訓練樣本數": len(y_train), "驗證樣本數": len(y_test),
        "訓練期上漲率_pct": base_probability * 100,
        "驗證期上漲率_pct": y_test.mean() * 100,
        "驗證準確率_pct": accuracy_score(y_test, base_prediction) * 100,
        "驗證平衡準確率_pct": balanced_accuracy_score(y_test, base_prediction) * 100,
        "驗證AUC": math.nan, "驗證Brier": brier_score_loss(y_test, np.full(len(y_test), base_probability)),
        "驗證LogLoss": log_loss(y_test, np.full(len(y_test), base_probability), labels=[0, 1]),
    })
    for model_name, columns in _feature_sets().items():
        x_train = model_data.loc[train_mask, columns]
        x_test = model_data.loc[test_mask, columns]
        model = make_pipeline(
            SimpleImputer(strategy="median", add_indicator=True),
            StandardScaler(),
            LogisticRegression(C=1.0, max_iter=2000, class_weight="balanced", random_state=17),
        )
        model.fit(x_train, y_train)
        probability = model.predict_proba(x_test)[:, 1]
        prediction = (probability >= 0.5).astype(int)
        rows.append({
            "模型": model_name, "欄位數": len(columns),
            "訓練樣本數": len(y_train), "驗證樣本數": len(y_test),
            "訓練期上漲率_pct": base_probability * 100,
            "驗證期上漲率_pct": y_test.mean() * 100,
            "驗證準確率_pct": accuracy_score(y_test, prediction) * 100,
            "驗證平衡準確率_pct": balanced_accuracy_score(y_test, prediction) * 100,
            "驗證AUC": roc_auc_score(y_test, probability) if y_test.nunique() > 1 else math.nan,
            "驗證Brier": brier_score_loss(y_test, probability),
            "驗證LogLoss": log_loss(y_test, probability, labels=[0, 1]),
            "準確率較基準差_pct": (accuracy_score(y_test, prediction) - accuracy_score(y_test, base_prediction)) * 100,
            "平衡準確率較50%差_pct": (balanced_accuracy_score(y_test, prediction) - 0.5) * 100,
        })
        pred = pd.DataFrame({
            "訊號日": detail.loc[test_mask, "日期"].to_numpy(),
            "隔日日期": detail.loc[test_mask, "隔日日期"].to_numpy(),
            "模型": model_name, "實際隔日方向": y_test.to_numpy(),
            "預測上漲機率": probability, "預測方向": prediction,
        })
        predictions.append(pred)
    result = pd.DataFrame(rows).sort_values(
        ["驗證AUC", "驗證平衡準確率_pct"], ascending=[False, False], na_position="last"
    ).reset_index(drop=True)
    return result, pd.concat(predictions, ignore_index=True) if predictions else pd.DataFrame()


def main() -> None:
    parser = argparse.ArgumentParser(description="期貨主力新建部位與隔日漲跌方向回測")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--split-date", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--futures-id", default="TX")
    parser.add_argument("--output-dir", type=Path, default=Path("../output/backtest/futures_main_position_direction_backtest"))
    args = parser.parse_args()

    source = load_source(args.start, args.end, args.futures_id)
    detail = prepare_detail(source, args.start, args.end)
    dates = pd.to_datetime(detail["日期"]).dt.date
    train = detail[dates < args.split_date].copy()
    validation = detail[dates >= args.split_date].copy()
    tests = pd.concat([
        scan_univariate(detail, "全期間"),
        scan_univariate(train, f"探索期_至{args.split_date.isoformat()}前"),
        scan_univariate(validation, f"驗證期_{args.split_date.isoformat()}起"),
    ], ignore_index=True)
    groups = build_group_summary(detail)
    models, predictions = evaluate_models(detail, args.split_date)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    end_label = args.end.strftime("%Y%m%d") if args.end else "latest"
    suffix = f"{args.start:%Y%m%d}_{end_label}"
    outputs = {
        "detail": detail.to_csv(args.output_dir / f"main_position_direction_detail_{suffix}.csv", index=False, encoding="utf-8-sig"),
        "tests": tests.to_csv(args.output_dir / f"main_position_direction_univariate_tests_{suffix}.csv", index=False, encoding="utf-8-sig"),
        "groups": groups.to_csv(args.output_dir / f"main_position_direction_groups_{suffix}.csv", index=False, encoding="utf-8-sig"),
        "models": models.to_csv(args.output_dir / f"main_position_direction_models_{suffix}.csv", index=False, encoding="utf-8-sig"),
        "predictions": predictions.to_csv(args.output_dir / f"main_position_direction_predictions_{suffix}.csv", index=False, encoding="utf-8-sig"),
    }
    manifest = {
        "analysis": "期貨View主力部位欄位對隔日漲跌方向",
        "source_view": SOURCE_VIEW,
        "futures_id": args.futures_id,
        "start": args.start.isoformat(),
        "end": args.end.isoformat() if args.end else None,
        "split_date": args.split_date.isoformat(),
        "signals": list(SIGNALS),
        "target": "下一個實際交易日（日盤收-日盤開）/日盤開；正報酬=上漲、負報酬=下跌；平盤不納入方向分類",
        "univariate_tests": ["原始值、絕對值、正值部分、負值幅度、正負符號與隔日報酬的Spearman", "正值/非正值與負值/非負值的隔日上漲率差", "原始值與絕對值Q5-Q1隔日報酬差"],
        "combination_models": "訓練期擬合的標準化、平衡類別 Logistic Regression；2023年起樣本外評估；缺值中位數只由訓練期估計",
        "warnings": ["探索期與驗證期依訊號日切分；不使用隔日資料形成特徵", "方向統計不代表可交易獲利，尚未計入手續費、滑價與進出場規則", "Logistic模型用於比較特徵組合，不保證最佳參數或未來績效", "對多種假設做FDR修正，仍應以獨立新期間再驗證"],
    }
    (args.output_dir / f"main_position_direction_manifest_{suffix}.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"有效訊號日：{len(detail):,}；探索期：{len(train):,}；驗證期：{len(validation):,}")
    print(f"單因子檢定：{len(tests):,}；FDR顯著：{int(tests.get('FDR_5pct顯著', pd.Series(dtype=bool)).fillna(False).sum()):,}")
    print(f"模型比較輸出：{args.output_dir / f'main_position_direction_models_{suffix}.csv'}")


if __name__ == "__main__":
    main()
