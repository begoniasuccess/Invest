"""
用 XGBoost 分位數回歸同時預測「下一交易日日盤最高／最低漲跌幅」。

執行：
    python -m main.futures_predict

定義：
    明日最高漲幅_pct = (明日日盤高 / 今日日盤收 - 1) * 100
    明日最低漲跌幅_pct = (明日日盤低 / 今日日盤收 - 1) * 100

輸出目錄 futures_predict_output：
    model_summary.csv       模型、基準模型的樣本外評估
    test_predictions.csv    測試集逐日預測
    feature_importance.csv  最高／最低 Q50 模型的重要特徵
    latest_prediction.csv   全歷史資料重訓後的正式最新預測
    expanding_backtest.csv  每 60 個交易日重訓一次的擴展式回測

套件：
    pip install -U pandas numpy scikit-learn xgboost
    分位數回歸需要 xgboost >= 2.0。
"""

from pathlib import Path
import hashlib
import json
import sys

import numpy as np
import pandas as pd

from common import db

try:
    import xgboost as xgb
    from xgboost import XGBRegressor
except ImportError as exc:
    raise SystemExit(
        "尚未安裝 xgboost，請先執行：pip install -U xgboost"
    ) from exc

from sklearn.metrics import mean_absolute_error, mean_squared_error


# =========================================================
# 可調整參數
# =========================================================
# 對外報表會隱藏部分模型仍需使用的欄位；預測固定讀內部完整 view。
VIEW_NAME = "report.v_futures_report_daily_model"
FUTURES_ID = "TX"
OUTPUT_DIR = Path(
    "C:/Users/USER/Desktop/Han/學習/金融策略/Invest/output/futures_predict_output"
)

# 按時間排序後：前 70% 訓練、中間 15% 驗證、最後 15% 測試。
TRAIN_RATIO = 0.70
VALID_RATIO = 0.15

# 原驗證集前段用於 early stopping，後段獨立用於分位數校準。
TUNING_SHARE_OF_VALID = 0.50

# Q90 使用最近已揭曉的樣本外殘差做動態校準；不會使用預測當日或未來答案。
ROLLING_CALIBRATION_WINDOW = 500

# 擴展式回測每隔多少個交易日，使用當時全部已知資料重新訓練。
EXPANDING_REFIT_EVERY = 60

# 日常只需要最新預測時設為 False；模型改版或定期檢驗時再設為 True。
# False 時不執行擴展式回測，也不覆寫既有 expanding_backtest.csv。
RUN_EXPANDING_BACKTEST = False

# 外圍區間 Q10~Q90（理論涵蓋 80%），另保留 Q50 中位數預測。
QUANTILES = (0.10, 0.50, 0.90)
RANDOM_STATE = 42

TARGETS = {
    "最高": {
        "target_col": "明日最高漲幅_pct",
        "actual_price_col": "明日日盤高",
        "actual_output_col": "實際明日最高",
        "point_suffix": "最高點",
        "tail_quantile": 0.90,
    },
    "最低": {
        "target_col": "明日最低漲跌幅_pct",
        "actual_price_col": "明日日盤低",
        "actual_output_col": "實際明日最低",
        "point_suffix": "最低點",
        "tail_quantile": 0.10,
    },
}

# 預測時點：T 日夜盤結束、T+1 日盤開盤前。
# 依目前資料定義，T 列夜盤代表 T 日晚上到 T+1 日清晨，因此直接使用 T 列，
# 不可再 shift(1)，否則會錯過最新一段夜盤資訊。
BASE_FEATURES = [
    "月", "週次",
    "日價漲跌_pct", "日收開_昨收_pct", "日收開_開盤_pct",
    "日振幅_昨收_pct", "日振幅_開盤_pct",
    "跳空缺口_昨收_pct", "實體_pct", "上影_pct", "下影_pct",
    "日量增減_昨日_pct", "日量增減_五日_pct",
    "日量增減_十日_pct", "日量增減_二十日_pct", "日量增減_六十日_pct",
    "日5均上升幅度_pct", "日10均上升幅度_pct",
    "日20均上升幅度_pct", "日60均上升幅度_pct",
    "日5_明日扣抵值影響_pct", "日10_明日扣抵值影響_pct",
    "日20_明日扣抵值影響_pct", "日60_明日扣抵值影響_pct",
    "日5MA乖離率", "日10MA乖離率", "日20MA乖離率", "日60MA乖離率",
]

NIGHT_RAW_COLUMNS = [
    "夜盤開", "夜盤收", "夜盤高", "夜盤低",
    "夜5MA", "夜10MA", "夜20MA", "夜60MA", "夜120MA",
    "夜成交量", "夜法人多單量", "夜法人空單量",
]


def _version_tuple(version: str) -> tuple[int, ...]:
    parts = []
    for item in version.split("."):
        digits = "".join(ch for ch in item if ch.isdigit())
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)


def load_data() -> pd.DataFrame:
    """讀取 view；目標以 shift(-1) 建立，保留最後一列供實際預測。"""
    df = db.query_to_df(
        f"""
        SELECT *
        FROM {VIEW_NAME}
        WHERE futures_id = ?
        ORDER BY 日期
        """,
        (FUTURES_ID,),
    )
    if df is None or df.empty:
        raise RuntimeError(f"{VIEW_NAME} 查無 {FUTURES_ID} 資料")

    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"]).sort_values("日期").drop_duplicates("日期")

    for col in ("日盤收", "日盤高", "日盤低"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["明日日期"] = df["日期"].shift(-1)
    df["明日日盤高"] = df["日盤高"].shift(-1)
    df["明日日盤低"] = df["日盤低"].shift(-1)
    df["明日最高漲幅_pct"] = (df["明日日盤高"] / df["日盤收"] - 1.0) * 100.0
    df["明日最低漲跌幅_pct"] = (df["明日日盤低"] / df["日盤收"] - 1.0) * 100.0
    return df.reset_index(drop=True)


def _ensure_derived_day_columns(df: pd.DataFrame) -> pd.DataFrame:
    """補回新版 View 可能不再直接輸出的日盤衍生欄位。

    計算方式與舊版 ``v_futures_report_daily`` 相同。只補不存在的欄位；
    若 View 仍有提供，則保留資料庫原值。
    """
    out = df.copy()
    required_ohlc = ("日盤開", "日盤收", "日盤高", "日盤低")
    missing_ohlc = [col for col in required_ohlc if col not in out.columns]
    if missing_ohlc:
        raise RuntimeError(
            f"{VIEW_NAME} 缺少必要日盤欄位：{missing_ohlc}；無法建立模型特徵"
        )

    for col in required_ohlc:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    group_key = out["futures_id"] if "futures_id" in out.columns else None

    def previous(values: pd.Series) -> pd.Series:
        return values.groupby(group_key, sort=False).shift(1) if group_key is not None else values.shift(1)

    if "昨日收盤" not in out.columns:
        out["昨日收盤"] = previous(out["日盤收"])
        print("[相容處理] view 未提供「昨日收盤」，已由前一交易日日盤收盤重建")
    else:
        out["昨日收盤"] = pd.to_numeric(out["昨日收盤"], errors="coerce")

    day_range = (out["日盤高"] - out["日盤低"]).replace(0, np.nan)
    body_top = out[["日盤開", "日盤收"]].max(axis=1)
    body_bottom = out[["日盤開", "日盤收"]].min(axis=1)

    derived = {
        "實體_pct": (out["日盤收"] - out["日盤開"]).abs() / day_range,
        "上影_pct": (out["日盤高"] - body_top).abs() / day_range,
        "下影_pct": (body_bottom - out["日盤低"]).abs() / day_range,
    }

    previous_high = previous(out["日盤高"])
    previous_low = previous(out["日盤低"])
    previous_body_top = previous(body_top)
    previous_body_bottom = previous(body_bottom)
    gap = pd.Series(np.nan, index=out.index, dtype="float64")
    up_gap = out["日盤低"] > previous_high
    down_gap = out["日盤高"] < previous_low
    gap.loc[up_gap] = body_bottom.loc[up_gap] - previous_body_top.loc[up_gap]
    gap.loc[down_gap] = body_top.loc[down_gap] - previous_body_bottom.loc[down_gap]
    derived["跳空缺口_昨收_pct"] = gap / out["昨日收盤"].replace(0, np.nan)

    rebuilt = []
    for col, values in derived.items():
        if col not in out.columns:
            out[col] = values
            rebuilt.append(col)
        else:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if rebuilt:
        print(f"[相容處理] 已由日盤 OHLC 重建 {len(rebuilt)} 個特徵：{rebuilt}")
    return out


def add_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """加入 T 日以前及 T 列夜盤特徵；所有 rolling 都只含 T 列及更早資料。"""
    out = _ensure_derived_day_columns(df)
    available = [col for col in BASE_FEATURES if col in out.columns]
    missing = [col for col in BASE_FEATURES if col not in out.columns]
    if missing:
        print(f"[提醒] view 缺少 {len(missing)} 個預設特徵，將略過：{missing}")

    for col in available:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # 自行從 OHLC 建立近期波動，讓預測區間可隨行情放大或縮小。
    for col in ("日盤高", "日盤低", "日盤收", "昨日收盤", "日成交量"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    prev_close = out["昨日收盤"].replace(0, np.nan)
    out["當日真實波幅_pct"] = (
        pd.concat(
            [
                out["日盤高"] - out["日盤低"],
                (out["日盤高"] - prev_close).abs(),
                (out["日盤低"] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        / prev_close
        * 100.0
    )
    out["日報酬_pct_自算"] = out["日盤收"].pct_change(fill_method=None) * 100.0

    engineered = []
    for window in (5, 10, 20, 60):
        atr_col = f"ATR{window}_pct"
        vol_col = f"近{window}日報酬標準差_pct"
        out[atr_col] = out["當日真實波幅_pct"].rolling(window, min_periods=window).mean()
        out[vol_col] = out["日報酬_pct_自算"].rolling(window, min_periods=window).std()
        engineered.extend([atr_col, vol_col])

    # 量只使用相對值，避免不同年代的絕對成交量尺度。
    if "日成交量" in out.columns:
        for window in (5, 20, 60):
            col = f"日成交量_近{window}日均量比"
            avg = out["日成交量"].rolling(window, min_periods=window).mean()
            out[col] = out["日成交量"] / avg.replace(0, np.nan)
            engineered.append(col)

    # T 列夜盤是 T 日晚上至 T+1 日清晨，在 T+1 日盤開盤前已完整可知。
    night_available = [col for col in NIGHT_RAW_COLUMNS if col in out.columns]
    night_missing = [col for col in NIGHT_RAW_COLUMNS if col not in out.columns]
    if night_missing:
        print(f"[提醒] view 缺少 {len(night_missing)} 個夜盤欄位，將略過：{night_missing}")
    for col in night_available:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    required_ohlc = {"夜盤開", "夜盤收", "夜盤高", "夜盤低"}
    if required_ohlc.issubset(out.columns):
        night_open = out["夜盤開"].replace(0, np.nan)
        day_close = out["日盤收"].replace(0, np.nan)
        night_features = {
            "夜盤開_日收_pct": (out["夜盤開"] / day_close - 1.0) * 100.0,
            "夜盤收_日收_pct": (out["夜盤收"] / day_close - 1.0) * 100.0,
            "夜盤高_日收_pct": (out["夜盤高"] / day_close - 1.0) * 100.0,
            "夜盤低_日收_pct": (out["夜盤低"] / day_close - 1.0) * 100.0,
            "夜盤實體_pct": (out["夜盤收"] / night_open - 1.0) * 100.0,
            "夜盤振幅_pct": (out["夜盤高"] - out["夜盤低"]) / night_open * 100.0,
            "夜盤上影_pct": (out["夜盤高"] - out[["夜盤開", "夜盤收"]].max(axis=1)) / night_open * 100.0,
            "夜盤下影_pct": (out[["夜盤開", "夜盤收"]].min(axis=1) - out["夜盤低"]) / night_open * 100.0,
        }
        for name, values in night_features.items():
            out[name] = values
            engineered.append(name)

    # 均線改用相對夜盤收盤的乖離率，避免指數長期由萬點升至數萬點的尺度問題。
    if "夜盤收" in out.columns:
        night_close = out["夜盤收"].replace(0, np.nan)
        for window in (5, 10, 20, 60, 120):
            source = f"夜{window}MA"
            if source in out.columns:
                name = f"夜{window}MA乖離率"
                out[name] = (night_close / out[source].replace(0, np.nan) - 1.0) * 100.0
                engineered.append(name)

    if "夜成交量" in out.columns:
        for window in (5, 20, 60):
            name = f"夜成交量_近{window}日均量比"
            avg = out["夜成交量"].rolling(window, min_periods=window).mean()
            out[name] = out["夜成交量"] / avg.replace(0, np.nan)
            engineered.append(name)

    if {"夜法人多單量", "夜法人空單量"}.issubset(out.columns):
        total = (out["夜法人多單量"] + out["夜法人空單量"]).replace(0, np.nan)
        out["夜法人淨多空量"] = out["夜法人多單量"] - out["夜法人空單量"]
        out["夜法人淨多空比_pct"] = out["夜法人淨多空量"] / total * 100.0
        engineered.extend(["夜法人淨多空量", "夜法人淨多空比_pct"])

    feature_cols = available + engineered
    out[feature_cols] = out[feature_cols].replace([np.inf, -np.inf], np.nan)
    return out, feature_cols


def time_split(df: pd.DataFrame):
    n = len(df)
    train_end = int(n * TRAIN_RATIO)
    valid_end = int(n * (TRAIN_RATIO + VALID_RATIO))
    if train_end < 200 or valid_end <= train_end or valid_end >= n:
        raise RuntimeError(f"有效樣本只有 {n} 筆，無法穩定切分；建議至少 300 筆")
    return df.iloc[:train_end], df.iloc[train_end:valid_end], df.iloc[valid_end:]


def make_model(
    alpha: float,
    n_estimators: int = 2000,
    use_early_stopping: bool = True,
) -> XGBRegressor:
    params = dict(
        objective="reg:quantileerror",
        quantile_alpha=alpha,
        n_estimators=n_estimators,
        learning_rate=0.02,
        max_depth=3,
        min_child_weight=8,
        subsample=0.80,
        colsample_bytree=0.80,
        reg_alpha=0.10,
        reg_lambda=2.0,
        tree_method="hist",
        random_state=RANDOM_STATE,
        # hist 在多執行緒環境可能因浮點加總順序不同而產生不同的樹。
        # 正式預測重現性比訓練速度重要，因此固定單執行緒。
        n_jobs=1,
    )
    if use_early_stopping:
        params["early_stopping_rounds"] = 100
    return XGBRegressor(**params)


def calibration_offset(y_true, y_pred, alpha: float) -> float:
    """以獨立校準集估計加法修正，使 P(y <= 預測Qα) 接近 α。"""
    residual = np.asarray(y_true) - np.asarray(y_pred)
    try:
        return float(np.quantile(residual, alpha, method="higher"))
    except TypeError:  # numpy < 1.22
        return float(np.quantile(residual, alpha, interpolation="higher"))


def rolling_offsets(
    seed_residuals,
    observed_residuals,
    alpha: float,
    window: int,
) -> np.ndarray:
    """逐期校準：第 i 筆只能使用 seed 與 i 之前已揭曉的殘差。"""
    history = list(np.asarray(seed_residuals, dtype=float))
    observed = np.asarray(observed_residuals, dtype=float)
    result = []
    for residual in observed:
        recent = np.asarray(history[-window:], dtype=float)
        result.append(calibration_offset(recent, np.zeros(len(recent)), alpha))
        history.append(float(residual))
    return np.asarray(result, dtype=float)


def dataframe_fingerprint(df: pd.DataFrame) -> str:
    """建立可跨次執行比較的資料指紋，不把完整資料塞進 run_info。"""
    normalized = df.copy()
    for col in normalized.select_dtypes(include=["datetime", "datetimetz"]).columns:
        normalized[col] = normalized[col].astype("string")
    row_hashes = pd.util.hash_pandas_object(normalized, index=True).to_numpy()
    return hashlib.sha256(row_hashes.tobytes()).hexdigest()


def expanding_backtest(
    labeled: pd.DataFrame,
    feature_cols: list[str],
    test_start: int,
    best_rounds: dict,
    q50_offset: float,
    target_name: str,
    target_col: str,
    tail_quantile: float,
) -> pd.DataFrame:
    """定期擴展訓練窗；每一區塊只使用該區塊開始日前已知的標籤。"""
    blocks = []
    tail_residual_history = []
    for start in range(test_start, len(labeled), EXPANDING_REFIT_EVERY):
        end = min(start + EXPANDING_REFIT_EVERY, len(labeled))
        history = labeled.iloc[:start]
        block = labeled.iloc[start:end]
        if history.empty or block.empty:
            continue

        result = pd.DataFrame({
            "預測基準日": block["日期"].dt.strftime("%Y-%m-%d"),
            "實際明日日期": block["明日日期"].dt.strftime("%Y-%m-%d"),
            "今日收盤": block["日盤收"].values,
            "預測目標": target_name,
            f"實際明日{target_name}漲跌幅_pct": block[target_col].values,
            "本區塊訓練資料迄日": history["日期"].max().strftime("%Y-%m-%d"),
        })
        raw_predictions = {}
        for alpha in QUANTILES:
            model = make_model(alpha, n_estimators=best_rounds[alpha], use_early_stopping=False)
            model.fit(history[feature_cols], history[target_col], verbose=False)
            raw_predictions[alpha] = model.predict(block[feature_cols])

        for alpha in QUANTILES:
            result[f"預測Q{int(alpha * 100):02d}_pct"] = raw_predictions[alpha]
        result["預測Q50_pct"] += q50_offset

        # 極端分位數逐日滾動校準：最高看 Q90，最低看 Q10。
        base_history = np.asarray(tail_residual_history[-ROLLING_CALIBRATION_WINDOW:], dtype=float)
        if len(base_history) < 30:
            warm_n = min(ROLLING_CALIBRATION_WINDOW, len(history))
            warm_train = history.iloc[:-warm_n]
            if warm_train.empty:
                warm_train = history.iloc[:max(1, len(history) // 2)]
                warm_tail = history.iloc[len(warm_train):]
            else:
                warm_tail = history.iloc[-warm_n:]
            warm_model = make_model(
                tail_quantile,
                n_estimators=best_rounds[tail_quantile],
                use_early_stopping=False,
            )
            warm_model.fit(
                warm_train[feature_cols], warm_train[target_col],
                verbose=False,
            )
            base_history = (
                warm_tail[target_col].to_numpy()
                - warm_model.predict(warm_tail[feature_cols])
            )
        block_residuals = block[target_col].to_numpy() - raw_predictions[tail_quantile]
        tail_add = rolling_offsets(
            base_history, block_residuals, tail_quantile, ROLLING_CALIBRATION_WINDOW
        )
        tail_label = f"Q{int(tail_quantile * 100):02d}"
        result[f"{tail_label}近期滾動校準加值_pct百分點"] = tail_add
        result[f"預測{tail_label}_pct"] = raw_predictions[tail_quantile] + tail_add
        tail_residual_history.extend(block_residuals.tolist())

        q_cols = ["預測Q10_pct", "預測Q50_pct", "預測Q90_pct"]
        result[q_cols] = np.sort(result[q_cols].to_numpy(), axis=1)
        result["Q10_Q90是否涵蓋實際"] = (
            (result[f"實際明日{target_name}漲跌幅_pct"] >= result["預測Q10_pct"])
            & (result[f"實際明日{target_name}漲跌幅_pct"] <= result["預測Q90_pct"])
        )
        blocks.append(result)
        print(f"{target_name}價擴展式回測：{result['預測基準日'].iloc[0]} ~ {result['預測基準日'].iloc[-1]}")

    return pd.concat(blocks, ignore_index=True) if blocks else pd.DataFrame()


def pinball_loss(y_true, y_pred, alpha: float) -> float:
    error = np.asarray(y_true) - np.asarray(y_pred)
    return float(np.mean(np.maximum(alpha * error, (alpha - 1.0) * error)))


def metric_row(name, y_true, y_pred, alpha=None) -> dict:
    result = {
        "模型": name,
        "樣本數": len(y_true),
        "MAE_pct百分點": mean_absolute_error(y_true, y_pred),
        "RMSE_pct百分點": mean_squared_error(y_true, y_pred) ** 0.5,
        "平均誤差_預測減實際": float(np.mean(np.asarray(y_pred) - np.asarray(y_true))),
    }
    result["Pinball_loss"] = pinball_loss(y_true, y_pred, alpha) if alpha is not None else np.nan
    return result


def main() -> None:
    if _version_tuple(xgb.__version__) < (2, 0):
        raise SystemExit(
            f"目前 xgboost={xgb.__version__}，分位數回歸需要 >=2.0；"
            "請執行：pip install -U xgboost"
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    raw = load_data()
    data, feature_cols = add_features(raw)

    required_targets = [cfg["target_col"] for cfg in TARGETS.values()]
    required_prices = [cfg["actual_price_col"] for cfg in TARGETS.values()]
    labeled = data.dropna(subset=["日盤收", *required_prices, *required_targets]).copy()
    labeled = labeled.loc[labeled[feature_cols].notna().any(axis=1)].reset_index(drop=True)
    train, valid, test = time_split(labeled)
    tuning_end = max(1, int(len(valid) * TUNING_SHARE_OF_VALID))
    tuning = valid.iloc[:tuning_end].copy()
    calibration = valid.iloc[tuning_end:].copy()
    if len(tuning) < 30 or len(calibration) < 30:
        raise RuntimeError(
            f"驗證集只有 {len(valid)} 筆，切分後不足以調參與校準；請增加資料或調整比例"
        )

    X_train = train[feature_cols]
    X_tuning = tuning[feature_cols]
    X_cal = calibration[feature_cols]
    X_test = test[feature_cols]

    print(f"XGBoost 版本：{xgb.__version__}")
    print(f"資料日期：{labeled['日期'].min().date()} ~ {labeled['日期'].max().date()}")
    print(
        f"切分筆數：train={len(train)}, tuning={len(tuning)}, "
        f"calibration={len(calibration)}, test={len(test)}"
    )
    print(f"特徵數：{len(feature_cols)}")

    test_pred = pd.DataFrame(
        {
            "預測基準日": test["日期"].dt.strftime("%Y-%m-%d"),
            "實際明日日期": test["明日日期"].dt.strftime("%Y-%m-%d"),
            "今日收盤": test["日盤收"].values,
        }
    )
    summary = []
    calibration_rows = []
    target_state = {}
    for target_name, cfg in TARGETS.items():
        target_col = cfg["target_col"]
        tail_quantile = cfg["tail_quantile"]
        y_train, y_tuning = train[target_col], tuning[target_col]
        y_cal, y_test = calibration[target_col], test[target_col]
        test_pred[cfg["actual_output_col"]] = test[cfg["actual_price_col"]].values
        test_pred[f"實際明日{target_name}漲跌幅_pct"] = y_test.values
        models, best_rounds, offsets = {}, {}, {}

        for alpha in QUANTILES:
            label = f"Q{int(alpha * 100):02d}"
            print(f"訓練{target_name}價 {label} ...")
            model = make_model(alpha)
            model.fit(X_train, y_train, eval_set=[(X_tuning, y_tuning)], verbose=False)
            best_round = int(getattr(model, "best_iteration", model.n_estimators - 1)) + 1
            best_rounds[alpha] = best_round
            cal_raw = model.predict(X_cal)
            pred_raw = model.predict(X_test)
            fixed_offset = calibration_offset(y_cal, cal_raw, alpha)
            if alpha == 0.50:
                pred, offset, method = pred_raw + fixed_offset, fixed_offset, "獨立校準期固定校準"
            elif alpha == tail_quantile:
                dynamic = rolling_offsets(
                    y_cal.to_numpy() - cal_raw,
                    y_test.to_numpy() - pred_raw,
                    alpha,
                    ROLLING_CALIBRATION_WINDOW,
                )
                pred, offset = pred_raw + dynamic, float(dynamic[-1])
                method = f"最近{ROLLING_CALIBRATION_WINDOW}筆殘差滾動校準"
            else:
                pred, offset, method = pred_raw, 0.0, "不校準"
            models[alpha], offsets[alpha] = model, offset
            prefix = f"{target_name}_{label}"
            test_pred[f"預測{prefix}_未校準_pct"] = pred_raw
            test_pred[f"預測{prefix}_pct"] = pred
            summary.append({"預測目標": target_name, **metric_row(f"XGBoost_{label}", y_test, pred, alpha)})
            calibration_rows.append({
                "預測目標": target_name, "分位數": label, "alpha": alpha,
                "最佳訓練輪數": best_round, "校準方式": method,
                "固定校準候選值_pct百分點": fixed_offset,
                "正式預測使用校準加值_pct百分點": offset,
                "校準集_實際低於等於預測比例_校準前": float(np.mean(y_cal <= cal_raw)),
                "校準集_固定校準後命中比例": float(np.mean(y_cal <= cal_raw + fixed_offset)),
                "測試集_實際低於等於預測比例_校準後": float(np.mean(y_test <= pred)),
            })

        tail_label = f"Q{int(tail_quantile * 100):02d}"
        tail_cal_raw = models[tail_quantile].predict(X_cal)
        tail_test_raw = test_pred[f"預測{target_name}_{tail_label}_未校準_pct"].to_numpy()
        known = np.concatenate([y_cal.to_numpy() - tail_cal_raw, y_test.to_numpy() - tail_test_raw])
        offsets[tail_quantile] = calibration_offset(
            known[-ROLLING_CALIBRATION_WINDOW:],
            np.zeros(min(ROLLING_CALIBRATION_WINDOW, len(known))),
            tail_quantile,
        )
        for row in calibration_rows:
            if row["預測目標"] == target_name and row["alpha"] == tail_quantile:
                row["正式預測使用校準加值_pct百分點"] = offsets[tail_quantile]

        q_cols = [f"預測{target_name}_Q{int(q * 100):02d}_pct" for q in QUANTILES]
        raw_cols = [f"預測{target_name}_Q{int(q * 100):02d}_未校準_pct" for q in QUANTILES]
        crossing = (test_pred[q_cols].diff(axis=1).iloc[:, 1:] < 0).any(axis=1)
        test_pred[f"{target_name}價分位數原始交叉"] = crossing
        test_pred[q_cols] = np.sort(test_pred[q_cols].to_numpy(), axis=1)
        for alpha in QUANTILES:
            label = f"Q{int(alpha * 100):02d}"
            test_pred[f"預測{target_name}_{label}_{cfg['point_suffix']}"] = (
                test_pred["今日收盤"] * (1 + test_pred[f"預測{target_name}_{label}_pct"] / 100)
            )
        actual = test_pred[f"實際明日{target_name}漲跌幅_pct"]
        test_pred[f"{target_name}價Q10_Q90是否涵蓋實際"] = (
            (actual >= test_pred[q_cols[0]]) & (actual <= test_pred[q_cols[-1]])
        )
        test_pred[f"{target_name}價未校準Q10_Q90是否涵蓋實際"] = (
            (actual >= test_pred[raw_cols[0]]) & (actual <= test_pred[raw_cols[-1]])
        )
        summary.append({"預測目標": target_name, **metric_row(
            "基準_訓練集中位數", y_test, np.full(len(y_test), y_train.median()), 0.50
        )})
        target_state[target_name] = dict(
            cfg=cfg, models=models, best_rounds=best_rounds, offsets=offsets,
            y_train=y_train, y_test=y_test,
        )

    summary_df = pd.DataFrame(summary)
    summary_df["測試集Q10_Q90涵蓋率"] = np.nan
    summary_df["測試集未校準Q10_Q90涵蓋率"] = np.nan
    summary_df["分位數交叉率"] = np.nan
    for target_name in TARGETS:
        mask = summary_df["預測目標"].eq(target_name)
        first = summary_df.index[mask][0]
        summary_df.loc[first, "測試集Q10_Q90涵蓋率"] = test_pred[f"{target_name}價Q10_Q90是否涵蓋實際"].mean()
        summary_df.loc[first, "測試集未校準Q10_Q90涵蓋率"] = test_pred[f"{target_name}價未校準Q10_Q90是否涵蓋實際"].mean()
        summary_df.loc[first, "分位數交叉率"] = test_pred[f"{target_name}價分位數原始交叉"].mean()
    summary_df["測試起日"] = test["日期"].min().strftime("%Y-%m-%d")
    summary_df["測試迄日"] = test["日期"].max().strftime("%Y-%m-%d")

    # 最新有完整日盤收盤資料的列，即使尚無 T+1 真值也能預測。
    latest_candidates = data.dropna(subset=["日盤收"])
    latest_candidates = latest_candidates.loc[latest_candidates[feature_cols].notna().any(axis=1)]
    latest = latest_candidates.iloc[[-1]]
    latest_close = float(latest["日盤收"].iloc[0])
    latest_input_features = latest[["日期", "日盤收", *feature_cols]].copy()
    production_fingerprint_cols = ["日期", *feature_cols, *required_targets]
    production_data_fingerprint = dataframe_fingerprint(
        labeled[production_fingerprint_cols]
    )
    latest_feature_fingerprint = dataframe_fingerprint(latest_input_features)
    latest_row = {
        "預測基準日": latest["日期"].iloc[0].strftime("%Y-%m-%d"),
        "今日收盤": latest_close,
        "正式模型訓練資料迄日": labeled["日期"].max().strftime("%Y-%m-%d"),
    }
    importance_parts = []
    expanding_parts = []
    production_diagnostics = {}
    for target_name, state in target_state.items():
        cfg = state["cfg"]
        production_models = {}
        latest_values = []
        for alpha in QUANTILES:
            model = make_model(alpha, state["best_rounds"][alpha], use_early_stopping=False)
            model.fit(labeled[feature_cols], labeled[cfg["target_col"]], verbose=False)
            production_models[alpha] = model
            # 同一個已訓練模型重複預測必須完全一致；否則立即停止，避免輸出
            # 無法重現的正式點位。
            raw_latest_1 = float(model.predict(latest[feature_cols])[0])
            raw_latest_2 = float(model.predict(latest[feature_cols])[0])
            if not np.isclose(raw_latest_1, raw_latest_2, rtol=0.0, atol=1e-12):
                raise RuntimeError(
                    f"{target_name}價 Q{int(alpha * 100):02d} 正式模型重複預測不一致："
                    f"{raw_latest_1} != {raw_latest_2}"
                )
            latest_values.append(raw_latest_1 + state["offsets"][alpha])
            production_diagnostics.setdefault(target_name, {})[
                f"Q{int(alpha * 100):02d}"
            ] = {
                "training_rows": int(len(labeled)),
                "non_null_target_rows": int(labeled[cfg["target_col"]].notna().sum()),
                "best_rounds": int(state["best_rounds"][alpha]),
                "raw_latest_prediction_pct": raw_latest_1,
                "calibration_offset_pct_points": float(state["offsets"][alpha]),
            }
        latest_values = np.sort(np.asarray(latest_values, dtype=float))
        for index, alpha in enumerate(QUANTILES):
            label = f"Q{int(alpha * 100):02d}"
            latest_row[f"預測{target_name}_{label}_pct"] = latest_values[index]
            latest_row[f"預測{target_name}_{label}_{cfg['point_suffix']}"] = (
                latest_close * (1 + latest_values[index] / 100)
            )
        importance_parts.append(pd.DataFrame({
            "預測目標": target_name,
            "分位數": "Q50",
            "特徵": feature_cols,
            "重要性": production_models[0.50].feature_importances_,
        }))
        if RUN_EXPANDING_BACKTEST:
            exp = expanding_backtest(
                labeled, feature_cols, len(train) + len(valid),
                state["best_rounds"], state["offsets"][0.50],
                target_name, cfg["target_col"], cfg["tail_quantile"],
            )
            if not exp.empty:
                expanding_parts.append(exp)
                summary_df = pd.concat([summary_df, pd.DataFrame([{
                    "預測目標": target_name,
                    **metric_row(
                        "擴展式重訓_Q50",
                        exp[f"實際明日{target_name}漲跌幅_pct"],
                        exp["預測Q50_pct"], 0.50,
                    ),
                    "測試集Q10_Q90涵蓋率": exp["Q10_Q90是否涵蓋實際"].mean(),
                    "測試集未校準Q10_Q90涵蓋率": np.nan,
                    "分位數交叉率": np.nan,
                    "測試起日": exp["預測基準日"].min(),
                    "測試迄日": exp["預測基準日"].max(),
                }])], ignore_index=True)

    latest_prediction = pd.DataFrame([latest_row])
    importance = pd.concat(importance_parts, ignore_index=True).sort_values(
        ["預測目標", "重要性"], ascending=[True, False]
    )
    calibration_df = pd.DataFrame(calibration_rows)
    expanding = pd.concat(expanding_parts, ignore_index=True) if expanding_parts else pd.DataFrame()

    test_pred.to_csv(OUTPUT_DIR / "test_predictions.csv", index=False, encoding="utf-8-sig")
    summary_df.to_csv(OUTPUT_DIR / "model_summary.csv", index=False, encoding="utf-8-sig")
    importance.to_csv(OUTPUT_DIR / "feature_importance.csv", index=False, encoding="utf-8-sig")
    calibration_df.to_csv(OUTPUT_DIR / "quantile_calibration.csv", index=False, encoding="utf-8-sig")
    latest_prediction.to_csv(OUTPUT_DIR / "latest_prediction.csv", index=False, encoding="utf-8-sig")
    latest_input_features.to_csv(
        OUTPUT_DIR / "latest_input_features.csv", index=False, encoding="utf-8-sig"
    )
    if RUN_EXPANDING_BACKTEST:
        expanding.to_csv(OUTPUT_DIR / "expanding_backtest.csv", index=False, encoding="utf-8-sig")

    run_info = {
        "targets": {
            "最高": "(明日日盤高 / 今日日盤收 - 1) * 100",
            "最低": "(明日日盤低 / 今日日盤收 - 1) * 100",
        },
        "prediction_time": "T日夜盤結束後、T+1日盤開盤前",
        "night_data_alignment": "直接使用T列夜盤（T日晚間至T+1日清晨），不shift",
        "xgboost_version": xgb.__version__,
        "features": feature_cols,
        "train_dates": [str(train["日期"].min().date()), str(train["日期"].max().date())],
        "valid_dates": [str(valid["日期"].min().date()), str(valid["日期"].max().date())],
        "test_dates": [str(test["日期"].min().date()), str(test["日期"].max().date())],
        "tuning_dates": [str(tuning["日期"].min().date()), str(tuning["日期"].max().date())],
        "calibration_dates": [str(calibration["日期"].min().date()), str(calibration["日期"].max().date())],
        "production_train_dates": [
            str(labeled["日期"].min().date()), str(labeled["日期"].max().date())
        ],
        "production_training_rows": int(len(labeled)),
        "production_data_fingerprint_sha256": production_data_fingerprint,
        "latest_feature_fingerprint_sha256": latest_feature_fingerprint,
        "latest_feature_missing_count": int(latest[feature_cols].isna().sum(axis=1).iloc[0]),
        "production_best_rounds": {
            name: {f"Q{int(q * 100):02d}": state["best_rounds"][q] for q in QUANTILES}
            for name, state in target_state.items()
        },
        "calibration_offsets_pct_points": {
            name: {f"Q{int(q * 100):02d}": state["offsets"][q] for q in QUANTILES}
            for name, state in target_state.items()
        },
        "calibration_rules": {
            "最高": {"Q10": "不校準", "Q50": "固定校準", "Q90": "近期滾動校準"},
            "最低": {"Q10": "近期滾動校準", "Q50": "固定校準", "Q90": "不校準"},
        },
        "expanding_refit_every_trading_days": EXPANDING_REFIT_EVERY,
        "run_expanding_backtest": RUN_EXPANDING_BACKTEST,
        "deterministic_training": {
            "random_state": RANDOM_STATE,
            "n_jobs": 1,
            "production_diagnostics": production_diagnostics,
        },
    }
    (OUTPUT_DIR / "run_info.json").write_text(
        json.dumps(run_info, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print("\n===== 樣本外評估 =====")
    print(summary_df.to_string(index=False))
    print("\n===== 最新預測 =====")
    print(latest_prediction.to_string(index=False))
    print(f"\n報表已輸出至：{OUTPUT_DIR}")
    if not RUN_EXPANDING_BACKTEST:
        print("擴展式回測已關閉；既有 expanding_backtest.csv 未覆寫。")
    print("請優先貼回 model_summary.csv、quantile_calibration.csv、latest_prediction.csv，")
    print("並附上 feature_importance.csv 前 20 列及執行畫面。")


# python -m machine_learning.futures_predict
if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\n執行失敗：{type(exc).__name__}: {exc}", file=sys.stderr)
        raise
