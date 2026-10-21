# 波動強度：簡單分數與正則化模型比較

這一步不直接上 XGBoost，而是先確認四個原始訊號的「連續強度」是否比 0～4 分的切點計分更有資訊。

## 比較內容

- 輸入特徵：布林低位、散戶比偏高、MA10 斜率偏低、主力淨多空 OI 偏低，全部使用當時可得的歷史分位數。
- 連續目標：隔日日盤 `(最高－最低) / 開盤`，比較簡單分數歷史均值與 Ridge。
- 事件目標：隔日振幅是否至少 1.0%／1.5%，比較簡單分數歷史命中率與 Logistic。
- 驗證方式：年度 expanding walk-forward。例如預測 2024 年時，只使用 2023 年以前資料訓練。
- 評估：MAE、RMSE、Spearman、Brier、ROC AUC、LogLoss，以及預測最高 20% 樣本的實際波動與提升倍數。

這能避免模型直接看見待測年度，但四個特徵是先前全期探索後選出的，因此整體研究仍不能宣稱是完全未觀察樣本外。

## 執行

這支程式需要 `scikit-learn`；若環境尚未安裝，可先執行 `python -m pip install scikit-learn`。

先在 PostgreSQL 執行：

```text
db_migrate/20261020_add_futures_volatility_model.sql
```

PowerShell 請使用反引號換行：

```powershell
python -m backtest.futures_volatility_model_compare `
  --start 2018-01-01 `
  --end 2026-10-16 `
  --first-test-year 2021 `
  --save-db
```

或直接寫成一行：

```powershell
python -m backtest.futures_volatility_model_compare --start 2018-01-01 --end 2026-10-16 --first-test-year 2021 --save-db
```

若只想先輸出 CSV，不入庫，移除 `--save-db`。

## 輸出

- `volatility_model_predictions_*.csv`：每日樣本外預測與實際值。
- `volatility_model_metrics_*.csv`：全期及逐年模型比較。
- `volatility_model_coefficients_*.csv`：各年度標準化係數，檢查訊號方向是否穩定。
- `volatility_model_manifest_*.json`：本次設定、版本與研究限制。

## 判讀原則

模型只有在多數指標上持續勝過簡單分數才值得保留：Ridge 的 MAE/RMSE 越低越好；Logistic 的 Brier/LogLoss 越低越好、ROC AUC 越高越好。最高 20% 提升倍數則比較接近實際用途：若穩定大於 1，代表模型有能力把高波動日排到前面。

如果複雜模型只改善全期數字、逐年忽好忽壞，就保留簡單分數，不增加策略複雜度。
