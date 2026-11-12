# 隔日日盤波動初版模型組合掃描

此分析以訊號日收盤後已知資料預測下一個有效台指期日盤，只研究振幅大小，不預測多空方向。

## 執行方式

將 ZIP 解壓縮到專案的 `code` 資料夾，確認檔案位於 `code/analysis/`。從 `code` 資料夾執行：

```powershell
python -m analysis.futures_volatility_combo_model `
  --start 2018-01-01 `
  --first-test-year 2023
```

若有 scikit-learn，無需另外安裝套件。省略 `--end` 時，使用今天作為結果資料截止日。預設輸出在 `../output/analysis/futures_volatility_combo_model/`。

## 比較設計

基準模型先使用當日已實現振幅倍數、近5日平均振幅倍數、近20日平均振幅倍數。然後比較這些基準特徵和七個候選特徵的所有子集合，共 128 種模型（包含基準模型）。

七項候選：

- 布林通道位置偏低
- 散戶多空比偏高
- MA10 斜率偏低
- 主力淨多空／OI 比偏低
- MA5 乖離率偏低
- 外資三日累計賣超（以三日買賣超／成交金額比衡量）
- 融資維持率三日淨下降幅度

每個特徵轉成歷史百分位分數，僅與此前最多 252 個有效觀測比較，分數越高代表該候選條件越強。三日維持率特徵是「三個交易日前維持率減當日維持率」，不要求連續三天下降。

採年度 expanding walk-forward：測試某一年時，模型只使用該年以前的樣本訓練。振幅模型用 StandardScaler + Ridge 預測 log(range_ratio)，評估 MAE、RMSE、Spearman；劇烈以上分類模型用 StandardScaler + L2 Logistic Regression，評估 Brier、LogLoss、ROC AUC、PR AUC 及預測風險最高20%之事件率 Lift。正規化參數固定，不在測試年調參。

## 如何讀輸出

- `volatility_model_combo_summary_*.csv`：所有組合，振幅排名按跨測試年度平均 MAE 由低至高；分類可按年平均 Brier 由低至高。
- `volatility_model_walk_forward_yearly_*.csv`：各模型各測試年度的表現，以及相對近期振幅基準的改善。
- `volatility_model_feature_ablation_*.csv`：從全特徵模型移除單一特徵後，振幅誤差及事件分類表現如何變化。移除後平均指標變好，代表該特徵在完整模型中可能增加雜訊或與其他特徵互相干擾。
- `volatility_model_feature_correlation_*.csv`：七個候選特徵間的 Spearman 相關。高相關表示資訊重疊，不單獨等同衝突。
- `volatility_model_full_coefficients_*.csv`：全特徵模型各年度係數方向，需與消融結果並讀。
- `volatility_model_detail_*.csv`：共同完整樣本的特徵、目標和結果日期。
- `volatility_model_manifest_*.json`：參數和方法記錄。

所有組合使用相同的完整樣本，避免某模型因缺值較少而占優。請先看跨年平均表現和每年勝過基準的次數，不要只看一個最佳年度或排名第一。

此掃描會比較 127 種候選組合，排名第一可能是多重比較造成的偶然結果。過去已看過這些資料特徵的關聯，因此結果屬初步模型篩選；選出少數組合後，仍應固定設定並用未來新增資料作前瞻檢驗。
