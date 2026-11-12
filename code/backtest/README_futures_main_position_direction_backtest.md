# 期貨主力部位欄位與隔日漲跌方向

此程式直接讀取 `report.v_futures_report_daily_corrected_turning`，檢查以下欄位和下一個實際交易日台指期日盤開收方向的關係：

- `主力新建多單`
- `主力新建空單`
- `主力淨多空和_5日`
- `主力淨多空和_10日`
- `主力淨多空和_20日`
- `主力淨多空和_60日`

隔日目標是 `(隔日日盤收－隔日日盤開)/隔日日盤開`。正報酬定義為上漲，負報酬定義為下跌，平盤樣本不納入二元方向模型。

## 測試層次

1. 對每個欄位分別測原始值、絕對值、正值部分、負值幅度、正負符號和隔日報酬的 Spearman 關係。
2. 測「欄位為正」對「欄位非正」，以及「欄位為負」對「欄位非負」的隔日上漲率差，觀察單邊訊號。
3. 依欄位原始值和絕對值分成五分位，檢查 Q1 到 Q5 的隔日報酬和上漲率，並檢定 Q5 對 Q1。
4. 用 2018 年至切分日前資料訓練 Logistic Regression，從切分日起做時間外驗證。比較單欄原始值、單欄絕對值、單欄正負拆分，以及六欄原始值、絕對值、正負拆分和兩者合併的模型。
5. 單因子各項 p 值依期間做 Benjamini-Hochberg FDR 修正。

## 執行方式

在 `code` 專案根目錄執行：

```powershell
python -m backtest.futures_main_position_direction_backtest `
  --start 2018-01-01 `
  --split-date 2023-01-01
```

想指定資料截止日：

```powershell
python -m backtest.futures_main_position_direction_backtest `
  --start 2018-01-01 `
  --end 2026-11-10 `
  --split-date 2023-01-01
```

需要專案原有的 PostgreSQL `POSTGRES_DSN` 設定，以及 `pandas`、`numpy`、`scipy`、`scikit-learn`。預設輸出至 `../output/backtest/futures_main_position_direction_backtest/`。

## 輸出檔

- `..._univariate_tests_...csv`：單因子效果與顯著性，先看驗證期和 FDR q 值。
- `..._groups_...csv`：正負號、原始值五分位、絕對值五分位的隔日上漲率與報酬。
- `..._models_...csv`：時間外模型的準確率、平衡準確率、AUC、Brier、LogLoss，並和訓練期多數方向基準比較。
- `..._predictions_...csv`：各模型逐日預測上漲機率和方向。
- `..._detail_...csv`：訊號日欄位與下一交易日結果對照。
- `..._manifest_...json`：期間、資料來源、目標定義和限制。

方向分類模型是預測能力比較，不等於交易策略績效；此版沒有設定進場、出場、成本或滑價。
