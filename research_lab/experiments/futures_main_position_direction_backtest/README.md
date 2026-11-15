# 期貨主力部位與隔日方向

## 研究目的與實作

檢查主力新建多空與多個期間淨多空和的原值、絕對值、正負拆分及分位數，對隔日日盤開收方向做單因子檢定及時間切分Logistic模型比較。

## 結果判讀與限制

區分探索與時間外測試、方向正確率與基準、FDR校正；非逐筆交易損益。

## 程式來源

- 實作：`code/backtest/futures_main_position_direction_backtest.py`
- 原有 README：`code/backtest/README_futures_main_position_direction_backtest.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_main_position_direction_backtest --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--split-date', type=date.fromisoformat, default=date(2023, 1, 1))`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest/futures_main_position_direction_backtest'))`

### 輸出檔名線索

- `main_position_direction_detail_{suffix}.csv`
- `main_position_direction_groups_{suffix}.csv`
- `main_position_direction_manifest_{suffix}.json`
- `main_position_direction_models_{suffix}.csv`
- `main_position_direction_predictions_{suffix}.csv`
- `main_position_direction_univariate_tests_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

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

