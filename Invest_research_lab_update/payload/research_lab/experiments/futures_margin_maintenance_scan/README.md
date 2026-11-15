# 大盤融資維持率與隔日日盤振幅

## 研究目的與實作

分析融資維持率相對此前有效觀測的水準、單日下降、三日累計下降與低位×急降組合，對齊下一有效日盤觀察振幅、劇烈以上及其他方向路徑指標。

## 結果判讀與限制

維持率資料可能缺漏；事件有效樣本分母與分組總筆數不同，檢查低位急降組樣本及跨期驗證。

## 程式來源

- 實作：`code/analysis/futures_margin_maintenance_scan.py`
- 原有 README：`code/analysis/README_futures_margin_maintenance_scan.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m analysis.futures_margin_maintenance_scan --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--split-date', type=date.fromisoformat, default=date(2023, 1, 1))`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--history-window', type=int, default=100)`
- `parser.add_argument('--minimum-history', type=int, default=80)`
- `parser.add_argument('--low-percentile', type=float, default=20.0)`
- `parser.add_argument('--steep-drop-percentile', type=float, default=90.0)`
- `parser.add_argument('--minimum-group-size', type=int, default=20)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/analysis/futures_margin_maintenance_scan'))`

### 輸出檔名線索

- `margin_maintenance_buckets_{suffix}.csv`
- `margin_maintenance_detail_{suffix}.csv`
- `margin_maintenance_low_drop_combo_{suffix}.csv`
- `margin_maintenance_manifest_{suffix}.json`
- `margin_maintenance_tests_{suffix}.csv`
- `margin_maintenance_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 大盤融資維持率與次日日盤振幅分析

這是第一階段探索分析，資料來源是專案中已新增的 FinMind 大盤融資維持率原始表；不需要修改資料庫 schema。

## 執行

將 ZIP 解壓縮到專案的 `code` 資料夾，確認檔案放在 `code/analysis/`。在 `code` 資料夾執行：

```powershell
python -m analysis.futures_margin_maintenance_scan `
  --start 2018-01-01 `
  --end 2026-11-09 `
  --split-date 2023-01-01
```

省略 `--end` 時，會使用今天為結果資料截止日。預設輸出至 `../output/analysis/futures_margin_maintenance_scan/`。

## 分析設計

- 特徵分開檢驗：維持率相對此前 100 個有效觀測的水準、單日下降百分點、三日累計下降百分點。
- 每個特徵均與此前最多 100 個有效觀測比較，再切為五分位；分位數只使用訊號日前資料。
- 額外比較「低位 × 急降」四組。預設低位為維持率歷史分位不高於 20%，急降為單日下降幅度歷史分位不低於 90%。可用 `--low-percentile` 和 `--steep-drop-percentile` 調整。
- 訊號日融資維持率只配對下一個有效台指期日盤，結果包括振幅百分比、既有振幅倍數、劇烈以上發生率，以及另行報告的方向與低點收盤反彈點數。
- 摘要中的「樣本數」是分組總筆數；「劇烈以上有效樣本數」才是計算劇烈振幅發生率的分母，因為部分日期可能沒有既有振幅標籤。
- 輸出全期、探索期、驗證期及逐年摘要。檢定只作探索用途，不代表因果關係；本版尚未控制訊號日前的大盤報酬與既有波動狀態。
- 不對缺漏維持率日期前向填補。維持率資料或目標結果缺漏的觀測不納入分析。

## 輸出檔案

- `margin_maintenance_detail_*.csv`：訊號日、結果日、原始特徵、歷史分位及次日結果。
- `margin_maintenance_buckets_*.csv`：三個特徵各自五分位的振幅與方向統計，分全期／探索期／驗證期。
- `margin_maintenance_low_drop_combo_*.csv`：低位與單日急降的四組交叉比較。
- `margin_maintenance_yearly_*.csv`：各特徵、五分位逐年摘要。
- `margin_maintenance_tests_*.csv`：連續歷史分位與次日振幅/劇烈以上的趨勢檢定、Q1/Q5比較及 BH-FDR 校正值。
- `margin_maintenance_manifest_*.json`：參數、資料來源與分析限制。

CSV 使用 UTF-8 BOM，Excel 可直接開啟。

