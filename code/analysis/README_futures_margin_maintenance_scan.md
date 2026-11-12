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
