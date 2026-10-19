# 台指期夜盤十組分層分析 v1

## 第一次使用

先在 DBeaver 執行：

`db_migrate/20260913_create_research_night_bucket_schema.sql`

再於專案 `code` 目錄執行：

```powershell
python -m analysis.futures_night_bucket
```

預設分析期間為 `2018-01-01` 至 `2026-09-10`。需要臨時更換期間時：

```powershell
python -m analysis.futures_night_bucket --start-date 2020-01-01 --end-date 2026-09-10
```

永久修改預設值則編輯：

`analysis/configs/futures_night_bucket_v1.json`

程式會：

1. 從 `report.v_futures_report_daily_corrected` 讀取日盤與夜盤資料。
2. 依夜盤相對當日日盤收盤的漲跌幅分成十組。
3. 計算下一交易日日盤的開盤、收盤、日內、振幅及路徑指標。
4. 寫入 `research` Schema 的明細、全期間摘要與年度摘要。
5. 輸出 UTF-8 BOM CSV，方便 Excel 開啟及傳回分析。

Dashboard 啟動後開啟 `/night-bucket`。頁面會讀取最近一次成功完成的分析批次。

