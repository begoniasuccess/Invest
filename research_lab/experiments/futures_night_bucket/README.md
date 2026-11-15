# 台指期夜盤漲跌十組分層

## 研究目的與實作

依夜盤相對日盤收盤漲跌幅分十組，統計下一實際交易日日盤開收、振幅及路徑指標；支援研究資料庫及CSV。

## 結果判讀與限制

這支有既有Dashboard夜盤分層頁；新實驗頁只管理研究說明和批次，不能取代正式頁面或隨意更新其最新批次。

## 程式來源

- 實作：`code/analysis/futures_night_bucket.py`
- 原有 README：`code/analysis/README_futures_night_bucket.md`
- 現有設定：`code/analysis/configs/futures_night_bucket_v1.json`

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m analysis.futures_night_bucket --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--config', default=str(DEFAULT_CONFIG_PATH), help='JSON 設定檔')`
- `parser.add_argument('--start-date', help='覆寫開始日期 YYYY-MM-DD')`
- `parser.add_argument('--end-date', help='覆寫結束日期 YYYY-MM-DD')`
- `parser.add_argument('--no-db', action='store_true', help='不寫入 research Schema')`
- `parser.add_argument('--no-csv', action='store_true', help='不輸出 CSV')`

### 輸出檔名線索

- `futures_night_bucket_daily_{suffix}.csv`
- `futures_night_bucket_manifest_{suffix}.json`
- `futures_night_bucket_summary_{suffix}.csv`
- `futures_night_bucket_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

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


