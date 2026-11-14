# 加權／期貨報表欄位更新

## 套用順序

1. 先備份資料庫。
2. 執行 `db_migrate/fix_futures_report_requirements_20260817.sql`。
3. 執行 `db_migrate/fix_taiex_report_requirements_20260817.sql`。
4. 以本壓縮檔內程式覆蓋原本的 `code`。
5. 正常執行一次 `python -m main.futures`，確認最新一日及 `日240MA`。
6. 如需重新輸出加權 CSV，再執行 `python -m main.stock_report_taiex`。

兩份 SQL 都只修改本次指定的資料表／View，並且可以重複執行；不要重跑整份舊版 `postgres_views.sql`。
SQL 內含強制驗收：欄位未刪乾淨或位置不正確時會直接報錯並回滾。

## 實體資料表

- `stock_report_daily`：新增 `120MA`、`240MA`，SQL 回填歷史，Python 每次以完整 DB 歷史重算。
- `futures_report_daily_raw`：新增 `K線型態`、`日240MA`，SQL 回填歷史，Python 後續持續更新。

## 內部 View

- `v_futures_report_daily_model`：保留完整欄位給 `futures_predict.py` 使用。
- `v_futures_report_daily`：對外報表，移除指定欄位並新增 `K線型態`、`日240MA`。
- `v_taiex_report_column_source`：保留調整前的加權完整欄位。
- `v_taiex_report`：對外報表，新增 120MA／240MA 並完成欄位移動。

## 天別規則

- 星期一：頭
- 星期二～星期四：週間
- 星期五：尾
- 週末：週間（與加權指數既有邏輯一致）
