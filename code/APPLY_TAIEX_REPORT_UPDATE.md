# 加權指數報表欄位更新

## 已修改

- `db_migrate/postgres_views.sql`
  - `散戶多空比_微台`：依日期取自 `v_futures_report_daily`，並除以 100。
  - `布林標準差`：20 日收盤價的樣本標準差。
  - `布林上軌`：20MA + 2 × 布林標準差。
  - `布林下軌`：20MA - 2 × 布林標準差。

## 套用方式

請在 PostgreSQL 執行更新後的 `db_migrate/postgres_views.sql`，重建 views。
完成後照常執行：

```powershell
python -m main.stock_report_taiex
```

`main.stock_report_taiex` 本來就是 `SELECT * FROM v_taiex_report` 輸出 CSV，
因此不需再修改 Python 匯出欄位清單。

## 驗證查詢

```sql
SELECT
    日期,
    "60日乖離",
    布林標準差,
    布林上軌,
    布林下軌,
    法人買賣超_總成交_pct,
    散戶多空比_微台
FROM v_taiex_report
ORDER BY 日期 DESC
LIMIT 10;
```

前 19 筆資料因不足 20 個交易日，三個布林欄位為 `NULL`，屬正常結果。
