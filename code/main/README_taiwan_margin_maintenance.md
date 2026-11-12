# 台灣大盤融資維持率資料流

- FinMind dataset：`TaiwanTotalExchangeMarginMaintenance`
- 原始表：`raw_finmind.fm_taiwan_total_exchange_margin_maintenance`
- 下載區間：`core.date_span`，key 為
  `fm_taiwan_total_exchange_margin_maintenance / all`
- 更新方式：依快取尾端增量下載，並回看最近 7 天承接晚間發布或修訂資料
- Dashboard：資料更新中心 → 其他資料 → 大盤融資維持率
- 本資料暫不併入期貨報表 view

首次使用前先執行：

```sql
\i db_migrate/20261109_add_taiwan_total_margin_maintenance.sql
```

也可手動更新：

```bash
python -m main.download_taiwan_margin_maintenance
```

FinMind 官方目前將此 dataset 列為 Backer／Sponsor 會員資料；若 token
權限不足，Dashboard 會顯示 FinMind API 回傳的權限錯誤。
