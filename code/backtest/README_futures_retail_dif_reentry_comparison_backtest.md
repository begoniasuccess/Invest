# 散戶極空5/12R：破失效價後DIF止跌重進對照

固定訊號：核心多、散戶歷史分位Q20以下、排除MA5正乖離Q80以上、夜盤收向下5/12個近30日平均振幅為原定進場價、失效價再向下1/4R、每筆成本3點。

三組對照：

- `CANCEL_DAY`：原規則。前30分鐘跌破失效價，當日取消。
- `DIRECT_ENTRY`：08:45起觸及5/12R直接進場，不做失效價過濾。
- `DIF_REVERSAL_ENTRY`：前30分鐘跌破失效價後先觀望；等完整5分K的DIF連續上升兩次，確認收盤後保留完整1分鐘，以確認時刻加1分鐘後第一筆成交價模擬入場。未跌破失效價的交易日完全沿用原規則。

三組進場後皆不設停損停利，持有至日盤收盤，以便只比較進場規則。

```powershell
python -m backtest.futures_retail_dif_reentry_comparison_backtest --end 2026-11-02
```

主要輸出：

- `retail_dif_reentry_summary_*.csv`：三組總績效。
- `retail_dif_reentry_yearly_*.csv`：分年績效。
- `retail_dif_reentry_rescued_detail_*.csv`：原本會取消、改由DIF止跌嘗試救回的交易。
- `retail_dif_reentry_detail_*.csv`：完整逐筆明細。
