# 台指期日盤漲跌反轉事件統計 V1.0

## 安裝及執行

將本增量包 `code/` 內三個檔案，按原路徑放入你現有的 `code/` 專案。使用既有 `.env` 中的 `POSTGRES_DSN`，不修改資料庫。

在 `code` 目錄執行：

```powershell
python -m backtest.futures_intraday_reversal --config backtest/configs/futures_intraday_reversal_v1.json
```

使用 `common.db.sql_to_df` 連接 PostgreSQL。預設 2018-01-01 至 2026-11-06，可自行修改 JSON。輸出在 `code` 同層的 `output/backtest/intraday_reversal/`。

## 資料來源及口徑

- 主資料：`raw_finmind.fm_taiwan_futures_tick`；實際契約由 `raw_finmind.fm_taiwan_futures_kbar_1m` 日盤08:45第一根 K，挑有效6位契約月份最近月。避免把不同月份的價格串接。
- 日盤08:45–13:45；開盤價由首筆完整日盤tick判定。`source_date=交易日`，不含夜盤。
- 前20個已完成交易日的振幅使用 `core.futures_report_daily_raw` 的 `日盤高－日盤低`，不足20日不生成VOL倍數事件。
- 1天同方向同門檻只取首次觸發。觸發後的高低/MFE/MAE只看之後的tick；門檻價為理論參考價，不是模擬成交價。
- 反轉先達標、延續先達標依tick排序；同tick達標標記 `SAME_TICK`，不猜先後。
- 只納入足夠完整的tick日：第一筆08:46以前，最後一筆13:44以後，覆蓋至少295分鐘、最大相鄰tick間隔15分鐘、source_row_no沒有重複。被排除日詳見資料品質報表。
- 沒有達到門檻的日子不屬於反轉率分母，但有計入觸發頻率分母。

## 輸出

- `event_detail.csv`：所有事件的觸發價、時間、MFE、MAE、收盤反轉及20日前振幅。
- `reversal_probability.csv`：上漲回跌／下跌反彈 × 門檻 × 反轉目標；含樣本數及Wilson 95%區間。
- `first_hit_probability.csv`：反轉／延續先達標；含NEITHER、SAME_TICK。
- `first_hit_detail.csv`：每筆反轉/延續門檻組合的事件結果（檔案可能較大）。
- `mfe_mae_summary.csv`：MFE/MAE及收盤結果分布。
- `time_analysis.csv`、`yearly_analysis.csv`、`validation_summary.csv`：時段、年度、探索與驗證分析。
- `yearly_mfe_mae.csv`、`trigger_frequency.csv`、`data_quality_detail.csv`、`data_quality_summary.csv`、`manifest.json`。

## 重要限制

1. **非實際交易績效**：門檻價不是可成交價，未計入滑價、稅費及手續費。
2. **契約口徑**：按現有專案近月契約選取規則，不能視為連續調整後指數。結算日及換月口徑需看獨立契約結果。
3. **資料缺漏**：因交易量低而tick較疏的日子會被排除；觀察 `data_quality_summary` 是否存在系統性偏誤。
4. **樣本相依**：同日不同門檻共用行情，不可把它們視為互不相關的獨立實驗。
5. **末日資料**：只測設定結束日期之前的資料，且只計入符合完整性檢查的日子。
6. **價格越級**：如果tick直接跨越門檻，`first_cross_price` 和 `trigger_price` 不同。理論反轉/續漲點數是相對門檻價，並非真實交易報酬。

## 本地單元測試

```powershell
python -m pytest -q backtest/tests/test_futures_intraday_reversal.py
```
