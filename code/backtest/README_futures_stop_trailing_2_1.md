# 2-1 固定停損／移動停利回測

## 規則

- 訊號使用前一交易日的 `策略_核心_五日價量`（設定檔可改成 `策略_核心_昨價量`）。
- 跳空基準是前一交易日 `全日收`。
- 多訊號向上跳空「超過」門檻不進場；空訊號向下跳空「超過」門檻不進場。
- 其餘於日盤 08:45 後第一筆 Tick 進場。
- 虧損觸及停損門檻即出場。
- 獲利觸及啟動門檻後，從持倉最佳價格回檔指定門檻即出場。
- 都未觸發時，以 13:45 以前最後一筆 Tick 出場，不留倉。
- 以開盤當下有成交的最近月份 `YYYYMM` 契約交易；不同月份不混用。不可用當日
  整天成交量選契約，因為那會在 08:45 的決策中偷看收盤後才完整的資料。

門檻模式：

- `POINTS`：固定點數。
- `PRICE_PERCENT`：價格百分比，設定值 `0.3` 代表 0.3%。
- `PROFIT_GIVEBACK_PERCENT`：最大浮盈回吐比例，只能用於 `trailing_drawdown`。

## 執行

在專案 `code` 目錄：

```powershell
python -m backtest.futures_stop_trailing --config backtest/configs/futures_stop_trailing_2_1.json
```

`parameter_grid.long` 與 `parameter_grid.short` 分別設定多單及空單參數。每個陣列
可以放入多個數值，程式會在方向內產生排列組合。同一交易日的 Tick 只讀取一次，
再套用當日該方向的所有參數，避免每組參數重新查詢資料庫。

預設粗網格為多單 162 組、空單 162 組，合計 324 組。多空分開排名，不將兩邊
做笛卡兒積；兩邊交易日互斥，分開完成後即可任意合併，避免產生沒有必要的
26,244 組重複運算。

輸出：

- `../output/backtest/futures_stop_trailing_2_1_directional_detail_*.csv`
- `../output/backtest/futures_stop_trailing_2_1_directional_summary_*.csv`

`parameter_id` 會帶 `long_` 或 `short_` 前綴，是方向與參數內容的穩定識別碼，
可用來連結摘要和明細。
