# 2-1 固定停損與移動保利

## 研究目的與實作

使用前一交易日核心多空；先篩選沿訊號方向的開盤跳空，符合者於日盤第一筆 Tick 進場，固定停損或獲利啟動後從最佳價回吐出場，未觸發尾盤平倉。支援多空獨立門檻網格。

## 結果判讀與限制

檢查多空各組成本、進場率、PF、最大回撤及門檻單位；不能用現行設定推定歷史執行參數。

## 程式來源

- 實作：`code/backtest/futures_stop_trailing.py`
- 原有 README：`code/backtest/README_futures_stop_trailing_2_1.md`
- 現有設定：`code/backtest/configs/futures_stop_trailing_2_1.json`

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_stop_trailing --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--config', type=Path, default=Path('backtest/configs/futures_stop_trailing_2_1.json'))`

### 輸出檔名線索

- `futures_stop_trailing_2_1_directional_detail_{suffix}.csv`
- `futures_stop_trailing_2_1_directional_summary_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

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

## 執行效能

- 程式會先套用開盤濾網，只有需要交易的日期才讀取 Tick。
- Tick 改為每批 20 個交易日一次查詢，不再逐日往返資料庫；避免一次載入多年
  Tick 耗盡記憶體。
- Terminal 會在每批完成後顯示已處理交易日數，長期間回測時可確認進度。

