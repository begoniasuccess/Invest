# 散戶極空反彈重進五分K圖庫

## 研究目的與實作

重新執行反彈重進策略，載入交易日Tick聚合五分K，輸出包含三方案進出場線、損益、年度及失效／救援篩選的HTML圖庫。

## 結果判讀與限制

屬於案例檢視工具，不是新增策略；圖庫生成時間與原始CSV批次未必相同。

## 程式來源

- 實作：`code/backtest/futures_retail_bounce_reentry_chart_gallery.py`
- 原有 README：未找到對應 README；本頁依實作建立。
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_retail_bounce_reentry_chart_gallery --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--cost-points', type=float, default=3.0)`
- `parser.add_argument('--output', type=Path, default=Path('../output/backtest/retail_bounce_reentry/retail_bounce_reentry_5m_gallery.html'))`

### 輸出檔名線索

- `../output/backtest/retail_bounce_reentry/retail_bounce_reentry_5m_gallery.html`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。
