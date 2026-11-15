# 散戶極空策略：跌破失效價後反彈重進

## 研究目的與實作

共用散戶極空5/12R條件，對照取消當日、直接進場、破失效價後從動態低點反彈1/6R再等完整1分鐘進場三組；進場後均持有至收盤。

## 結果判讀與限制

重點看救回交易、延後進場代價、與基準比較及年度一致性；注意觸發後等待的實際時間。

## 程式來源

- 實作：`code/backtest/futures_retail_bounce_reentry_comparison_backtest.py`
- 原有 README：`code/backtest/README_futures_retail_bounce_reentry_comparison_backtest.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_retail_bounce_reentry_comparison_backtest --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--cost-points', type=float, default=3.0)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest/retail_bounce_reentry'))`

### 輸出檔名線索

- `retail_bounce_reentry_detail_{suffix}.csv`
- `retail_bounce_reentry_entry_status_{suffix}.csv`
- `retail_bounce_reentry_manifest_{suffix}.json`
- `retail_bounce_reentry_rescued_detail_{suffix}.csv`
- `retail_bounce_reentry_summary_{suffix}.csv`
- `retail_bounce_reentry_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 散戶極空5/12R：破失效價後反彈1/6R重進

固定訊號：核心多、散戶歷史分位Q20以下、排除MA5正乖離Q80以上、夜盤收向下5/12個近30日平均振幅為原定進場價、失效價再向下1/4R、每筆成本3點。

三組對照：

- `CANCEL_DAY`：原規則。前30分鐘跌破失效價，當日取消。
- `DIRECT_ENTRY`：08:45起觸及5/12R直接進場，不做失效價過濾。
- `BOUNCE_1_6R`：前30分鐘跌破失效價後，自跌破以來的動態最低價反彈1/6R即觸發；若先創新低，反彈門檻隨新低重算。觸發後等待完整1分鐘，以觸發時刻加1分鐘後第一筆成交價進場。未跌破失效價的交易日完全沿用原規則。

三組進場後都不設停損停利，持有至日盤收盤，只比較進場規則。

```powershell
python -m backtest.futures_retail_bounce_reentry_comparison_backtest --end 2026-11-02
```

輸出符合前置條件之每日 5 分 K 圖庫：

```bash
python -m backtest.futures_retail_bounce_reentry_chart_gallery --end 2026-11-02
```

預設輸出：

`../output/backtest/retail_bounce_reentry/retail_bounce_reentry_5m_gallery.html`

圖庫包含原規則、直接進場、反彈 1/6R 三組的預設進場價、失效價、實際進出場與每日損益，並可按年份、是否跌破失效價、是否救援進場篩選。寬螢幕每列 3 天、中型螢幕 2 天、手機 1 天。

主要輸出：

- `retail_bounce_reentry_summary_*.csv`：三組總績效。
- `retail_bounce_reentry_yearly_*.csv`：分年績效。
- `retail_bounce_reentry_rescued_detail_*.csv`：原本會取消、改由反彈1/6R嘗試救回的交易。
- `retail_bounce_reentry_detail_*.csv`：完整逐筆明細。

