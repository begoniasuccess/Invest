# 散戶極空策略：DIF止跌確認重進

## 研究目的與實作

共用5/12R極空條件，破失效價後用完整5分K的DIF連續上升兩次確認止跌，再等待完整1分鐘進場，與取消及直接進場比較；無盤中停損停利。

## 結果判讀與限制

核對DIF只使用完成K棒、確認時間及救援樣本，避免將確認前的價格用於成交。

## 程式來源

- 實作：`code/backtest/futures_retail_dif_reentry_comparison_backtest.py`
- 原有 README：`code/backtest/README_futures_retail_dif_reentry_comparison_backtest.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_retail_dif_reentry_comparison_backtest --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--cost-points', type=float, default=3.0)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest/retail_dif_reentry'))`

### 輸出檔名線索

- `retail_dif_reentry_detail_{suffix}.csv`
- `retail_dif_reentry_entry_status_{suffix}.csv`
- `retail_dif_reentry_manifest_{suffix}.json`
- `retail_dif_reentry_rescued_detail_{suffix}.csv`
- `retail_dif_reentry_summary_{suffix}.csv`
- `retail_dif_reentry_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

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

