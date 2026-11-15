# 策略多空與隔日報表欄位掃描

## 研究目的與實作

自動找出報表策略多空欄位，將其他數值欄轉為隔日變化、適用時的變化率或原值，類別欄轉隔日分布；比較多空分組及驗證穩定性。

## 結果判讀與限制

篩選候選不是交易績效；須檢查效果量、樣本、FDR及跨年度方向一致性。

## 程式來源

- 實作：`code/backtest/futures_strategy_next_day_field_scan.py`
- 原有 README：未找到對應 README；本頁依實作建立。
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_strategy_next_day_field_scan --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--split-date', type=date.fromisoformat, default=date(2023, 1, 1))`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--minimum-group-size', type=int, default=100)`
- `parser.add_argument('--minimum-year-size', type=int, default=20)`
- `parser.add_argument('--alpha', type=float, default=0.05)`
- `parser.add_argument('--winsor-tail', type=float, default=0.005)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest/futures_strategy_next_day_field_scan'))`

### 輸出檔名線索

- `strategy_next_day_field_scan_all_{suffix}.csv`
- `strategy_next_day_field_scan_manifest_{suffix}.json`
- `strategy_next_day_field_scan_shortlist_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。
