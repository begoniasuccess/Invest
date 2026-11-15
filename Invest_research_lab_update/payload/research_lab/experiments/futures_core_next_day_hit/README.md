# 五日核心多空隔日命中率

## 研究目的與實作

以前一交易日五日價量核心多空，對齊下一實際交易日日盤開高低收，計算方向命中、上下影線超過門檻及綜合命中；另計算依訊號方向的開收盤點數。

## 結果判讀與限制

任一長影線即可增加綜合命中，與方向勝率及真實可成交損益不同；分開檢查多空及影線追加命中。

## 程式來源

- 實作：`code/backtest/futures_core_next_day_hit.py`
- 原有 README：未找到對應 README；本頁依實作建立。
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_core_next_day_hit --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', required=True, type=date.fromisoformat)`
- `parser.add_argument('--end', required=True, type=date.fromisoformat)`
- `parser.add_argument('--shadow-threshold', type=float, default=125.0)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest'))`
- `parser.add_argument('--no-save-db', action='store_true', help='只輸出 CSV，不寫入資料庫')`

### 輸出檔名線索

- `futures_core_next_day_detail_corrected_{suffix}.csv`
- `futures_core_next_day_summary_corrected_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。
