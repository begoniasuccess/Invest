# 日盤開盤向前夜收盤收斂研究

## 研究目的與實作

對齊前日日收、夜收與隔日日盤OHLC，依開盤相對夜收距離及日夜收錨點位置分層，計算朝夜收方向可達空間、收斂程度與組間差異。

## 結果判讀與限制

價格高低只能判斷是否達到某區域，不能推定成交先後；比較不同跳空分組的樣本及達標率。

## 程式來源

- 實作：`code/backtest/futures_open_to_night_close_convergence.py`
- 原有 README：未找到對應 README；本頁依實作建立。
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_open_to_night_close_convergence --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--minimum-sample-size', type=int, default=30)`
- `parser.add_argument('--alpha', type=float, default=0.05)`
- `parser.add_argument('--output-dir', type=Path, default=Path(__file__).resolve().parents[2] / 'output/backtest/futures_open_to_night_close_convergence')`

### 輸出檔名線索

- `open_night_convergence_by_anchor_position_{suffix}.csv`
- `open_night_convergence_by_gap_{suffix}.csv`
- `open_night_convergence_detail_{suffix}.csv`
- `open_night_convergence_gap_matrix_{suffix}.csv`
- `open_night_convergence_manifest_{suffix}.json`
- `open_night_convergence_tests_{suffix}.csv`
- `open_night_convergence_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。
