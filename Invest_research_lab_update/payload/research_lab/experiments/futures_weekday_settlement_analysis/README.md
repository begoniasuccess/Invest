# 台股星期效應與週三週五統計

## 研究目的與實作

沿用Dashboard市場資料及週期統計，對指定市場及期間產生星期整體方向／振幅、逐年及週三週五比較。

## 結果判讀與限制

目前程式比較星期三、星期五，沒有另外辨識實際期貨結算日，不能稱為精確結算日事件研究。

## 程式來源

- 實作：`code/analysis/futures_weekday_settlement_analysis.py`
- 原有 README：未找到對應 README；本頁依實作建立。
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m analysis.futures_weekday_settlement_analysis --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--market', choices=sorted(MARKETS), default='tx_day', help='市場：taiex、tx_day、tx_night、tx_full；預設 tx_day。')`
- `parser.add_argument('--start', default='2018-01-01', help='統計起日，含當日。')`
- `parser.add_argument('--end', default=None, help='統計迄日，含當日；省略為資料庫最新日。')`
- `parser.add_argument('--output-dir', default=None, help='輸出資料夾；預設為專案根目錄/output/analysis/futures_weekday_settlement_analysis。')`

### 輸出檔名線索

- `wednesday_friday_comparison.csv`
- `weekday_by_year.csv`
- `weekday_overall.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。
