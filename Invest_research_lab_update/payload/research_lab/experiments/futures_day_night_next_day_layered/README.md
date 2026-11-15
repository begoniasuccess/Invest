# 日夜盤方向與隔日路徑分層

## 研究目的與實作

依前日日盤漲跌、夜盤漲跌、反向比率及幅度分桶，觀察隔日價格位於日收夜收之間或外側，及反向與同向分組差異。

## 結果判讀與限制

確認日夜盤時間定義，檢查各分桶樣本數、對照組與年度變化。

## 程式來源

- 實作：`code/backtest/futures_day_night_next_day_layered.py`
- 原有 README：`code/backtest/README_futures_day_night_next_day_layered.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_day_night_next_day_layered --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--minimum-sample-size', type=int, default=30)`
- `parser.add_argument('--alpha', type=float, default=0.05)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest/futures_day_night_next_day_layered'))`

### 輸出檔名線索

- `day_night_bucket_matrix_{suffix}.csv`
- `day_night_layered_detail_{suffix}.csv`
- `day_night_layered_manifest_{suffix}.json`
- `day_night_layered_summary_{suffix}.csv`
- `day_night_layered_yearly_{suffix}.csv`
- `day_night_opposite_vs_same_tests_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 日盤收、夜盤收與隔日日盤路徑分層回測

在專案 `code` 目錄執行：

```powershell
python -m backtest.futures_day_night_next_day_layered --start 2018-01-01 --end 2026-10-16
```

## 研究定義

- 日盤漲跌：昨日日盤收相對再前一日日盤收。
- 夜盤漲跌：昨日夜盤收相對昨日日盤收。
- 日夜反向：上述兩個漲跌幅異號。
- 隔日：訊號日之後下一個實際存在的交易日，週五接週一。
- 兩者之間：隔日價格位於昨日日盤收與昨日夜盤收的閉區間。
- 夜收外側：沿夜盤方向超過夜盤收。
- 日收外側：越過昨日日盤收，完全收復夜盤走勢。

## 分層

- 日盤、夜盤漲跌幅使用 0.25% 桶，尾端合併為 ±1%以上。
- 夜盤絕對幅度：0～0.25%、0.25～0.5%、0.5～1%、1%以上。
- 反向比率：夜盤絕對漲跌幅 ÷ 日盤絕對漲跌幅，分為 0～0.5、0.5～1、1～1.5、1.5以上。

## 輸出

- `detail`：逐日價格、分層與隔日結果。
- `bucket_matrix`：日盤漲跌桶 × 夜盤漲跌桶二維矩陣。
- `layered_summary`：方向、夜盤幅度、反向比率分層摘要。
- `opposite_vs_same_tests`：相同夜盤幅度桶內，反向與同向的比例檢定及FDR校正。
- `yearly`：分年度穩定性。
- `manifest`：公式與研究口徑。

日K只能判斷當日是否曾觸及兩個價位，不能判斷先觸及哪一邊。若候選情境成立，下一階段再使用一分K或五分K研究盤中路徑與實際進出規則。

