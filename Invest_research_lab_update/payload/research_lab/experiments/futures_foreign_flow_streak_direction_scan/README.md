# 外資連續買賣超與隔日方向

## 研究目的與實作

比較外資連續2、3、5日同向買賣超，以及連3日同向且累計買賣超率屬極端Q1/Q5，檢查隔日開收及極值先後方向。

## 結果判讀與限制

不能將3日累計同向誤認為每日連續同向；比較不同條件樣本與基準。

## 程式來源

- 實作：`code/analysis/futures_foreign_flow_streak_direction_scan.py`
- 原有 README：`code/analysis/README_futures_foreign_flow_streak_direction_scan.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m analysis.futures_foreign_flow_streak_direction_scan --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--split-date', type=date.fromisoformat, default=date(2023, 1, 1))`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--history-window', type=int, default=252)`
- `parser.add_argument('--minimum-history', type=int, default=60)`
- `parser.add_argument('--foreign-days', type=int, default=3)`
- `parser.add_argument('--lower-percentile', type=float, default=20.0)`
- `parser.add_argument('--upper-percentile', type=float, default=80.0)`
- `parser.add_argument('--minimum-rule-sample', type=int, default=20)`
- `parser.add_argument('--minimum-year-rule-sample', type=int, default=5)`
- `parser.add_argument('--alpha', type=float, default=0.05)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/analysis/futures_foreign_flow_streak_direction_scan'))`

### 輸出檔名線索

- `foreign_flow_streak_direction_classifier_{suffix}.csv`
- `foreign_flow_streak_direction_detail_{suffix}.csv`
- `foreign_flow_streak_direction_manifest_{suffix}.json`
- `foreign_flow_streak_direction_summary_{suffix}.csv`
- `foreign_flow_streak_direction_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 外資連續買賣超與隔日日盤方向

這支程式檢驗外資連續買超或賣超，是否比單純的3日累計幅度更能預測隔日日盤方向。

## 條件

分別測試：

- 連續2日每天賣超／買超
- 連續3日每天賣超／買超
- 連續5日每天賣超／買超
- 連續3日每天同向，且3日累計買賣超率進入歷史Q1大賣／Q5大買

最後一組才定義為嚴格的「連續大賣／連續大買」。

每個條件都檢驗兩種方向：

1. 隔日日盤收盤相對開盤上漲或下跌。
2. 隔日日盤為漲勢盤或跌勢盤。

## 執行

在 `code` 資料夾執行：

```powershell
python -m analysis.futures_foreign_flow_streak_direction_scan `
  --start 2018-01-01 `
  --end 2026-11-05 `
  --split-date 2023-01-01
```

預設輸出至：

```text
output/analysis/futures_foreign_flow_streak_direction_scan/
```

## 輸出

- `foreign_flow_streak_direction_summary_*.csv`：16組規則的全期、探索期與驗證期結果
- `foreign_flow_streak_direction_yearly_*.csv`：逐年穩定性
- `foreign_flow_streak_direction_classifier_*.csv`：買超預測向上、賣超預測向下的整體正確率
- `foreign_flow_streak_direction_detail_*.csv`：每日連續條件與隔日方向
- `foreign_flow_streak_direction_manifest_*.json`：設定、定義與限制

相鄰日期的連續條件會重疊，因此不可只看p值，必須同時看探索期、驗證期與逐年一致性。

