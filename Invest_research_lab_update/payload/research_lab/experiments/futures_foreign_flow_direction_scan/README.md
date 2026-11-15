# 外資買賣超與隔日多空方向

## 研究目的與實作

沿用外資近3日累計買賣超率，分位Q1/Q5測試外資大賣向下與大買向上的假說；以隔日開收方向與極值先後走勢分別定義方向。

## 結果判讀與限制

兩種方向標籤分開判讀；正確率需與基準、樣本與年度穩定性比較。

## 程式來源

- 實作：`code/analysis/futures_foreign_flow_direction_scan.py`
- 原有 README：`code/analysis/README_futures_foreign_flow_direction_scan.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m analysis.futures_foreign_flow_direction_scan --help
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
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/analysis/futures_foreign_flow_direction_scan'))`

### 輸出檔名線索

- `foreign_flow_direction_buckets_{suffix}.csv`
- `foreign_flow_direction_classifier_{suffix}.csv`
- `foreign_flow_direction_detail_{suffix}.csv`
- `foreign_flow_direction_manifest_{suffix}.json`
- `foreign_flow_direction_summary_{suffix}.csv`
- `foreign_flow_direction_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 外資買賣超與隔日日盤方向

這支程式沿用劇烈振幅分析的外資定義，另外檢驗外資是否能預測隔日日盤方向。

## 外資訊號

```text
近3日外資買賣超合計 ÷ 近3日大盤成交金額合計 × 100
```

每一天只和此前最多252筆歷史比較：

- Q1最低20%：外資近期大賣
- Q5最高20%：外資近期大買

資料來源：

- 期貨價格與日盤走勢：`report.v_futures_report_daily_corrected_turning`
- 外資買賣超與大盤成交金額：`core.stock_report_daily` 的 `TAIEX`

## 兩種方向定義

1. 開收方向：隔日日盤收盤高於開盤為上漲，低於開盤為下跌。
2. 極值先後走勢：隔日日盤最高點晚於最低點為漲勢盤，反之為跌勢盤。

程式分別測試外資大賣是否預測向下，以及外資大買是否預測向上。另輸出Q1～Q5完整分位分布與極端訊號整體分類正確率。

## 執行

在 `code` 資料夾執行：

```powershell
python -m analysis.futures_foreign_flow_direction_scan `
  --start 2018-01-01 `
  --end 2026-11-05 `
  --split-date 2023-01-01
```

預設輸出至：

```text
output/analysis/futures_foreign_flow_direction_scan/
```

## 輸出

- `foreign_flow_direction_summary_*.csv`：四個方向假說的全期、探索期與驗證期結果
- `foreign_flow_direction_yearly_*.csv`：逐年一致性
- `foreign_flow_direction_buckets_*.csv`：Q1～Q5的上漲率、下跌率與平均報酬
- `foreign_flow_direction_classifier_*.csv`：Q1預測向下、Q5預測向上的整體正確率
- `foreign_flow_direction_detail_*.csv`：每日訊號與隔日方向明細
- `foreign_flow_direction_manifest_*.json`：設定、定義與限制

這是方向統計，不包含進場點、停損、停利、交易成本或交易損益。

