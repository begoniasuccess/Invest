# 外資買賣超與隔日劇烈振幅

## 研究目的與實作

以外資現貨近3日累計買賣超除同期大盤成交金額，對歷史分位Q1/Q5分組，並與近期及夜盤振幅雙高條件組合，分析隔日劇烈振幅機率。

## 結果判讀與限制

外資單因子比較全體；組合需比較原有雙高基準，避免將既有波動訊號的效果誤歸因外資。

## 程式來源

- 實作：`code/analysis/futures_daily_amplitude_foreign_flow_scan.py`
- 原有 README：`code/analysis/README_futures_daily_amplitude_foreign_flow_scan.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m analysis.futures_daily_amplitude_foreign_flow_scan --help
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
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/analysis/futures_daily_amplitude_foreign_flow_scan'))`

### 輸出檔名線索

- `violent_amplitude_foreign_flow_detail_{suffix}.csv`
- `violent_amplitude_foreign_flow_manifest_{suffix}.json`
- `violent_amplitude_foreign_flow_summary_{suffix}.csv`
- `violent_amplitude_foreign_flow_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 外資買賣超與隔日日盤劇烈振幅

這支程式只測外資現貨買賣超是否能預測隔日日盤「劇烈振幅或極端振幅」，不加入美股訊號。

## 外資指標

資料來源分開讀取後依日期連接：

- 振幅與期貨特徵：`report.v_futures_report_daily_corrected_turning`
- 外資現貨買賣超與大盤成交金額：`core.stock_report_daily` 的 `TAIEX`

外資資料使用欄位：

- `買超_外資_億`
- `總成交金額_億`

預設指標為：

```text
近3日外資買賣超合計 ÷ 近3日市場成交金額合計 × 100
```

每一天只和此前最多252筆歷史比較：

- Q1最低20%：外資近期大賣
- Q5最高20%：外資近期大買

## 四組規則

1. 外資3日累計大賣Q1
2. 外資3日累計大買Q5
3. 近期振幅Q5＋夜盤振幅Q5＋外資大賣Q1
4. 近期振幅Q5＋夜盤振幅Q5＋外資大買Q5

前兩組與全部可判定交易日比較；後兩組則直接與「近期振幅＋夜盤振幅皆Q5」比較，檢查外資是否真的提供額外預測力。

## 執行

在 `code` 資料夾執行：

```powershell
python -m analysis.futures_daily_amplitude_foreign_flow_scan `
  --start 2018-01-01 `
  --end 2026-11-05 `
  --split-date 2023-01-01
```

預設輸出到與 `code` 平行的：

```text
output/analysis/futures_daily_amplitude_foreign_flow_scan/
```

## 輸出

- `violent_amplitude_foreign_flow_summary_*.csv`：四組規則的全期、探索期、驗證期與增量檢定
- `violent_amplitude_foreign_flow_yearly_*.csv`：逐年結果
- `violent_amplitude_foreign_flow_detail_*.csv`：每日外資數值、分位、連續買賣天數及規則判定
- `violent_amplitude_foreign_flow_manifest_*.json`：設定、公式與限制

主要先看摘要中的：

- `驗證期_參考組事件率_pct`
- `驗證期_事件率_pct`
- `驗證期_相對參考組Lift`
- `驗證期_增量Fisher_p值`
- `年度正向率`
- `外資增量候選`

外資指標代表統計關聯，不代表外資買賣必然造成隔日波動。

