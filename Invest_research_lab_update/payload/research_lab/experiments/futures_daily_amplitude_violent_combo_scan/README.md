# 劇烈振幅多因子組合分析

## 研究目的與實作

以近期日盤均幅、前夜振幅、前夜成交量相對歷史基準的百分位，測試Q4以上或Q5的單因子、兩因子、三因子共14組規則對隔日劇烈以上事件機率。

## 結果判讀與限制

比較事件率、Lift、信賴區間及樣本減少；不區分方向也不計交易損益。

## 程式來源

- 實作：`code/analysis/futures_daily_amplitude_violent_combo_scan.py`
- 原有 README：`code/analysis/README_futures_daily_amplitude_violent_combo_scan.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m analysis.futures_daily_amplitude_violent_combo_scan --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--split-date', type=date.fromisoformat, default=date(2023, 1, 1))`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--history-window', type=int, default=252)`
- `parser.add_argument('--minimum-history', type=int, default=60)`
- `parser.add_argument('--minimum-rule-sample', type=int, default=20)`
- `parser.add_argument('--minimum-year-rule-sample', type=int, default=8)`
- `parser.add_argument('--alpha', type=float, default=0.05)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/analysis/futures_daily_amplitude_violent_combo_scan'))`

### 輸出檔名線索

- `violent_amplitude_combo_detail_{suffix}.csv`
- `violent_amplitude_combo_manifest_{suffix}.json`
- `violent_amplitude_combo_summary_{suffix}.csv`
- `violent_amplitude_combo_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 隔日日盤劇烈振幅：核心特徵組合統計

這支程式只回答一個問題：盤前已知的波動與夜盤狀態，能否提高隔日日盤出現「劇烈振幅或極端振幅」的機率。

不再區分強單邊、多空方向或劇烈震盪，也不計算進出場與交易損益。

## 使用的三項特徵

- 近5日日盤平均振幅／此前30日平均振幅
- 夜盤振幅／此前30日平均夜盤振幅
- 夜成交量／此前20日平均夜成交量

每項特徵只使用當時以前最多252筆歷史資料計算百分位，不偷看未來。

## 規則

門檻分成：

- Q4以上：歷史百分位至少60%，也就是偏高的前40%
- Q5：歷史百分位至少80%，也就是最高20%

三項特徵會產生：

- 6組單因子規則
- 6組兩兩組合
- 2組三因子組合

總共14組，不展開其他指標。

## 執行

在 `code` 資料夾執行：

```powershell
python -m analysis.futures_daily_amplitude_violent_combo_scan `
  --start 2018-01-01 `
  --end 2026-11-05 `
  --split-date 2023-01-01
```

預設輸出到與 `code` 平行的：

```text
output/analysis/futures_daily_amplitude_violent_combo_scan/
```

## 輸出

- `violent_amplitude_combo_summary_*.csv`：14組規則的全期、探索期、驗證期機率、Lift、信賴區間與候選判定
- `violent_amplitude_combo_yearly_*.csv`：逐年結果
- `violent_amplitude_combo_detail_*.csv`：每日是否符合各規則，方便日後查詢
- `violent_amplitude_combo_manifest_*.json`：本次設定、規則與限制

重點先看 `summary` 的：

- `驗證期_符合數`
- `驗證期_事件率_pct`
- `驗證期_Lift`
- `驗證期_事件率95CI下限_pct`
- `年度正向率`
- `簡化候選`

事件率提高時仍要檢查符合數。三項條件同時成立可能有很高的事件率，但如果樣本過少，就不能視為穩定結論。

