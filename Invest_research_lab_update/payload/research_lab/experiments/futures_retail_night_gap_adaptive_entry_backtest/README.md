# 散戶極空策略：夜盤位移與自適應進場

## 研究目的與實作

將夜收相對昨日日收位移除以前30日均幅分為夜跌、平、漲，交叉測四種進場距離，並比較固定與自適應整體政策；共用前30分鐘失效條件。

## 結果判讀與限制

先看各夜盤分組的隔日路徑，再看政策整體、樣本不平衡和分年結果。

## 程式來源

- 實作：`code/backtest/futures_retail_night_gap_adaptive_entry_backtest.py`
- 原有 README：`code/backtest/README_futures_retail_night_gap_adaptive_entry_backtest.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_retail_night_gap_adaptive_entry_backtest --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--cost-points', type=float, default=3.0)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest/retail_night_gap'))`

### 輸出檔名線索

- `retail_night_gap_entry_status_{suffix}.csv`
- `retail_night_gap_manifest_{suffix}.json`
- `retail_night_gap_matrix_detail_{suffix}.csv`
- `retail_night_gap_matrix_summary_{suffix}.csv`
- `retail_night_gap_path_detail_{suffix}.csv`
- `retail_night_gap_path_summary_{suffix}.csv`
- `retail_night_gap_policy_detail_{suffix}.csv`
- `retail_night_gap_policy_summary_{suffix}.csv`
- `retail_night_gap_policy_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 夜盤相對昨收位移 × 自適應進場距離

固定前置訊號：核心多、綜合散戶歷史分位 Q20 以下、排除 MA5 正乖離歷史分位 Q80 以上。

夜盤位移定義：

`(夜盤收－昨日日盤收)／近30日平均日盤振幅`

先分成夜盤跌（≤-1/6R）、夜盤平（-1/6R～+1/6R）、夜盤漲（≥+1/6R），再於每組測試夜盤收向下 1/3R、5/12R、1/2R、7/12R 四種進場距離。

三種整體政策：

- 固定 5/12R。
- 線性調整：夜盤跌用 1/3R、平用 5/12R、漲用 1/2R。
- 只對夜盤漲保守：跌與平用 5/12R、漲用 7/12R。

所有方案沿用原本前30分鐘不進場、先破失效價則取消、09:15後等待觸價、持有至日盤收盤、扣3點成本的規則。

```powershell
python -m backtest.futures_retail_night_gap_adaptive_entry_backtest --end 2026-11-02
```

最先看：

- `retail_night_gap_path_summary_*.csv`：夜盤漲跌是否改變隔日日盤路徑。
- `retail_night_gap_matrix_summary_*.csv`：三個夜盤分組各自適合哪個進場距離。
- `retail_night_gap_policy_summary_*.csv`：固定與兩種自適應政策的總績效。
- `retail_night_gap_policy_yearly_*.csv`：政策分年穩定性。

