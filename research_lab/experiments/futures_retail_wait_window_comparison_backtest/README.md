# 散戶極空策略：前30分鐘等待限制

## 研究目的與實作

固定核心多、散戶Q20、排除正乖離Q80及5/12R進場，對照原本等待30分鐘且破失效價取消，與開盤起可直接觸價的方案，均尾盤出場。

## 結果判讀與限制

直接比較 WAIT_30M 與 NO_WAIT，檢查新增交易、虧損尾部及年度結果。

## 程式來源

- 實作：`code/backtest/futures_retail_wait_window_comparison_backtest.py`
- 原有 README：`code/backtest/README_futures_retail_wait_window_comparison_backtest.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_retail_wait_window_comparison_backtest --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--cost-points', type=float, default=3.0)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest/retail_wait_window'))`

### 輸出檔名線索

- `retail_wait_window_detail_{suffix}.csv`
- `retail_wait_window_entry_status_{suffix}.csv`
- `retail_wait_window_manifest_{suffix}.json`
- `retail_wait_window_summary_{suffix}.csv`
- `retail_wait_window_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 散戶極空5/12R：前30分鐘限制對照回測

固定條件：核心多、散戶歷史分位Q20以下、排除MA5正乖離Q80以上、夜盤收向下5/12個近30日平均振幅進場、進場後持有至13:45、每筆成本3點。

- `WAIT_30M`：原規則。09:15後才可進場；08:45～09:15若先跌破「進場價再向下1/4R」則當日取消。
- `NO_WAIT`：取消前30分鐘限制。08:45起第一次觸及5/12R進場價就做多，不套用前30分鐘取消條件。

```powershell
python -m backtest.futures_retail_wait_window_comparison_backtest --end 2026-11-02
```

主要看 `retail_wait_window_summary_*.csv`；分年比較看 `retail_wait_window_yearly_*.csv`。

