# 散戶極空策略：分位門檻與進場距離擴張

## 研究目的與實作

固定核心多及排除MA5正乖離極端，分別掃描散戶偏空分位門檻和夜收下方進場距離；前30分鐘觀察、先破失效價取消，進場後持有至收盤。

## 結果判讀與限制

比較兩組共同基準是否一致，再看樣本數、訊號數、成交率、成本及年度績效。

## 程式來源

- 實作：`code/backtest/futures_retail_extreme_expansion_backtest.py`
- 原有 README：`code/backtest/README_futures_retail_extreme_expansion_backtest.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_retail_extreme_expansion_backtest --help
```

### 程式宣告的命令列參數

- `p.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `p.add_argument('--end', type=date.fromisoformat)`
- `p.add_argument('--cost-points', type=float, default=3.0)`
- `p.add_argument('--output-dir', type=Path, default=Path('../output/backtest/retail_expansion'))`

### 輸出檔名線索

- `retail_expansion_detail_{suffix}.csv`
- `retail_expansion_entry_status_{suffix}.csv`
- `retail_expansion_manifest_{suffix}.json`
- `retail_expansion_summary_{suffix}.csv`
- `retail_expansion_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 散戶極空策略：兩方向擴張測試

固定核心多、排除MA5乖離Q80以上、前30分鐘不進場且先穿停損取消、進場後持有至13:45。

- 方向A：固定散戶Q20，測進場距離 `1/4R、1/3R、5/12R、1/2R`。
- 方向B：固定進場距離 `1/2R`，測散戶門檻 `Q10、Q20、Q25、Q30`。

```powershell
python -m backtest.futures_retail_extreme_expansion_backtest --end 2026-11-02
```

基準 `A_BASE_ENTRY_1_2R` 與 `B_BASE_RETAIL_Q20` 應完全相同，並接近前次187次訊號、64筆進場；若不一致，先不要分析其他版本。

