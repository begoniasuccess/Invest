# 五組複合訊號 × 四種出場法

## 研究目的與實作

五組複合訊號共用夜收向反方向半個均幅限價，前30分鐘觀察若先破失效價則取消；比較尾盤出場、固定停損及兩種移動保利方案。

## 結果判讀與限制

在相同進場樣本比較 A/B/C/D，檢查淨點數、回撤、保利及未進場原因。

## 程式來源

- 實作：`code/backtest/futures_combination_exit_backtest.py`
- 原有 README：`code/backtest/README_futures_combination_exit_backtest.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_combination_exit_backtest --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat, default=None)`
- `parser.add_argument('--cost-points', type=float, default=3.0)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest/combination_exit'))`

### 輸出檔名線索

- `combination_exit_detail_{suffix}.csv`
- `combination_exit_diagnostics_{suffix}.csv`
- `combination_exit_manifest_{suffix}.json`
- `combination_exit_summary_{suffix}.csv`
- `combination_exit_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 五組組合訊號 × 四種出場法逐筆回測

這支程式是獨立研究工具，不會被 Dashboard 或每日報表更新呼叫。

## 執行

```powershell
python -m backtest.futures_combination_exit_backtest --end 2026-11-02
```

預設每筆扣除來回成本 3 點；可用 `--cost-points 0` 產生未計成本結果。

## 共用進場規則

- 以訊號日夜盤收盤價與訊號日以前30個完整交易日的平均振幅 `R` 計算。
- 做多掛價：夜盤收－`1/2R`；做空掛價：夜盤收＋`1/2R`。
- 08:45～09:15只觀察、不進場。
- 若觀察期內已穿越預定停損（掛價再逆向`1/4R`），當日取消進場。
- 09:15後才等待首次觸價，並以第一筆實際成交價計算，不倒填理論掛價。
- 尾盤仍未觸價則記為未進場。

## 四個出場版本

- `A_CLOSE_ONLY`：觸價進場後，尾盤出。
- `B_STOP_ONLY`：停損為30日平均振幅的1/4，否則尾盤出。
- `C_OLD_2_1`：停損1/4；獲利達1/4後，從最佳價回撤1/5振幅出場。
- `D_NEW_1_3_1_6`：停損1/4；獲利達1/3後，從最佳價回撤1/6振幅出場。

沒有固定停利。平均振幅只使用訊號日及之前30個完整交易日；進出場依逐筆成交資料的時間順序判斷。

## 輸出

- `combination_exit_detail_*.csv`：每筆交易與保利診斷。
- `combination_exit_summary_*.csv`：五組訊號與四版本總績效。
- `combination_exit_yearly_*.csv`：逐年穩定性。
- `combination_exit_diagnostics_*.csv`：按出場原因彙整。
- `combination_exit_manifest_*.json`：本次設定與防偷看定義。

明細另外保留原本尾盤出場損益、MFE、MAE、保利啟動時間、最終保利線，以及保利出場後是否再創更有利極值，方便判斷保利是否過早。

