# 訊號日與隔日日盤完整價格路徑

## 研究目的與實作

對齊多空訊號與下一交易日實際Tick，計算開收、MFE/MAE、首次達標、跳空及達標後回吐等分布，建立停損與移動保利研究基礎。

## 結果判讀與限制

依多空、基準和門檻比較先有利或先不利；最大有利空間不是可直接成交的損益。

## 程式來源

- 實作：`code/backtest/futures_signal_path_statistics.py`
- 原有 README：`code/backtest/README_futures_signal_path_statistics.md`
- 現有設定：`code/backtest/configs/futures_signal_path_statistics.json`

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_signal_path_statistics --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--config', type=Path, default=Path('backtest/configs/futures_signal_path_statistics.json'))`

### 輸出檔名線索

- `futures_signal_manifest_{suffix}.json`
- `futures_signal_{name}_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 台指期訊號隔日日盤基礎統計

這個模組在設計停損停利策略前，先分析前一交易日多空訊號出現後，隔日日盤
08:45～13:45 的實際 Tick 價格路徑。預設期間為 2016-01-01～2026-09-10。

## 執行

在專案 `code` 目錄執行：

```powershell
python -m backtest.futures_signal_path_statistics --config backtest/configs/futures_signal_path_statistics.json
```

完成後，將以下整個資料夾壓縮：

```text
../output/backtest/signal_path_statistics
```

## 主要輸出

- `daily_path_detail`：每個訊號日的隔日日盤開收、MFE、MAE、發生時間及跳空。
- `direction_summary`：多空方向性、勝率，以及開收到、MFE、MAE分位數。
- `threshold_first_hit_detail`／`threshold_summary`：各點數與百分比門檻的達標率，及先有利或先不利。
- `gap_bucket_summary`：朝訊號方向跳空的分層績效。
- `trailing_pullback_detail`／`trailing_summary`：達到指定浮盈後的回吐，以及回吐後是否再創最佳價。
- `environment_summary`：以前一日已知的MA20方向、MA20上下、位置與波動分組。
- `baseline_summary`：多訊號、空訊號、無明確訊號和全部交易日的開收盤基準。
- `data_quality_summary`：缺少開盤契約或Tick的日期數量。
- `manifest`：本次分析期間、訊號欄位及完整輸出清單。

所有百分比設定直接用百分點，例如 `0.2` 代表 `0.2%`。

