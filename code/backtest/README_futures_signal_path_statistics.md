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
