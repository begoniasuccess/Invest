# 散戶極空策略：兩方向擴張測試

固定核心多、排除MA5乖離Q80以上、前30分鐘不進場且先穿停損取消、進場後持有至13:45。

- 方向A：固定散戶Q20，測進場距離 `1/4R、1/3R、5/12R、1/2R`。
- 方向B：固定進場距離 `1/2R`，測散戶門檻 `Q10、Q20、Q25、Q30`。

```powershell
python -m backtest.futures_retail_extreme_expansion_backtest --end 2026-11-02
```

基準 `A_BASE_ENTRY_1_2R` 與 `B_BASE_RETAIL_Q20` 應完全相同，並接近前次187次訊號、64筆進場；若不一致，先不要分析其他版本。
