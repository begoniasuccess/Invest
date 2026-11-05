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
