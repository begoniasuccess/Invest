# 散戶極空5/12R：前30分鐘限制對照回測

固定條件：核心多、散戶歷史分位Q20以下、排除MA5正乖離Q80以上、夜盤收向下5/12個近30日平均振幅進場、進場後持有至13:45、每筆成本3點。

- `WAIT_30M`：原規則。09:15後才可進場；08:45～09:15若先跌破「進場價再向下1/4R」則當日取消。
- `NO_WAIT`：取消前30分鐘限制。08:45起第一次觸及5/12R進場價就做多，不套用前30分鐘取消條件。

```powershell
python -m backtest.futures_retail_wait_window_comparison_backtest --end 2026-11-02
```

主要看 `retail_wait_window_summary_*.csv`；分年比較看 `retail_wait_window_yearly_*.csv`。
