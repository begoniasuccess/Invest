# 外資連續買賣超與隔日日盤方向

這支程式檢驗外資連續買超或賣超，是否比單純的3日累計幅度更能預測隔日日盤方向。

## 條件

分別測試：

- 連續2日每天賣超／買超
- 連續3日每天賣超／買超
- 連續5日每天賣超／買超
- 連續3日每天同向，且3日累計買賣超率進入歷史Q1大賣／Q5大買

最後一組才定義為嚴格的「連續大賣／連續大買」。

每個條件都檢驗兩種方向：

1. 隔日日盤收盤相對開盤上漲或下跌。
2. 隔日日盤為漲勢盤或跌勢盤。

## 執行

在 `code` 資料夾執行：

```powershell
python -m analysis.futures_foreign_flow_streak_direction_scan `
  --start 2018-01-01 `
  --end 2026-11-05 `
  --split-date 2023-01-01
```

預設輸出至：

```text
output/analysis/futures_foreign_flow_streak_direction_scan/
```

## 輸出

- `foreign_flow_streak_direction_summary_*.csv`：16組規則的全期、探索期與驗證期結果
- `foreign_flow_streak_direction_yearly_*.csv`：逐年穩定性
- `foreign_flow_streak_direction_classifier_*.csv`：買超預測向上、賣超預測向下的整體正確率
- `foreign_flow_streak_direction_detail_*.csv`：每日連續條件與隔日方向
- `foreign_flow_streak_direction_manifest_*.json`：設定、定義與限制

相鄰日期的連續條件會重疊，因此不可只看p值，必須同時看探索期、驗證期與逐年一致性。
