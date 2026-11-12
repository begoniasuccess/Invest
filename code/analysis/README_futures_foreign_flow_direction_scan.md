# 外資買賣超與隔日日盤方向

這支程式沿用劇烈振幅分析的外資定義，另外檢驗外資是否能預測隔日日盤方向。

## 外資訊號

```text
近3日外資買賣超合計 ÷ 近3日大盤成交金額合計 × 100
```

每一天只和此前最多252筆歷史比較：

- Q1最低20%：外資近期大賣
- Q5最高20%：外資近期大買

資料來源：

- 期貨價格與日盤走勢：`report.v_futures_report_daily_corrected_turning`
- 外資買賣超與大盤成交金額：`core.stock_report_daily` 的 `TAIEX`

## 兩種方向定義

1. 開收方向：隔日日盤收盤高於開盤為上漲，低於開盤為下跌。
2. 極值先後走勢：隔日日盤最高點晚於最低點為漲勢盤，反之為跌勢盤。

程式分別測試外資大賣是否預測向下，以及外資大買是否預測向上。另輸出Q1～Q5完整分位分布與極端訊號整體分類正確率。

## 執行

在 `code` 資料夾執行：

```powershell
python -m analysis.futures_foreign_flow_direction_scan `
  --start 2018-01-01 `
  --end 2026-11-05 `
  --split-date 2023-01-01
```

預設輸出至：

```text
output/analysis/futures_foreign_flow_direction_scan/
```

## 輸出

- `foreign_flow_direction_summary_*.csv`：四個方向假說的全期、探索期與驗證期結果
- `foreign_flow_direction_yearly_*.csv`：逐年一致性
- `foreign_flow_direction_buckets_*.csv`：Q1～Q5的上漲率、下跌率與平均報酬
- `foreign_flow_direction_classifier_*.csv`：Q1預測向下、Q5預測向上的整體正確率
- `foreign_flow_direction_detail_*.csv`：每日訊號與隔日方向明細
- `foreign_flow_direction_manifest_*.json`：設定、定義與限制

這是方向統計，不包含進場點、停損、停利、交易成本或交易損益。
