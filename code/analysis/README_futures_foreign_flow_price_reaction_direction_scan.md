# 外資連續大買大賣 × 價格反應方向分析

這支程式研究：外資連續且極端買賣時，價格有沒有配合，能否改善隔日日盤方向預測。

## 外資前置條件

只納入嚴格的連續大買／大賣：

- 最近3個交易日每天外資同向買超或賣超。
- 3日累計外資買賣超 ÷ 3日市場成交金額，位於此前252筆歷史的Q5大買或Q1大賣。

## 價格反應

同時測兩個期間：

1. `1日反應`：訊號日日盤收盤相對訊號日日盤開盤。
2. `3日反應`：訊號日日盤收盤相對最近3個交易日首日的日盤開盤。

每個期間分成：

- 同向確認：外資大買且價格上漲，或外資大賣且價格下跌。
- 背離吸收：外資大買但價格沒有上漲，或外資大賣但價格沒有下跌。

同向確認預測隔日延續外資方向；背離吸收預測隔日反向。每項都測隔日開收到收盤方向，以及隔日極值先後走勢。

每一規則只和「同側連續極端外資、但價格反應相反」的日期比較，不拿全部交易日當參考組。

## 執行

在 `code` 資料夾執行：

```powershell
python -m analysis.futures_foreign_flow_price_reaction_direction_scan `
  --start 2018-01-01 `
  --end 2026-11-05 `
  --split-date 2023-01-01
```

預設輸出至與 `code` 平行的：

```text
output/analysis/futures_foreign_flow_price_reaction_direction_scan/
```

## 輸出

- `foreign_flow_price_reaction_direction_summary_*.csv`：16組規則的全期、探索期、驗證期結果。
- `foreign_flow_price_reaction_direction_yearly_*.csv`：分年穩定性。
- `foreign_flow_price_reaction_direction_classifier_*.csv`：確認與背離邏輯合併買賣兩側後的正確率。
- `foreign_flow_price_reaction_direction_detail_*.csv`：每日外資條件、價格反應及隔日結果。
- `foreign_flow_price_reaction_direction_manifest_*.json`：完整設定與定義。

相鄰訊號高度重疊，因此不要只看單一p值；要同時檢查探索／驗證、FDR校正與逐年一致性。
