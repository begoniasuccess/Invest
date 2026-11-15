# 結果回顧｜散戶極空策略：夜盤位移與自適應進場

批次：`hist-a0a5dd6e26df2efaff7f`  
研究期間：2018-01-01 ～ 2026-11-02  
執行時間：未記錄  
參數來源：manifest 設定快照（以檔案實際記載為限）

## 研究結果與判讀

### retail_night_gap_matrix_summary_20180101_20260127.csv

來源：`output/backtest/retail_night_gap/retail_night_gap_matrix_summary_20180101_20260127.csv`

原始表共 12 列；以下依原始欄位或選定指標整理，未把重疊策略或多組參數相加。
- 全部列：12/12 組／列淨點數為正；最高 722（ENTRY_5_12R／夜盤漲（≥+1/6R）），最低 69（ENTRY_7_12R／夜盤平（-1/6R～+1/6R））。
上述是原始表內的描述性排名，不是樣本外選模。應同看交易數、平均每筆、PF、最大回撤、成本與年度表；尤其參數重疊不可合併為獨立績效。

| variant | night_gap_group_label | entry_count | signal_count | win_rate_pct | total_net_points | average_net_points | profit_factor | max_drawdown_points |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ENTRY_5_12R | 夜盤漲（≥+1/6R） | 32 | 75 | 62.5 | 722 | 22.5625 | 2.2622 | -250 |
| ENTRY_5_12R | 夜盤跌（≤-1/6R） | 23 | 61 | 56.5217 | 668 | 29.0435 | 3.1139 | -80 |
| ENTRY_1_2R | 夜盤漲（≥+1/6R） | 28 | 75 | 67.8571 | 615 | 21.9643 | 2.4009 | -228 |
| ENTRY_7_12R | 夜盤跌（≤-1/6R） | 17 | 61 | 52.9412 | 267 | 15.7059 | 2.4355 | -152 |
| ENTRY_7_12R | 夜盤平（-1/6R～+1/6R） | 10 | 49 | 60 | 69 | 6.9 | 1.2816 | -234 |

### retail_night_gap_path_summary_20180101_20260127.csv

來源：`output/backtest/retail_night_gap/retail_night_gap_path_summary_20180101_20260127.csv`

原始表共 3 列；以下依原始欄位或選定指標整理，未把重疊策略或多組參數相加。

| night_gap_group_label | signal_count | next_day_close_up_rate_pct |
| --- | --- | --- |
| 夜盤跌（≤-1/6R） | 61 | 54.0984 |
| 夜盤平（-1/6R～+1/6R） | 49 | 51.0204 |
| 夜盤漲（≥+1/6R） | 75 | 54.6667 |

### retail_night_gap_policy_summary_20180101_20260127.csv

來源：`output/backtest/retail_night_gap/retail_night_gap_policy_summary_20180101_20260127.csv`

原始表共 3 列；以下依原始欄位或選定指標整理，未把重疊策略或多組參數相加。
- 全部列：3/3 組／列淨點數為正；最高 1,693（FIXED_5_12R），最低 1,468（ADAPTIVE_UP_CONSERVATIVE）。
上述是原始表內的描述性排名，不是樣本外選模。應同看交易數、平均每筆、PF、最大回撤、成本與年度表；尤其參數重疊不可合併為獨立績效。

| variant | entry_count | signal_count | win_rate_pct | total_net_points | average_net_points | profit_factor | max_drawdown_points |
| --- | --- | --- | --- | --- | --- | --- | --- |
| FIXED_5_12R | 76 | 185 | 61.8421 | 1,693 | 22.2763 | 2.3609 | -271 |
| ADAPTIVE_LINEAR | 70 | 185 | 64.2857 | 1,479 | 21.1286 | 2.2397 | -242 |
| ADAPTIVE_UP_CONSERVATIVE | 67 | 185 | 61.194 | 1,468 | 21.9104 | 2.3981 | -253 |

## 研究限制與後續

- 上述數字來自本批次原始輸出，未重新執行回測或更改成交、成本與樣本口徑。
- 同一實驗可能包含重疊日期、不同方向、網格參數或不同模型。不要直接將表格各列加總。
- 優先檢查樣本量、交易成本、資料品質、探索／驗證分割與逐年一致性；統計關聯或樣本內最佳列不構成可交易策略。
- 歷史批次沒有紀錄的時間、完整參數或程式版本維持未知。新批次可利用manifest重現參數，但資料庫日後更新可能改變結果。

## 我的研究心得／下一步

（可在 Dashboard 直接編輯、儲存。本段留給後續人工研究。）
