# 結果回顧｜日盤開盤漲跌反轉事件

批次：`hist-8ca257b9def5cd94ece9`  
研究期間：2018-01-01 ～ 2026-11-06  
執行時間：2026-11-08T21:43:55.165195  
參數來源：manifest 設定快照（以檔案實際記載為限）

## 研究結果與判讀

原manifest記錄2038個有效交易日（2018/01/01～2026/11/06）。反轉/續行為事件門檻，不是交易策略；同一天多個門檻高度重疊，判讀需看觸發頻率及品質表。

### data_quality_summary.csv

來源：`output/backtest/intraday_reversal/data_quality_summary.csv`

原始表共 2 列；以下依原始欄位或選定指標整理，未把重疊策略或多組參數相加。

| status | days |
| --- | --- |
| MISSING_CLOSE | 109 |
| OK | 2,038 |

### validation_summary.csv

來源：`output/backtest/intraday_reversal/validation_summary.csv`

原始表共 60,925 列；以下依原始欄位或選定指標整理，未把重疊策略或多組參數相加。
不同門檻在同一天可能重疊，且反轉比例不等於可成交的策略勝率；應優先核對樣本數、信賴區間與有效 Tick 品質。

| direction | threshold_type | threshold_value | threshold_points | reversal_target | sample_count | reversal_rate | wilson95_low | wilson95_high | low_sample |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DOWN | PCT | 0.25 | 21.5225 | 50 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 100 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 150 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 200 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 300 | 1 | 0 | 0 | 0.7935 | True |
| DOWN | PCT | 0.25 | 22.05 | 50 | 1 | 0 | 0 | 0.7935 | True |
| DOWN | PCT | 0.25 | 22.05 | 100 | 1 | 0 | 0 | 0.7935 | True |
| DOWN | PCT | 0.25 | 22.05 | 150 | 1 | 0 | 0 | 0.7935 | True |
| DOWN | PCT | 0.25 | 22.05 | 200 | 1 | 0 | 0 | 0.7935 | True |
| DOWN | PCT | 0.25 | 22.05 | 300 | 1 | 0 | 0 | 0.7935 | True |
| DOWN | PCT | 0.25 | 23.4175 | 50 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 23.4175 | 100 | 1 | 0 | 0 | 0.7935 | True |

### first_hit_probability.csv

來源：`output/backtest/intraday_reversal/first_hit_probability.csv`

原始表共 241,740 列；以下依原始欄位或選定指標整理，未把重疊策略或多組參數相加。
不同門檻在同一天可能重疊，且反轉比例不等於可成交的策略勝率；應優先核對樣本數、信賴區間與有效 Tick 品質。

| direction | threshold_type | threshold_value | threshold_points | reversal_target | continuation_target | sample_count | reversal_rate | decisive_reversal_rate | wilson95_low | wilson95_high | low_sample |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DOWN | PCT | 0.25 | 21.5225 | 50 | 50 | 1 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 50 | 100 | 1 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 50 | 150 | 1 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 50 | 200 | 1 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 100 | 50 | 1 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 100 | 100 | 1 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 100 | 150 | 1 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 100 | 200 | 1 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 150 | 50 | 1 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 150 | 100 | 1 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 150 | 150 | 1 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 150 | 200 | 1 | 1 | 1 | 0.2065 | 1 | True |

### reversal_probability.csv

來源：`output/backtest/intraday_reversal/reversal_probability.csv`

原始表共 60,435 列；以下依原始欄位或選定指標整理，未把重疊策略或多組參數相加。
不同門檻在同一天可能重疊，且反轉比例不等於可成交的策略勝率；應優先核對樣本數、信賴區間與有效 Tick 品質。

| direction | threshold_type | threshold_value | threshold_points | reversal_target | sample_count | reversal_rate | wilson95_low | wilson95_high | low_sample |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DOWN | PCT | 0.25 | 21.5225 | 50 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 100 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 150 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 200 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 21.5225 | 300 | 1 | 0 | 0 | 0.7935 | True |
| DOWN | PCT | 0.25 | 22.05 | 50 | 1 | 0 | 0 | 0.7935 | True |
| DOWN | PCT | 0.25 | 22.05 | 100 | 1 | 0 | 0 | 0.7935 | True |
| DOWN | PCT | 0.25 | 22.05 | 150 | 1 | 0 | 0 | 0.7935 | True |
| DOWN | PCT | 0.25 | 22.05 | 200 | 1 | 0 | 0 | 0.7935 | True |
| DOWN | PCT | 0.25 | 22.05 | 300 | 1 | 0 | 0 | 0.7935 | True |
| DOWN | PCT | 0.25 | 23.4175 | 50 | 1 | 1 | 0.2065 | 1 | True |
| DOWN | PCT | 0.25 | 23.4175 | 100 | 1 | 0 | 0 | 0.7935 | True |

### trigger_frequency.csv

來源：`output/backtest/intraday_reversal/trigger_frequency.csv`

原始表共 52 列；以下依原始欄位或選定指標整理，未把重疊策略或多組參數相加。

| direction | threshold_type | threshold_value | trigger_days | eligible_days | trigger_frequency |
| --- | --- | --- | --- | --- | --- |
| DOWN | PCT | 0.25 | 1,398 | 2,038 | 0.686 |
| DOWN | PCT | 0.5 | 902 | 2,038 | 0.4426 |
| DOWN | PCT | 0.75 | 554 | 2,038 | 0.2718 |
| DOWN | PCT | 1 | 343 | 2,038 | 0.1683 |
| DOWN | PCT | 1.5 | 136 | 2,038 | 0.0667 |
| DOWN | PCT | 2 | 71 | 2,038 | 0.0348 |
| DOWN | PCT | 2.5 | 43 | 2,038 | 0.0211 |
| DOWN | PCT | 3 | 28 | 2,038 | 0.0137 |
| DOWN | PCT | 4 | 7 | 2,038 | 0.0034 |
| DOWN | POINT | 50 | 1,236 | 2,038 | 0.6065 |
| DOWN | POINT | 100 | 750 | 2,038 | 0.368 |
| DOWN | POINT | 150 | 458 | 2,038 | 0.2247 |

## 研究限制與後續

- 上述數字來自本批次原始輸出，未重新執行回測或更改成交、成本與樣本口徑。
- 同一實驗可能包含重疊日期、不同方向、網格參數或不同模型。不要直接將表格各列加總。
- 優先檢查樣本量、交易成本、資料品質、探索／驗證分割與逐年一致性；統計關聯或樣本內最佳列不構成可交易策略。
- 歷史批次沒有紀錄的時間、完整參數或程式版本維持未知。新批次可利用manifest重現參數，但資料庫日後更新可能改變結果。

## 我的研究心得／下一步

（可在 Dashboard 直接編輯、儲存。本段留給後續人工研究。）
