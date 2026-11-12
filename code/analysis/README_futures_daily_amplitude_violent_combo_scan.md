# 隔日日盤劇烈振幅：核心特徵組合統計

這支程式只回答一個問題：盤前已知的波動與夜盤狀態，能否提高隔日日盤出現「劇烈振幅或極端振幅」的機率。

不再區分強單邊、多空方向或劇烈震盪，也不計算進出場與交易損益。

## 使用的三項特徵

- 近5日日盤平均振幅／此前30日平均振幅
- 夜盤振幅／此前30日平均夜盤振幅
- 夜成交量／此前20日平均夜成交量

每項特徵只使用當時以前最多252筆歷史資料計算百分位，不偷看未來。

## 規則

門檻分成：

- Q4以上：歷史百分位至少60%，也就是偏高的前40%
- Q5：歷史百分位至少80%，也就是最高20%

三項特徵會產生：

- 6組單因子規則
- 6組兩兩組合
- 2組三因子組合

總共14組，不展開其他指標。

## 執行

在 `code` 資料夾執行：

```powershell
python -m analysis.futures_daily_amplitude_violent_combo_scan `
  --start 2018-01-01 `
  --end 2026-11-05 `
  --split-date 2023-01-01
```

預設輸出到與 `code` 平行的：

```text
output/analysis/futures_daily_amplitude_violent_combo_scan/
```

## 輸出

- `violent_amplitude_combo_summary_*.csv`：14組規則的全期、探索期、驗證期機率、Lift、信賴區間與候選判定
- `violent_amplitude_combo_yearly_*.csv`：逐年結果
- `violent_amplitude_combo_detail_*.csv`：每日是否符合各規則，方便日後查詢
- `violent_amplitude_combo_manifest_*.json`：本次設定、規則與限制

重點先看 `summary` 的：

- `驗證期_符合數`
- `驗證期_事件率_pct`
- `驗證期_Lift`
- `驗證期_事件率95CI下限_pct`
- `年度正向率`
- `簡化候選`

事件率提高時仍要檢查符合數。三項條件同時成立可能有很高的事件率，但如果樣本過少，就不能視為穩定結論。
