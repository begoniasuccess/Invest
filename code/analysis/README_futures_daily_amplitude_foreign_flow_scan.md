# 外資買賣超與隔日日盤劇烈振幅

這支程式只測外資現貨買賣超是否能預測隔日日盤「劇烈振幅或極端振幅」，不加入美股訊號。

## 外資指標

資料來源分開讀取後依日期連接：

- 振幅與期貨特徵：`report.v_futures_report_daily_corrected_turning`
- 外資現貨買賣超與大盤成交金額：`core.stock_report_daily` 的 `TAIEX`

外資資料使用欄位：

- `買超_外資_億`
- `總成交金額_億`

預設指標為：

```text
近3日外資買賣超合計 ÷ 近3日市場成交金額合計 × 100
```

每一天只和此前最多252筆歷史比較：

- Q1最低20%：外資近期大賣
- Q5最高20%：外資近期大買

## 四組規則

1. 外資3日累計大賣Q1
2. 外資3日累計大買Q5
3. 近期振幅Q5＋夜盤振幅Q5＋外資大賣Q1
4. 近期振幅Q5＋夜盤振幅Q5＋外資大買Q5

前兩組與全部可判定交易日比較；後兩組則直接與「近期振幅＋夜盤振幅皆Q5」比較，檢查外資是否真的提供額外預測力。

## 執行

在 `code` 資料夾執行：

```powershell
python -m analysis.futures_daily_amplitude_foreign_flow_scan `
  --start 2018-01-01 `
  --end 2026-11-05 `
  --split-date 2023-01-01
```

預設輸出到與 `code` 平行的：

```text
output/analysis/futures_daily_amplitude_foreign_flow_scan/
```

## 輸出

- `violent_amplitude_foreign_flow_summary_*.csv`：四組規則的全期、探索期、驗證期與增量檢定
- `violent_amplitude_foreign_flow_yearly_*.csv`：逐年結果
- `violent_amplitude_foreign_flow_detail_*.csv`：每日外資數值、分位、連續買賣天數及規則判定
- `violent_amplitude_foreign_flow_manifest_*.json`：設定、公式與限制

主要先看摘要中的：

- `驗證期_參考組事件率_pct`
- `驗證期_事件率_pct`
- `驗證期_相對參考組Lift`
- `驗證期_增量Fisher_p值`
- `年度正向率`
- `外資增量候選`

外資指標代表統計關聯，不代表外資買賣必然造成隔日波動。
