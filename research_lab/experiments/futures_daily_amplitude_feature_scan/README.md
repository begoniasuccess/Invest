# 劇烈振幅單因子特徵掃描

## 研究目的與實作

以全交易日振幅型態標籤為目標，盤前可知連續特徵做歷史五分位，檢查劇烈以上、強單邊及非單邊等事件率、Lift、檢定與年度一致性。

## 結果判讀與限制

先檢查標籤覆蓋與分組樣本，再看FDR及探索驗證一致性；不模擬成交。

## 程式來源

- 實作：`code/analysis/futures_daily_amplitude_feature_scan.py`
- 原有 README：`code/analysis/README_futures_daily_amplitude_feature_scan.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m analysis.futures_daily_amplitude_feature_scan --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--split-date', type=date.fromisoformat, default=date(2023, 1, 1))`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--history-window', type=int, default=252)`
- `parser.add_argument('--minimum-history', type=int, default=60)`
- `parser.add_argument('--minimum-group-size', type=int, default=20)`
- `parser.add_argument('--minimum-year-size', type=int, default=80)`
- `parser.add_argument('--alpha', type=float, default=0.05)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/analysis/futures_daily_amplitude_feature_scan'))`

### 輸出檔名線索

- `amplitude_feature_buckets_{suffix}.csv`
- `amplitude_feature_detail_{suffix}.csv`
- `amplitude_feature_distribution_{suffix}.csv`
- `amplitude_feature_manifest_{suffix}.json`
- `amplitude_feature_ranking_{suffix}.csv`
- `amplitude_feature_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 劇烈振幅第一階段單因子分析

這支程式以 `research.v_futures_daily_amplitude_pattern` 的全部有效交易日為母體，不套用既有策略篩選。每個特徵都只使用結果日日盤開盤以前已經知道的資料。

## 研究目標

- 劇烈以上：劇烈振幅或極端振幅
- 劇烈強單邊：劇烈以上且為強單邊多／空
- 劇烈強單邊多
- 劇烈強單邊空
- 劇烈非單邊：劇烈以上但不屬於強單邊多／空

## 執行

從 `code` 目錄執行：

```powershell
python -m analysis.futures_daily_amplitude_feature_scan `
  --start 2018-01-01 `
  --end 2026-11-05 `
  --split-date 2023-01-01
```

不需要新增資料表；程式直接讀取前一步已建立的振幅標籤表與正式期貨報表。

## 方法

每項連續特徵先轉成只使用過去最多252筆資料的歷史百分位，再切成五層：

- Q1最低20%
- Q2偏低
- Q3中間
- Q4偏高
- Q5最高20%

每個「特徵 × 研究目標」會計算：

- 各五分位樣本數、事件率、Lift與相對母體增減百分點
- Spearman強度趨勢
- Q5對Q1 Fisher精確檢定與勝算比
- 五層整體卡方檢定與效果量
- BH-FDR多重比較校正
- 2018～2022探索期與2023～2026驗證期是否同方向
- 逐年趨勢一致率

第一階段候選必須同時符合：全期至少一項檢定FDR顯著、探索驗證同方向、驗證期p值小於0.10、年度趨勢一致率至少60%。

## 輸出

預設輸出到與 `code` 平行的 `output/analysis/futures_daily_amplitude_feature_scan/`：

- `amplitude_feature_distribution_*.csv`：各目標、振幅標籤與路徑型態的全期／逐年分布
- `amplitude_feature_buckets_*.csv`：五分位事件率與Lift
- `amplitude_feature_ranking_*.csv`：單因子排名、探索驗證與候選判定
- `amplitude_feature_yearly_*.csv`：逐年穩定性明細
- `amplitude_feature_detail_*.csv`：每日特徵強度與目標標籤
- `amplitude_feature_manifest_*.json`：完整設定與限制

第二階段只應拿第一階段穩定候選做最多兩項特徵的組合，避免119個理想單邊樣本被切得太碎。

