# 原始訊號強度掃描

這支程式不使用既有 `策略_*` 多空結果，也不修改正式資料庫 View。它把價量、線型、位置、法人／散戶、期貨主力與選擇權的連續指標分開分析，檢查它們與隔日台指期日盤結果的關係。

## 執行方式

請先切到專案的 `code` 目錄，並確認 `POSTGRES_DSN` 已設定。

PowerShell 單行：

```powershell
python -m backtest.futures_raw_signal_strength_scan --start 2018-01-01 --end 2026-10-16
```

PowerShell 多行必須使用反引號 `` ` ``，不是 `^`：

```powershell
python -m backtest.futures_raw_signal_strength_scan `
  --start 2018-01-01 `
  --end 2026-10-16 `
  --split-date 2023-01-01
```

## 無前視強度

每個訊號日的歷史分位只和此前 252 筆資料比較，不包含當日及未來資料。預設至少累積 60 筆歷史後才產生分位。

```text
Q1最低20%
Q2偏低
Q3中間
Q4偏高
Q5最高20%
```

這些層級表示該指標在當時歷史分布中的相對位置，不代表預先認定的多空方向。

## 分析指標

- 價量：全價漲跌、相對昨日／五日量能
- 線型：MA5／MA10／MA20 斜率
- 位置：MA5乖離、布林通道位置
- 法人散戶：法人總買超、散戶多空比
- 期貨主力：新建多空、淨多空、活動量、淨多空／OI
- 選擇權：月選、週三、週五及主要短週的 `ln(call/put)`

## 輸出

預設輸出至 `backtest_output/futures_raw_signal_strength_scan/`：

- `raw_signal_strength_detail_*.csv`：逐日、逐指標的原始值、無前視分位與隔日結果
- `raw_signal_strength_by_bucket_*.csv`：五層完整統計
- `raw_signal_strength_tests_*.csv`：Spearman、極端兩層及 Kruskal 檢定
- `raw_signal_strength_candidates_*.csv`：通過探索期、驗證期及年度穩定性的候選
- `raw_signal_strength_yearly_*.csv`：年度五層統計
- `raw_signal_strength_manifest_*.json`：公式、參數及限制

## 注意

- MFE 及下行空間來自日 K，只能確認盤中曾到達，不能判斷高低點先後。
- 顯著性不等於可以直接交易；候選仍需以分鐘 K、手續費、滑價及固定規則回測。
- 第一版刻意不建立綜合分數，以免再次把衝突訊號壓縮掉。
