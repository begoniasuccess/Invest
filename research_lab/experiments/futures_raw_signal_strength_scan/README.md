# 原始指標強度與隔日報表結果

## 研究目的與實作

不使用既有策略多空標籤，對價量、線型、位置、法人散戶、期貨主力及選擇權等連續指標做歷史強度分層，分析隔日日盤結果與驗證期。

## 結果判讀與限制

區分方向與振幅目標，核對歷史分位視窗、FDR、多重比較與年度一致性；可選研究入庫。

## 程式來源

- 實作：`code/backtest/futures_raw_signal_strength_scan.py`
- 原有 README：`code/backtest/README_futures_raw_signal_strength_scan.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_raw_signal_strength_scan --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--split-date', type=date.fromisoformat, default=date(2023, 1, 1))`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--history-window', type=int, default=252)`
- `parser.add_argument('--minimum-history', type=int, default=60)`
- `parser.add_argument('--minimum-group-size', type=int, default=30)`
- `parser.add_argument('--minimum-year-size', type=int, default=20)`
- `parser.add_argument('--alpha', type=float, default=0.05)`
- `parser.add_argument('--winsor-tail', type=float, default=0.005)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest/futures_raw_signal_strength_scan'))`
- `parser.add_argument('--save-db', action='store_true', help='同時將本次明細與統計結果寫入 research schema（需先執行 migration）')`

### 輸出檔名線索

- `raw_signal_strength_by_bucket_{suffix}.csv`
- `raw_signal_strength_candidates_{suffix}.csv`
- `raw_signal_strength_detail_{suffix}.csv`
- `raw_signal_strength_manifest_{suffix}.json`
- `raw_signal_strength_tests_{suffix}.csv`
- `raw_signal_strength_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

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

## 保存研究結果至 PostgreSQL

第一次使用前，先在資料庫工具中執行：

```text
db_migrate/20261020_add_raw_signal_strength_research.sql
```

之後在原指令加上 `--save-db`：

```powershell
python -m backtest.futures_raw_signal_strength_scan `
  --start 2018-01-01 `
  --end 2026-10-16 `
  --split-date 2023-01-01 `
  --save-db
```

每次執行會建立新的 `run_id`，不覆蓋舊研究。資料分別保存在：

- `research.analysis_run`：參數、期間、版本與執行狀態
- `research.raw_signal_strength_daily`：逐日原始值、歷史分位及隔日結果
- `research.raw_signal_strength_bucket`：全期間五分位統計
- `research.raw_signal_strength_test`：顯著性、探索／驗證期及年度穩定性
- `research.raw_signal_strength_yearly`：逐年五分位統計

沒有加 `--save-db` 時仍只輸出 CSV，不會寫入資料庫。

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

預設輸出至 `../output/backtest/futures_raw_signal_strength_scan/`：

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

