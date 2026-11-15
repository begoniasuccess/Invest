# 原始指標強度與日夜盤多空方向

## 研究目的與實作

延伸原始訊號掃描，將夜盤及隔日日盤的開收、效率、收盤位置、多空空間差等結果轉為方向化指標，檢查強度越高是否越偏多或偏空。

## 結果判讀與限制

夜盤相對日收與夜盤自身開收分開；方向效率與振幅大小不同，核對驗證期及分位趨勢。

## 程式來源

- 實作：`code/backtest/futures_direction_signal_strength_scan.py`
- 原有 README：`code/backtest/README_futures_direction_signal_strength_scan.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_direction_signal_strength_scan --help
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
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest/futures_direction_signal_strength_scan'))`

### 輸出檔名線索

- `direction_signal_buckets_{suffix}.csv`
- `direction_signal_detail_{suffix}.csv`
- `direction_signal_manifest_{suffix}.json`
- `direction_signal_ranking_{suffix}.csv`
- `direction_signal_tests_{suffix}.csv`
- `direction_signal_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 原始指標強度與隔日多空方向掃描

這支程式把原本用來濃縮成策略多／空的原始連續指標逐項拆開，檢查「指標越強，當日夜盤及隔日日盤是否越偏多或越偏空」。它不使用既有 `策略_*` 結果，也不以振幅變大當作方向有效。

## 納入的指標

- 價量：全價漲跌、相對昨日／五日量能
- 線型：MA5／MA10／MA20 斜率
- 位置：MA5 乖離、布林通道位置
- 法人散戶：法人總買超、純 `散戶多空比_微台`、原合併散戶多空比
- 期貨主力：新建多單、新建空單、淨多空、活動量、淨多空／OI
- 選擇權：月選、週三、週五及主要短週的 `ln(call/put)`

`散戶多空比_微台` 不以小台補值；原合併欄位仍保留作為敏感度比較。

## 預測時段與方向結果

所有結果都統一成「數值越高越偏多」，因此：

- 正相關：指標越高，隔日越偏多。
- 負相關：指標越高，隔日越偏空。

當日夜盤會分開檢查：

- 夜盤收相對日盤收：`(夜盤收－日盤收)/日盤收`，包含夜盤開盤跳空，直接檢查行情是否已在夜盤率先反應。
- 夜盤開收到收：排除相對日盤收的跳空，只看夜盤交易時段本身。
- 夜盤方向效率、收盤位置、多空空間差與上漲機率。

隔日日盤結果包含：

- 隔日開收報酬
- 隔日方向效率：`(收盤－開盤)/(最高－最低)`，避免大振幅自動看起來比較有方向
- 隔日收盤位置
- 隔日多空空間差：上行 MFE－下行 MFE
- 隔日收漲與多方空間較大的機率

## 驗證方式

- 每日強度使用此前最多 252 筆資料的歷史分位，不包含當日與未來。
- 分為 Q1～Q5 五層，檢查是否隨強度單調變化。
- 使用 Spearman 趨勢與 Q5－Q1 Mann-Whitney U 檢定。
- 所有檢定一起做 Benjamini-Hochberg FDR 修正。
- 2018～2022 為探索期，2023 年後為驗證期，另檢查逐年方向一致性。

`重複驗證通過` 必須同時符合：全期 FDR 顯著、探索期 q<0.10、驗證期 p<0.05、前後期同方向，且至少 60% 可比較年度方向一致。

## 執行

PowerShell：

```powershell
python -m backtest.futures_direction_signal_strength_scan `
  --start 2018-01-01 `
  --end 2026-11-02 `
  --split-date 2023-01-01
```

本版只輸出研究 CSV，不寫入資料庫，因此不需要執行 migration。

## 輸出

- `direction_signal_ranking_*.csv`：每個指標分成「當日夜盤」與「隔日日盤」兩列，列出多空方向、強度與穩定證據數，優先看這份。
- `direction_signal_tests_*.csv`：完整相關係數、Q5－Q1、FDR、探索／驗證及年度穩定性。
- `direction_signal_buckets_*.csv`：Q1～Q5 的報酬、上漲率、方向效率及多空空間差。
- `direction_signal_yearly_*.csv`：逐年五層結果。
- `direction_signal_detail_*.csv`：逐日原始值、無前視分位及隔日方向結果。
- `direction_signal_manifest_*.json`：公式、參數與研究限制。

## 判讀

先依 `預測時段` 分開看，再檢查 `ranking` 的 `穩定方向證據數`。夜盤以 `夜盤收相對日盤收_rho` 為主要問題，並與 `盤內開收報酬_rho` 比較：若前者有效、後者無效，效果可能主要來自夜盤開盤跳空；若兩者都有效，才表示夜盤盤中仍持續往相同方向發展。若只在全期顯著、驗證期或逐年不穩，就不能當成可靠多空訊號。

盤中上下空間仍來自日 K，只知道當日曾經到達，無法判斷高低點先後。通過統計檢定後，仍需以分鐘 K、固定停損停利、交易成本及真正樣本外期間回測。

