# 策略多空與隔日當沖績效掃描

## 研究目的與實作

限制掃描結果為隔日日盤開收高低、日內振幅、回落反彈與方向化開收/MFE/MAE；以隔日開盤作統一假設進場，不模擬盤中出場。

## 結果判讀與限制

未扣成本的開收報酬與真實策略績效不同；比較多空分組與年度穩定性。

## 程式來源

- 實作：`code/backtest/futures_strategy_next_day_intraday_scan.py`
- 原有 README：`code/backtest/README_futures_strategy_next_day_intraday_scan.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_strategy_next_day_intraday_scan --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--split-date', type=date.fromisoformat, default=date(2023, 1, 1))`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--minimum-group-size', type=int, default=100)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest/futures_strategy_next_day_intraday_scan'))`

### 輸出檔名線索

- `intraday_directional_performance_{suffix}.csv`
- `intraday_group_differences_{suffix}.csv`
- `intraday_group_shortlist_{suffix}.csv`
- `intraday_scan_manifest_{suffix}.json`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 策略多空與隔日日盤當沖結果掃描

在專案 `code` 目錄執行：

```powershell
python -m backtest.futures_strategy_next_day_intraday_scan --start 2018-01-01 --end 2026-10-19
```

本工具只掃描能直接連結隔日台指期日盤當沖獲利的結果，不掃隔日均線、法人、選擇權、籌碼與其他無法直接代表日盤損益的欄位。

## 掃描結果

- 前日日收／夜收到隔日日盤開、收、高、低的報酬。
- 隔日日盤開到收、高、低的報酬。
- 隔日日盤振幅。
- 高點至收盤回落、低點至收盤反彈。
- 收盤位於當日高低區間的位置。

## 方向化策略檢查

每個策略欄位另依「多訊號做多、空訊號做空」計算：

- 隔日日盤開盤作為統一假設進場基準；前夜收盤只作為盤前比較基準，不冒充可成交進場價。
- 方向化收盤報酬與勝率。
- 最大有利空間（MFE）。
- 最大不利空間（MAE）。

MFE／MAE 只表示盤中曾到達的空間，不代表實際能在高低點成交。所有報酬尚未扣除交易成本與滑價，因此通過統計檢定仍只是後續設計進出規則的候選。

## 輸出

- `intraday_group_differences_*.csv`：所有策略多空群的當沖結果差異。
- `intraday_group_shortlist_*.csv`：通過全期FDR與前後期驗證的結果。
- `intraday_directional_performance_*.csv`：照策略方向操作的報酬、勝率、MFE與MAE。
- `intraday_scan_manifest_*.json`：研究口徑與限制。

