# 2-2 轉折支撐壓力 A～L 當沖

## 研究目的與實作

以昨日價量或五日價量核心判斷方向，搭配昨日日盤、夜盤或合併轉折支撐壓力作為限價；G～L 加入相對夜收距離過濾。依平均振幅比例固定停損停利。

## 結果判讀與限制

同一程式有 v1/v2 設定，須分開保留版本與輸出；比較點位來源、距離過濾和成交率。

## 程式來源

- 實作：`code/backtest/futures_turning_level_intraday.py`
- 原有 README：`code/backtest/README_futures_turning_level_intraday.md`
- 現有設定：`code/backtest/configs/futures_turning_level_intraday_v1.json`, `code/backtest/configs/futures_turning_level_intraday_v2.json`

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_turning_level_intraday --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--config', type=Path, default=Path('backtest/configs/futures_turning_level_intraday_v2.json'))`

### 輸出檔名線索

- `turning_level_detail_{suffix}.csv`
- `turning_level_manifest_{suffix}.json`
- `turning_level_summary_{suffix}.csv`
- `turning_level_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 台指期轉折支撐壓力 A～L 當沖回測

在 `code` 根目錄執行：

```powershell
python -m backtest.futures_turning_level_intraday
```

回測固定從 `2018-01-01` 起算，結束日設為 `2099-12-31`，會自動跑到資料庫目前最後一個交易日。

## 策略對照

| 策略 | 方向欄位 | 多單進場 | 空單進場 | 距離篩選 |
|---|---|---|---|---|
| A | 策略_核心_昨價量 | 昨日日盤支撐 | 昨日日盤壓力 | 無 |
| B | 策略_核心_五日價量 | 昨日日盤支撐 | 昨日日盤壓力 | 無 |
| C | 策略_核心_昨價量 | 昨日夜盤支撐 | 昨日夜盤壓力 | 無 |
| D | 策略_核心_五日價量 | 昨日夜盤支撐 | 昨日夜盤壓力 | 無 |
| E | 策略_核心_昨價量 | 日／夜支撐較低者 | 日／夜壓力較高者 | 無 |
| F | 策略_核心_五日價量 | 日／夜支撐較低者 | 日／夜壓力較高者 | 無 |
| G～L | 分別同 A～F | 分別同 A～F | 分別同 A～F | 有 |

G～L 的條件為 `abs(進場點－昨日夜盤收) >= 30日平均振幅 × 30%`。

30日振幅採前一報表交易日（含當日）往回30個交易日的 `日盤高－日盤低` 平均。成交後用 Tick 依序判斷15%停損、50%停利；兩者皆未觸發，以13:45前最後一筆 Tick 出場。預設每筆來回成本3點、TX每點200元、1口。

報告輸出到 `../output/backtest/futures_turning_level`：

- `turning_level_detail_*.csv`：逐日逐策略完整明細。
- `turning_level_summary_*.csv`：全期間績效及多空分列。
- `turning_level_yearly_*.csv`：逐年績效及多空分列。
- `turning_level_manifest_*.json`：參數與計算口徑。

摘要包含進場次數、訊號成交率、合格掛單成交率、每交易日進場頻率、平均每月進場次數、勝率、總／平均／中位淨點數、淨損益金額、Profit Factor、最大回撤、最大連敗及各種出場筆數。

注意：公開 Tick 無法還原限價單排隊順位，因此採「觸價即成交」，結果仍可能比實盤樂觀。

