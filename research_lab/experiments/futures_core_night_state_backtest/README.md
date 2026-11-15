# 核心方向 × 夜盤走勢與偏離程度

## 研究目的與實作

以核心多空、已完成夜盤漲跌勢與夜盤位移相對近均幅的分組建立訊號，隔日日盤首筆 Tick 進場，依均幅比例固定停損停利，否則尾盤平倉；提供單一核心與夜盤基準。

## 結果判讀與限制

比較各策略、分桶、年度及基準；核對缺 Tick 排除、實際成交口徑與樣本數。

## 程式來源

- 實作：`code/backtest/futures_core_night_state_backtest.py`
- 原有 README：`code/backtest/README_futures_core_night_state_backtest.md`
- 現有設定：`code/backtest/configs/futures_core_night_state_v1.json`

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_core_night_state_backtest --help
```

### 程式宣告的命令列參數

- `ap.add_argument('--config', type=Path, default=Path(__file__).parent / 'configs' / 'futures_core_night_state_v1.json')`

### 輸出檔名線索

- `{prefix}_{ending}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 核心 × 夜盤走勢 × 夜盤偏離程度：第一輪基準回測

## 安裝位置

將 ZIP 裡的三個檔案放進現有專案對應位置（不要覆寫原有回測程式）：

- `code/backtest/futures_core_night_state_backtest.py`
- `code/backtest/configs/futures_core_night_state_v1.json`
- `code/backtest/tests/test_futures_core_night_state_backtest.py`（測試）

本程式沿用現有 `common.db`、`futures_stop_trailing.load_day_ticks_bulk` 與 PostgreSQL 結構，不改資料庫。

## 執行

從 **`code` 資料夾**執行：

```powershell
python -m backtest.futures_core_night_state_backtest --config backtest/configs/futures_core_night_state_v1.json
```

第一次可先縮小 config 的日期確認流程，正式研究再改回 2018-01-01～2026-10-28。

## 策略定義

- 「核心」使用**訊號日** `策略_核心_五日價量`，可改 `策略_核心_昨價量`。
- 「夜盤走勢」使用訊號日 `夜盤走勢`，依現有報表極值先後決定漲勢／跌勢。
- 「夜盤偏離比例」預設 `abs(夜盤收 - 夜盤開) / 訊號日可得的近30個交易日日盤平均(高-低)`，並分 `0～25%`、`25～50%`、`50～75%`、`75～100%`、`100～125%`、`>=125%`。
- `night_move_direction` 是夜盤收開實際漲跌方向，**與夜盤走勢不相同**；兩者保留以供檢查。
- 另可設定 `amplitude_mode=NIGHT_CLOSE_MINUS_PREVIOUS_DAY_CLOSE`，但兩版結果不能當成相同策略。
- 對核心多／空 × 夜盤漲／跌**各跑開盤做多和開盤做空**（8 組），而不只是預設哪個方向有效。關心的四種情境可按策略名稱篩選，極端逆核心先觀察高偏離分桶。
- 在下一日 `08:45` 日盤首筆 Tick 成交進場；SL=30日平均振幅25%，TP=50%，兩者未觸發則當日日盤結束前最後一筆 Tick 離場。
- 同一時間序列 Tick 先碰到哪個價位先執行，停損採觸發時成交價（含跳價），停利採目標價。預設單次固定成本0點，請用 `cost_points` 做成本與滑價敏感度測試。
- `gap_limit_points=null` 代表不篩跳空；如要參考2-1可設定500，規則是「朝交易方向跳超過500點才略過」。
- 交易日期與訊號日期使用現有修正報表的相鄰交易日映射，開盤交易價格取 Tick，不會用下一日資訊決定訊號。
- 實際契約月份依既有程式邏輯從開盤1分鐘K取得**當時最早契約月份**，Tick 只取相同合約，禁止混契約。

## 輸出（與 code 同層 output/backtest）

- `*_signals.csv`：全部候選訊號，兩個方向（檢查篩選基數）。
- `*_skipped.csv`：缺資料／訊號失效等排除原因。
- `*_detail.csv`：每筆交易逐筆結果，包含無Tick的情況。
- `*_summary.csv`：8組總績效。
- `*_bucket_summary.csv`：每種市場狀態 × 偏離區間。
- `*_year_summary.csv`：各情境逐年。
- `*_year_bucket_summary.csv`：各情境 × 偏離區間逐年。
- `*_direction_bucket_summary.csv`：再切分「夜收實際高於／低於夜開」。
- `*_core_baseline.csv`：核心方向 × 交易方向的對照。
- `*_night_baseline.csv`：夜盤趨勢 × 交易方向的對照。

`MISSING_TICKS` 不納入勝率／績效統計，但會保留於明細，避免被當作0損益。兩個方向同一天都測屬於**研究用反事實對照**，不能直接將8組損益相加當作單一實際交易組合。

## 研究注意事項

1. 這是第一輪「市場狀態＋開盤進」基準測試，**不是人工關鍵點位策略**，不含DIF、限價掛單或移動停利。
2. 報表若夜盤跨日歸屬設定與這個 `訊號日 -> 下一交易日` 映射不同，須先核對數個交易日再使用結果。
3. 分桶很多，可能有資料探勘偏誤；請保留尚未用來挑參數的近期資料供樣本外／前向驗證。
4. 固定點數成本不能完整模擬流動性、委託種類與真實滑價；若Tick稀疏，強制收盤可能提前。輸出列明實際出場時間可供稽核。
5. 在此對話環境沒有連上你的PostgreSQL，所以**只驗證程式結構與合成測試**，未聲稱已跑出真實2018～2026績效。

## 測試

```powershell
python -m pytest -q backtest/tests/test_futures_core_night_state_backtest.py
```

