# 日盤走勢選點 2-2.7～2-2.10

## 研究目的與實作

以五日核心決定多空，依已完成日盤或夜盤走勢選擇轉折支撐或壓力作限價，日盤開盤立即成交或盤中觸價後，以近30日均幅比例停損停利。

## 結果判讀與限制

依策略、走勢、開盤立即與盤中觸價分組，檢查是否因盤勢篩選改善績效。

## 程式來源

- 實作：`code/backtest/futures_session_trend_intraday.py`
- 原有 README：`code/backtest/README_futures_session_trend_intraday.md`
- 現有設定：`code/backtest/configs/futures_session_trend_intraday_a_v1.json`, `code/backtest/configs/futures_session_trend_intraday_v1.json`

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_session_trend_intraday --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--config', type=Path, default=Path('backtest/configs/futures_session_trend_intraday_v1.json'))`

### 輸出檔名線索

- `session_trend_by_entry_type_{suffix}.csv`
- `session_trend_by_trend_{suffix}.csv`
- `session_trend_detail_{suffix}.csv`
- `session_trend_manifest_{suffix}.json`
- `session_trend_summary_{suffix}.csv`
- `session_trend_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 2-2.7～2-2.10 日盤走勢選點回測

本回測只交易台指期日盤，使用 `策略_核心_五日價量` 決定多空，使用日盤或夜盤走勢決定限價點位：

- 漲勢盤：使用該盤別的轉折支撐。
- 跌勢盤：使用該盤別的轉折壓力。
- 2-2.7：核心多、日盤走勢與日盤點位。
- 2-2.8：核心空、日盤走勢與日盤點位。
- 2-2.9：核心多、夜盤走勢與夜盤點位。
- 2-2.10：核心空、夜盤走勢與夜盤點位。

多單在價格小於等於掛單價時成交，空單在價格大於等於掛單價時成交。若日盤第一筆 Tick 已符合條件，以較優的第一筆 Tick 價成交，並標記為 `OPENING_IMMEDIATE`；否則標記為 `INTRADAY_TOUCH`。

停損為近30個交易日日盤平均振幅的15%，停利為50%；未觸發者以13:45以前最後一筆 Tick 平倉。預設來回成本3點。

## 執行

在 `code` 根目錄執行：

```powershell
python -m backtest.futures_session_trend_intraday
```

輸出位於：

```text
..\output\backtest\futures_session_trend
```

請回傳該資料夾內同一次執行產生的六個檔案，以便分析整體、年度、盤勢分組及開盤／盤中成交績效。

