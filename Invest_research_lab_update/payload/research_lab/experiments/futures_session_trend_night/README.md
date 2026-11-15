# 夜盤走勢選點 2-2.11～2-2.14

## 研究目的與實作

五日核心多空搭配當日日盤或前一已完成夜盤走勢，選轉折支撐壓力做限價進場；夜盤預設17:00後交易，固定停損停利，未觸發至次日05:00前平倉。

## 結果判讀與限制

比較日盤來源與夜盤來源點位、方向及年度；夜盤資料歸屬須與訊號可知時間一致。

## 程式來源

- 實作：`code/backtest/futures_session_trend_night.py`
- 原有 README：`code/backtest/README_futures_session_trend_night.md`
- 現有設定：`code/backtest/configs/futures_session_trend_night_v1.json`

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_session_trend_night --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--config', type=Path, default=Path('backtest/configs/futures_session_trend_night_v1.json'))`

### 輸出檔名線索

- `night_session_trend_by_entry_type_{suffix}.csv`
- `night_session_trend_by_trend_{suffix}.csv`
- `night_session_trend_detail_{suffix}.csv`
- `night_session_trend_manifest_{suffix}.json`
- `night_session_trend_summary_{suffix}.csv`
- `night_session_trend_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 2-2.11～2-2.14 夜盤走勢選點回測

本回測交易台指期夜盤，預設從17:00開始，避免在當日核心策略與法人資料尚未完成前進場。

- 2-2.11：五日核心多，依當日日盤走勢選日盤支撐或壓力。
- 2-2.12：五日核心空，依當日日盤走勢選日盤支撐或壓力。
- 2-2.13：五日核心多，依當日05:00前已完成的上一夜盤走勢選夜盤支撐或壓力。
- 2-2.14：五日核心空，依當日05:00前已完成的上一夜盤走勢選夜盤支撐或壓力。

漲勢盤使用支撐，跌勢盤使用壓力。多單在價格小於等於掛單價時成交，空單在價格大於等於掛單價時成交。17:00第一筆Tick已符合條件時使用較優價格，否則等待後續觸價。

停損為近30個交易日日盤平均振幅的15%，停利為50%；未觸發者使用次日05:00以前最後一筆Tick平倉。預設來回成本3點。

## 執行

在 `code` 根目錄執行：

```powershell
python -m backtest.futures_session_trend_night
```

輸出位於：

```text
..\output\backtest\futures_session_trend_night
```

