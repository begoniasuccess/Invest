# 日盤走勢選點與移動停利 2-2-A

## 研究目的與實作

延續日盤走勢選點策略，加入進場價距離前夜收門檻、固定停損、固定停利及浮盈回吐型移動停利，使用日盤 Tick 處理出場先後。

## 結果判讀與限制

與原2-2日盤版比較成交率、回撤及固定／移動停利出場分布；注意參數及樣本期間一致性。

## 程式來源

- 實作：`code/backtest/futures_session_trend_intraday_a.py`
- 原有 README：`code/backtest/README_futures_session_trend_intraday_a.md`
- 現有設定：`code/backtest/configs/futures_session_trend_intraday_a_v1.json`

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_session_trend_intraday_a --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--config', type=Path, default=Path('backtest/configs/futures_session_trend_intraday_a_v1.json'))`

### 輸出檔名線索

- `session_trend_a_by_entry_type_{suffix}.csv`
- `session_trend_a_by_trend_{suffix}.csv`
- `session_trend_a_detail_{suffix}.csv`
- `session_trend_a_manifest_{suffix}.json`
- `session_trend_a_summary_{suffix}.csv`
- `session_trend_a_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 2-2-A.7～2-2-A.10 日盤移動停利回測

## 策略

- `2-2-A.7`：五日核心多，使用昨日日盤走勢選日盤支撐／壓力。
- `2-2-A.8`：五日核心空，使用昨日日盤走勢選日盤支撐／壓力。
- `2-2-A.9`：五日核心多，使用昨日夜盤走勢選夜盤支撐／壓力。
- `2-2-A.10`：五日核心空，使用昨日夜盤走勢選夜盤支撐／壓力。
- 漲勢盤選支撐，跌勢盤選壓力；點位為限價進場價。
- 進場點距離昨日夜盤收盤不足30日平均振幅30%時，不交易。

## 出場

- 停損：進場後反向移動30日平均振幅25%。
- 固定停利：進場後有利移動30日平均振幅50%。
- 移動停利：獲利達30日平均振幅25%後啟動；從持倉後最大浮盈回吐50%時出場。
- 若以上條件均未觸發，使用13:45以前最後一筆Tick出場。
- 每趟預設扣除3點成本。

範例：平均振幅400點、做多進場40,000點。停損為39,900，移動停利在
40,100啟動，固定停利為40,200。若之後最高到40,160，則浮盈160點，回吐
一半的移動停利線為40,080。

## 執行

在`code`目錄執行：

```powershell
python -m backtest.futures_session_trend_intraday_a
```

結果輸出至：

```text
..\output\backtest\futures_session_trend_a
```

請將該目錄產生的六個檔案一起回傳，以便分析總績效、年度穩定性、盤勢分組、
開盤立即成交／盤中觸價，以及固定停利和移動停利的差異。

