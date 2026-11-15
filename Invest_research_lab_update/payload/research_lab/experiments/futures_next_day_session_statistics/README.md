# 隔日日盤盤前條件分層統計

## 研究目的與實作

依五日核心多空、夜盤漲跌四層與夜盤漲跌勢分層，研究隔日日盤相對夜收及開收報酬、高收回落與低收反彈、分位數和年度穩定性，不模擬成交。

## 結果判讀與限制

區分開收方向與夜收基準報酬；反彈回落是事後價格空間，不是可直接獲取的損益。

## 程式來源

- 實作：`code/backtest/futures_next_day_session_statistics.py`
- 原有 README：`code/backtest/README_futures_next_day_session_statistics.md`
- 現有設定：`code/backtest/configs/futures_next_day_session_statistics_v1.json`

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_next_day_session_statistics --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--config', type=Path, default=Path('backtest/configs/futures_next_day_session_statistics_v1.json'))`

### 輸出檔名線索

- `next_day_session_by_core_{suffix}.csv`
- `next_day_session_by_night_change_{suffix}.csv`
- `next_day_session_by_night_trend_{suffix}.csv`
- `next_day_session_detail_{suffix}.csv`
- `next_day_session_full_matrix_{suffix}.csv`
- `next_day_session_manifest_{suffix}.json`
- `next_day_session_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 隔日日盤盤前條件統計

本程式只研究盤前條件與隔日日盤結果，不模擬交易。

## 盤前條件

- `策略_核心_五日價量`：多／空。
- 訊號日夜盤漲跌四層：明顯下跌、小幅下跌、小幅上漲、明顯上漲。
- 訊號日夜盤走勢：漲勢盤／跌勢盤。
- 訊號日日盤走勢保留在逐日明細，第一版不加入主交叉分層。

## 觀察結果

- 隔日日盤收盤相對訊號日夜盤收盤的報酬。
- 隔日日盤開盤至收盤報酬。
- 收漲率、收跌率及核心方向命中率。
- 低收反彈、高收回落的點數、百分比與30日平均振幅比。
- 平均數、中位數、5%截尾平均數及P10／P25／P50／P75／P90。
- 各年度結果及有利年度比例。

所有名稱帶 `_pct` 的欄位已乘以100。例如`0.35`代表`0.35%`。

## 執行

在`code`目錄執行：

```powershell
python -m backtest.futures_next_day_session_statistics
```

輸出目錄：

```text
..\output\backtest\futures_next_day_session_statistics
```

完成後請將該目錄的七個檔案一起壓縮回傳。

