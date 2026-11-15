# 核心、夜盤、位置與籌碼複合訊號

## 研究目的與實作

以訊號日已完成的日夜盤資訊建立核心方向、夜盤回檔反轉、價格位置、散戶法人及均線等多組訊號；隔日日盤開盤進場、收盤出場，預設扣來回成本。

## 結果判讀與限制

比較單一與複合訊號的增益、樣本縮減、年度表現；尚未包含盤中停損停利及限價進場。

## 程式來源

- 實作：`code/backtest/futures_combination_signal_backtest.py`
- 原有 README：`code/backtest/README_futures_combination_signal_backtest.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_combination_signal_backtest --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat, default=None)`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--core-column', default='策略_核心_五日價量')`
- `parser.add_argument('--cost-points', type=float, default=3.0)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest'))`

### 輸出檔名線索

- `futures_combination_detail_{suffix}.csv`
- `futures_combination_manifest_{suffix}.json`
- `futures_combination_summary_{suffix}.csv`
- `futures_combination_yearly_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 核心、夜盤、位置與籌碼組合回測

這支程式是獨立研究模組，不修改資料庫、不刷新正式 View，也不掛進 Dashboard 日常更新。

## 回測對齊

- 訊號日：使用當日日盤與當日夜盤完成後已知的資料。
- 交易日：下一個資料庫交易日，不以曆日 `+1` 推算。
- 交易方式：隔日日盤開盤進場、日盤收盤出場。
- 預設成本：每筆來回 3 點，可用 `--cost-points` 修改。
- 歷史百分位：只使用當日前 252 筆，不把當日或未來資料放進門檻。
- 本版本比較的是組合方向強度，尚未加入盤中限價、停損與停利。

## 研究家族

| 代碼 | 組合 |
|---|---|
| A | 核心方向＋夜盤反向回檔／反彈 |
| B | 核心方向＋夜盤極端漲跌，測試反轉 |
| C | A＋夜盤收盤是否脫離最低／最高四分位 |
| D | 核心方向＋高低檔位置 |
| E | 核心方向＋綜合散戶／純微台散戶極端值 |
| F | 核心方向＋法人與散戶籌碼共振 |
| G | 核心方向＋MA20方向＋位置過濾 |
| H | 核心方向＋主力新建多單極端值＋位置 |
| I | 核心方向＋夜盤走勢＋收盤守住支撐／壓力 |
| J | 四個保留組合＋排除方向性追價（多單排除正乖離Q80、空單排除負乖離Q20） |
| K | 四個保留組合＋反方向極端乖離（多單負乖離Q20、空單正乖離Q80） |
| L | 四個保留組合＋MA5乖離Q20～Q80非極端區 |
| M | 夜盤極端漲跌＋同方向極端MA5乖離後測試反轉 |

MA5乖離使用 View 的 `全5MA乖離率`。所有Q20／Q80門檻都由當日前252筆資料動態計算，不把當日放進自己的歷史門檻，也不使用固定2%跨年代比較。

不同家族是不同研究假設，同一天可以同時出現在數個家族，不能把各家族損益直接相加當成一套策略。

## 執行

```powershell
python -m backtest.futures_combination_signal_backtest --start 2018-01-01 --end 2026-11-02
```

若要改測昨價量核心：

```powershell
python -m backtest.futures_combination_signal_backtest --start 2018-01-01 --end 2026-11-02 --core-column 策略_核心_昨價量
```

## 輸出

- `futures_combination_detail_*`：每個組合的逐日訊號與隔日結果。
- `futures_combination_summary_*`：總樣本、命中率、平均點數、PF、MDD、FDR校正及年度穩定度。
- `futures_combination_yearly_*`：逐年度結果。
- `futures_combination_manifest_*`：本次執行設定與防偷看說明。

先看 `summary`，再針對有價值的組合查 `detail`。`正式通過=True` 同時要求足夠樣本、命中率與平均損益通過 FDR 0.05，避免從大量組合中誤挑偶然結果。

