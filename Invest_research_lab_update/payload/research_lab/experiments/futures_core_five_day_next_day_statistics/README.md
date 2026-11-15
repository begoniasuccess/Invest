# 五日核心與隔日日盤報酬統計

## 研究目的與實作

以五日核心多空分組，分別用訊號日日盤收與夜盤收作為價格基準，計算隔日日盤高低收報酬、收漲率與組間差異檢定。

## 結果判讀與限制

比較兩種基準，避免將盤前價格基準當作開盤實際成交價；檢查樣本數、效果大小及顯著性。

## 程式來源

- 實作：`code/backtest/futures_core_five_day_next_day_statistics.py`
- 原有 README：`code/backtest/README_futures_core_five_day_next_day_statistics.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m backtest.futures_core_five_day_next_day_statistics --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--alpha', type=float, default=0.05)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/backtest/futures_core_five_day_next_day_statistics'))`

### 輸出檔名線索

- `core_five_day_next_day_detail_{suffix}.csv`
- `core_five_day_next_day_manifest_{suffix}.json`
- `core_five_day_next_day_summary_{suffix}.csv`
- `core_five_day_next_day_tests_{suffix}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 五日核心多空與隔日日盤統計

## 執行

請在專案 `code` 目錄執行：

```powershell
python -m backtest.futures_core_five_day_next_day_statistics --start 2018-01-01 --end 2026-10-19
```

若省略 `--end`，會統計資料庫目前可用的最新訊號日。

## 統計口徑

- 訊號：訊號日的 `策略_核心_五日價量`，只保留「多／空」。
- 隔日：資料庫中下一個實際交易日，不使用曆日加一天；週五會對到下週一。
- 基準一：前一日日盤收。
- 基準二：前一日夜盤收。
- 漲跌幅：`(隔日日盤收 - 基準價) / 基準價 * 100`。
- 上漲：隔日日盤收嚴格大於基準價；平盤不算上漲。
- 最高／最低點漲跌幅：分別以隔日日盤高／低代入相同公式。

## 輸出

- `detail.csv`：逐筆訊號明細，可人工核對日期和公式。
- `summary.csv`：兩種基準 × 多空兩群的四項摘要統計。
- `tests.csv`：多群減空群的差異與顯著性檢定。
- `manifest.json`：本次參數與公式定義。

## 檢定解讀

- Welch t：檢驗兩群平均值是否不同，不假設兩群變異數相同。
- Mann–Whitney U：檢驗兩群整體分布位置是否不同，降低極端值對結論的影響。
- 兩比例 z：檢驗兩群上漲機率是否不同。
- `多減空差異` 大於 0 代表「多」群數值較高，小於 0 代表「空」群較高。
- 同時做多項檢定會增加誤判機率，因此以 `BH校正p值 < 0.05` 作為主要顯著標準。
- 統計顯著不等於具交易價值；仍要同時看差異幅度、樣本數與不同年份是否穩定。

