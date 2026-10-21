# 隔日波動強度基準回測

這支程式把第一階段找到的四個代表性訊號組成 0～4 分。用途是預測隔日台指期日盤是否具有較大的當沖空間，不預測多空方向。

## 計分規則

每個條件成立加 1 分，歷史分位只使用訊號日以前最多 252 筆資料：

- 布林通道位置歷史分位 ≤ 20
- 散戶多空比歷史分位 ≥ 80
- MA10斜率歷史分位 ≤ 20
- 主力淨多空／OI歷史分位 ≤ 20

四個條件必須都有資料才產生分數：

- 0～1分：低波動
- 2分：中波動
- 3～4分：高波動

## 第一次執行

先在 PostgreSQL 執行：

```text
db_migrate/20261020_add_futures_volatility_strength.sql
```

再從專案 `code` 目錄執行：

```powershell
python -m backtest.futures_volatility_strength_backtest `
  --start 2018-01-01 `
  --end 2026-10-16 `
  --split-date 2023-01-01 `
  --save-db
```

PowerShell 多行使用反引號 `` ` ``。不加 `--save-db` 時只輸出 CSV。

## 輸出

預設位於 `backtest_output/futures_volatility_strength/`：

- `volatility_strength_daily_*.csv`：每天四個條件、分數、等級與隔日實際結果
- `volatility_strength_summary_*.csv`：全期、探索期、驗證期的分數及等級統計
- `volatility_strength_yearly_*.csv`：逐年低／中／高波動統計
- `volatility_strength_tests_*.csv`：分數趨勢與高低波動差異檢定
- `volatility_strength_manifest_*.json`：版本、規則與限制

## 研究限制

- 四個條件是在第一階段結果後挑選，因此目前是探索性基準，不是完全未觀察的樣本外模型。
- 分數若能區分振幅，也不代表能預測上漲或下跌。
- 日K的 MFE 無法判斷先到最高點或先到最低點；接到交易策略時仍須使用分鐘K。
