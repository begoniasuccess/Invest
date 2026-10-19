# 夜盤上漲後回檔掛多參數回測

## 策略

1. D 日日盤收盤為基準價，D 日夜盤收盤上漲幅為實際 x。
2. T 日日盤在「昨收 × (1 + (實際 x - y)%)」掛限價多單。
3. 成交後，以 D 日夜盤收盤價掛停利。
4. 從成交價下跌 z% 時停損。
5. 兩者都未觸發時，在 13:25 收盤價強制平倉。
6. 13:20 後不再接受新進場。

同一分鐘同時觸發停利與停損時，保守認定先停損，並將
`ambiguous_bar` 標成 `true`。如果限價在某分鐘棒內才成交，該棒高點不會
被拿來當停利，避免誤用成交以前的高點。

## 為什麼這樣設計 x、y、z

- x 同時包含原本 0.5% 分層與 0.25%、0.5%、0.75%、1% 最低門檻。
- y 測 0.1%～1.2%。原分層資料顯示，夜盤上漲組相對夜收的日盤最大回檔
  中位數約 0.38%～0.81%，此範圍涵蓋主要分布與較深回檔。
- z 測 0.2%～1.2%，避免極小停損完全被一分鐘雜訊與交易成本支配。
- 1.5%以上合併為一組，因為原分層的 1.5%～2% 與 2%以上合計僅 58 筆。

## 執行

先用 DBeaver 執行：

`db_migrate/20260913_create_night_retrace_backtest.sql`

再於 code 目錄執行：

```powershell
python -m backtest.futures_night_retrace
```

只輸出 CSV、不寫資料庫：

```powershell
python -m backtest.futures_night_retrace --no-db
```

## 報告

- `night_retrace_parameter_summary`：所有組合，分訓練、驗證、測試、全期間。
- `night_retrace_yearly_summary`：所有組合逐年結果。
- `night_retrace_selection`：把各期間攤平成一列，方便找穩健組合。
- `night_retrace_top_detail`：排名前 20 組逐日交易明細。
- `night_retrace_manifest`：本次設定與資料診斷。

資料切割固定為：2018～2023 訓練、2024～2025 驗證、2026 測試。排名只以
訓練期與驗證期計算，不使用 2026 測試期挑參數。

成本預設為進場滑價 1 點、出場滑價 1 點、手續費與交易稅折算 1 點，皆可在
`backtest/configs/futures_night_retrace_v1.json` 修改。
