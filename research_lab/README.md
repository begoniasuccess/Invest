# Invest 回測實驗

正式版研究資料：40 支研究程式（29 backtest 含 1 支圖庫工具、11 analysis）、43 組歷史結果、222 個可瀏覽檔案。來源是 2026-11-14 上傳的 code 與 output ZIP；不複製原 CSV、不修改原始策略。

- `catalog.json`：固定註冊的實驗、參數結構、原始程式 SHA-256、歷史輸出對應規則。
- `experiments/<id>/README.md`：實驗說明，可在網頁編輯。`metadata.json` 保存中文名稱、分類、狀態、標籤、收藏等人工設定。
- `reviews/<id>/<run_id>.md`：每一執行批次的獨立 Markdown 回顧；本次提供 43 份歷史回顧。
- `revisions/`：文件儲存前的舊版本。
- `history_snapshot.json`：原始輸出清單、大小與選定摘要／manifest SHA-256；原檔日後被覆蓋時提示重新核對。不是所有逐筆明細的全檔驗證。
- `review_template.md`：人工研究回顧模板。

歷史結果直接從 `../output/backtest`、`../output/analysis` 掃描。歷史執行時間以 manifest 為準；沒有則為未知，檔案修改時間只供參考。檔名日期通常是指定資料期間，不是執行日。歷史同名結果被覆蓋前的版本無法找回。

新回測輸出：`../output/research_runs/<experiment_id>/<run_id>/`，含 `config.json`、`manifest.json`、`execution.log`、`source_snapshot/`、`source_hashes.json`、`artifacts/`。回顧在 research_lab/reviews。網頁執行預設唯讀資料庫，固定入口且一次一個工作；不能執行任意 shell 指令。程式若修改，執行器會因來源雜湊不符停用；歷史查閱不受影響。新增研究需明確註冊 source_path/module/參數/輸出旗標/歷史路徑，檢查其讀寫行為後更新 catalog 及雜湊；不能直接上傳程式執行。

設定檔 v1/v2 只是參數預設；現行 Python 才是實際演算法，選擇舊設定不會回復舊 Python。歷史結果不會被重新執行或更改。自動摘要是描述性統計，不是策略採用判定。

備份時保留 research_lab、output 及 PostgreSQL；原 journal_media 不受影響。本頁不需新的資料庫資料表。
