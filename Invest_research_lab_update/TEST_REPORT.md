# 測試報告｜2026-11-14

基礎資料：本次上傳的 code(20261114-125036).zip、output.zip 與 Invest_structure(2).txt。未改動原回測實作或市場資料。歷史檔案讀取測試使用實際上傳的 output。

## 已實際完成

- 40 個實驗註冊及 41 個 CLI/JSON 預設設定組合：全部建立合法參數及固定命令，輸出指定為獨立目錄（必填起訖日測試使用 2020-01-01）。不代表已執行所有真實策略。
- 43 個歷史批次：222 個檔案的索引、檔案清單/大小/選定摘要雜湊及 43 份回顧均已核對。所有 15 份 Dashboard HTML 模板通過 Jinja2 語法解析。
- `python -m pytest -q tests/test_research_lab.py`：5 項測試通過。包含 CSV 歷史索引/搜尋/排序/分頁、下載與路徑限制、HTML 沙箱、Markdown 編輯與衝突保護、跨來源修改限制、參數驗證、獨立背景工作、禁止並行、取消、結果與回顧保存。獨立背景測試使用專用小型測試程式，不連線 PostgreSQL。
- Chromium：環境限制直接 localhost 導覽，因此以 `set_content` 載入實際 HTML/JS，使用 Playwright 將 fetch 連接至 FastAPI TestClient；驗證研究目錄(40/43)、搜尋及選擇 2-1、結果表格(2 列)、回顧、參數驗證(2組)、Markdown 草稿預覽和放棄修改。無 JS pageerror。未測試真實瀏覽器網路連線、全站 CSS 渲染或全部頁面操作。
- `node --check`：research_lab.js、navigation.js 通過。新增四支 Python 與 app.py 編譯通過。
- 增量安裝工具：以原始 ZIP 中的實際受影響檔案與舊研究草案建立獨立測試目錄；預覽不寫入、安裝、重複執行、程式回退、程式版本衝突攔截、保留使用者修改 Markdown、保留 .env/既有 output/媒體 sentinel，全部通過。

## 尚需本機驗證

- 環境未具備使用者的 PostgreSQL 與 FinMind 相依環境，未啟動實際完整 Dashboard app，也沒有執行 40 支真實多年期研究。請安裝後先檢查 `Dashboard import OK`、頁面及短期間回測。不同研究的資料表、資料量或依賴錯誤會留在日誌中。
- 歷史 PostgreSQL UUID 核對僅依 schema 實作及檢查欄位，未查詢真實資料庫。只接受 manifest 明確的 UUID；無 UUID 不猜測。新執行使用既有 CLI 禁止寫入旗標及 PostgreSQL session read-only options，不是以低權限帳號取代使用者原有帳號。
- 本機 Windows 子程序行為未在 Windows 實測；取消與背景工作測試在 Linux 完成。一次一個工作，不提供多工作排程佇列。進度為真實日誌與狀態，不虛構百分比。
- 大型 CSV 超過 40 MiB 不做全檔排序，仍可分頁、搜尋和下載；回顧是客觀描述性摘要，沒有重新計算所有交易指標或樣本外驗證。
- 歷史實際時間及參數若原始資料沒有保存則維持未知。相同檔名已覆蓋的歷史版本無法復原。歷史快照比對只核對全檔案清單、大小及部分摘要／manifest 的雜湊。
