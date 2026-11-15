# Invest 回測實驗｜增量更新

基礎版本：code(20261114-125036).zip。歷史研究：output.zip。保留現有程式結構、.env、啟動腳本、所有市場資料及舊輸出；無 PostgreSQL migration，不安裝新套件（沿用既有 Markdown 套件）。

## 安裝（Windows PowerShell）

1. 關閉 Dashboard 與研究程序。備份 code、research_lab（若有）及 output；不要刪除原始 CSV。
2. 解壓縮 ZIP 至 Invest 根目錄，應有 `Invest/Invest_research_lab_update/install.py`。
3. 在 Invest 根目錄執行：

```powershell
python .\Invest_research_lab_update\install.py
python .\Invest_research_lab_update\install.py --apply
```

第一行只預覽；有 CONFLICT 就停止，不要強行覆蓋。安裝器檢查程式基礎雜湊，既有的人工編輯 Markdown 會保留，catalog 若與舊草案及新版本都不同會攔截。備份會存入 `archive/update_backups/research_lab_...`。

4. 在 `Invest/code` 執行 `python -c "from dashboard.app import app; print('Dashboard import OK')"`。用原來的方式啟動 Dashboard，Ctrl+F5。
5. 策略模擬 → 回測實驗（或 `/research-lab`）。先查看 2-1 的歷史結果、回顧及參數驗證，再用短日期試跑。不需執行 setup_paper_trade.bat。

## 功能

40 個實驗／工具、43 歷史批次、222 可瀏覽檔案、40 方法說明、43 回顧。搜尋、分類、收藏、狀態、Markdown 編輯及版本保護。結果 CSV 分頁／全欄搜尋／排序（40 MiB 以內）／選欄／複製／下載；JSON 顯示、HTML 沙箱圖庫。歷史輸出直接從原本 output 讀取；未記錄的參數與時間不推測。歷史 DB UUID 核對需資料庫連線，無 UUID 不猜測匹配。新回測使用固定程式及參數、獨立輸出、完整日誌與參數快照、完成後客觀摘要、自動保存、取消與重跑新批次。一次只允許一個工作，第二個請求會被拒絕，不是多工作佇列。真實資料庫回測需本機驗證。

## 回退

關閉 Dashboard 並確定沒有回測正在執行。從 Invest 根目錄執行：

```powershell
python .\Invest_research_lab_update\install.py --rollback "完整備份目錄路徑"
```

回退僅處理這次新增或更新的 code 檔案，保留 research_lab 文件、output 及任何人工新增研究結果。若後續修改過程式，工具拒絕覆蓋。資料庫不變。

請不要直接把 payload 整包覆蓋。安裝包保留在 Invest 根目錄或 archive，不放進 code。
