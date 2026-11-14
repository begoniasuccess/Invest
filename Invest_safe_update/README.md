# Invest 安全增量更新（2026-11-14）

這不是完整 code 資料夾。**不要覆蓋、改名或刪除整個 code。** 本包根據 Invest_structure.txt、all_python_code(9).txt，以及能取得的上一份完整 ZIP 的前端檔案建立。Python 來源以此次提供的原始碼為準。未取得的本機前端修改由版本檢查攔截，不會強行覆蓋。

## 0. 準備

關閉 Dashboard 和正在寫入資料的程式，先備份 PostgreSQL（包括 paper_trade）、既有 code、data、output、journal_media。保留原本 .env、Python 環境、啟動腳本與捷徑。解壓本 ZIP 到 Invest 根目錄，使 `Invest_safe_update` 和 `code` 同層。以下指令在 PowerShell 逐行執行；任一步驟出現錯誤就停止，不要繼續下階段。不要把包含密碼的 .env 貼到聊天。

```powershell
Set-Location "C:\Users\USER\Desktop\Han\學習\金融策略\Invest"
```

## 1. 只安裝交易功能修正（features）

```powershell
python .\Invest_safe_update\apply_update.py features
python .\Invest_safe_update\apply_update.py features --apply
```

第一條僅檢查版本；第二條先備份全部待覆蓋程式，再逐檔安裝。任何 CONFLICT 會停止整個階段，保留本機原檔。若遇到衝突，提供檔案路徑及該檔案（排除秘密）。正常會印出 `BACKUP` 目錄，記下路徑。

```powershell
Push-Location .\code
python -c "from dashboard.app import app; print('Dashboard import OK')"
```

如果匯入成功，才執行以下資料庫 migration（需要 PostgreSQL 連線、schema 更新權限，缺套件時可能需要網路）：

```powershell
.\setup_paper_trade.bat
Pop-Location
```

確認安裝成功後，以原本的 start_dashboard.bat 或桌面捷徑啟動 Dashboard，Ctrl+F5。執行唯讀檢查：

```powershell
python .\Invest_safe_update\verify_dashboard.py
```

預設 http://127.0.0.1:8050；不同連接埠可加 `--url http://127.0.0.1:你的連接埠`。檢查首頁、回放、績效、日記、K 線圖、Yu 點位頁及相關 API。實際測試使用新的測試帳號／場次：四種觸價組合、跳價成交同 Tick 停損、部分平倉日記關聯、重新同步、日記刪除衝突、圖片上傳與媒體管理。不必為了整理路徑先關閉或刪除舊交易資料。

## 2. 確認第一階段正常後，才整理讀寫路徑（paths）

重新關閉 Dashboard 與更新程式：

```powershell
python .\Invest_safe_update\apply_update.py paths
python .\Invest_safe_update\apply_update.py paths --apply
Push-Location .\code
python -c "from dashboard.app import app; print('Dashboard import OK')"
Pop-Location
```

先檢查 Dashboard／Yu 資料能讀取（可以先啟動，驗證完再次關閉）。路徑修正支援尚在 code 根目錄的 Yu 輸入，整理前不會強制要求移動。確認沒問題後預覽和搬移：

```powershell
python .\code\tools\organize_project.py
python .\code\tools\organize_project.py --apply
```

先看清 PLAN 來源／目的地。嚴格衝突 KEEP 表示來源保留，需自行確認。FinMind 同名不同內容會保留既有 output 版本並封存另一份。每個實際搬移檔案都先建立可驗證的備份及目的地，才移除原位置。不覆蓋不同內容的既有檔案。記下 `Restore from:` 備份路徑。搬移完成後再啟動 Dashboard、執行唯讀檢查、實際查詢 Yu 點位分析和回測輸出。第二階段不需要額外資料庫 migration。

## 3. 回退

關閉所有寫入程式。若已搬移資料，先用目前仍存在的 organize_project.py 恢復原位置，再回退 paths 程式：

```powershell
python .\code\tools\organize_project.py --restore "<實際印出的 organization_backups 子目錄>"
python .\Invest_safe_update\apply_update.py --rollback "<實際印出的 update_backups 下 paths 備份目錄>"
```

請用實際路徑取代角括號的提示。restore 不刪新位置的檔案；若舊位置已被其他程式重新寫入，工具拒絕覆蓋。apply_update 回退只恢復該階段的程式，也會先檢查有無額外修改。

features 的程式可以用同一 --rollback 指令指定 features 備份。但 PostgreSQL migration **不會隨程式回退**；若已建立新版 STOP 委託或開倉日記連結，舊版可能無法理解其含義。回退前應先備份現有資料庫、確認相容性，必要時依你原本備份流程還原資料庫；不要為了回退而直接 DROP 新欄位。不要不加確認地還原舊資料庫而遺失新交易。

## 保留與限制

- 不動 .env、start_dashboard.*、install_dashboard.bat、桌面捷徑、ai_skill、deprecated、journal_media、既有報表／回測輸出目錄及不明根目錄檔案（含 data_center.db-journal、tmp.json）。不清理任意目錄，不執行歷史 migration SQL。
- 根目錄兩支研究／修復程式保留相容入口；原有策略算法未變更。大部分原來已可用的相對輸出路徑不做大規模改寫，仍以原工作目錄（code）執行舊研究命令。
- 確認實際資料移動清單見 FILE_MOVES.md。程式變更逐檔列在 features/manifest.json 與 paths/manifest.json。
- 新功能沒有在你的 PostgreSQL 上執行；本機資料庫結構可能仍為上次升級後版本。setup_paper_trade.bat 必須實際執行成功。真實交易、FinMind 網路抓取、影片完整解碼均需本機驗證。
