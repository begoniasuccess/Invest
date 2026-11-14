# 資料搬移對照表

全部路徑均以 Invest 為根。僅現存檔案會列入計畫；不同內容不會覆蓋。

| 來源 | 目的地 | 衝突規則 |
|---|---|---|
| `code/Yu點位資料整理_2025_2026.xlsx` | `data/yu/Yu點位資料整理_2025_2026.xlsx` | 保留兩份，人工確認 |
| `code/yu_futures_analysis_2026.xlsx` | `data/yu/yu_futures_analysis_2026.xlsx` | 保留兩份，人工確認 |
| `code/yu_taiex_analysis_2025.csv` | `data/yu/yu_taiex_analysis_2025.csv` | 保留兩份，人工確認 |
| `code/yu_taiex_analysis_2026.csv` | `data/yu/yu_taiex_analysis_2026.csv` | 保留兩份，人工確認 |
| `output/finMind.csv` | `output/finmind/finMind.csv` | 保留既有目的地，來源另封存 |
| `output/finMind_futures_final_settlement_price.csv` | `output/finmind/finMind_futures_final_settlement_price.csv` | 保留既有目的地，來源另封存 |
| `output/finMind_futures_institutional_investors.csv` | `output/finmind/finMind_futures_institutional_investors.csv` | 保留既有目的地，來源另封存 |
| `output/finMind_futures_institutional_investors_after_hours.csv` | `output/finmind/finMind_futures_institutional_investors_after_hours.csv` | 保留既有目的地，來源另封存 |
| `output/finMind_option_final_settlement_price.csv` | `output/finmind/finMind_option_final_settlement_price.csv` | 保留既有目的地，來源另封存 |
| `code/finMind.csv` | `output/finmind/finMind.csv` | 保留既有目的地，來源另封存 |
| `code/finMind_futures_final_settlement_price.csv` | `output/finmind/finMind_futures_final_settlement_price.csv` | 保留既有目的地，來源另封存 |
| `code/finMind_futures_institutional_investors.csv` | `output/finmind/finMind_futures_institutional_investors.csv` | 保留既有目的地，來源另封存 |
| `code/finMind_futures_institutional_investors_after_hours.csv` | `output/finmind/finMind_futures_institutional_investors_after_hours.csv` | 保留既有目的地，來源另封存 |
| `code/finMind_option_final_settlement_price.csv` | `output/finmind/finMind_option_final_settlement_price.csv` | 保留既有目的地，來源另封存 |
| `code/APPLY_REPORT_COLUMNS_20260817.md` | `code/docs/APPLY_REPORT_COLUMNS_20260817.md` | 保留兩份，人工確認 |
| `code/APPLY_TAIEX_REPORT_UPDATE.md` | `code/docs/APPLY_TAIEX_REPORT_UPDATE.md` | 保留兩份，人工確認 |
| `code/taifex_night_fallback.sql` | `code/db_migrate/taifex_night_fallback.sql` | 保留兩份，人工確認 |
| `code/all_python_code.txt` | `archive/code_exports/code/all_python_code.txt` | 保留既有目的地，來源另封存 |
| `all_python_code.txt` | `archive/code_exports/project/all_python_code.txt` | 保留既有目的地，來源另封存 |
| `postgres_schema.sql` | `output/db_exports/legacy_project_postgres_schema.sql` | 保留既有目的地，來源另封存 |
| `output/postgres_schema.sql` | `output/db_exports/legacy_output_postgres_schema.sql` | 保留既有目的地，來源另封存 |
| `code/tests/test_futures_open_to_night_close_convergence.py` | `archive/code_legacy/tests/test_futures_open_to_night_close_convergence.py` | 僅確認與 canonical 測試相同才封存 |
| `code/tests/backtest/futures_raw_signal_strength_scan.py` | `archive/code_legacy/tests/backtest/futures_raw_signal_strength_scan.py` | 保留既有目的地，來源另封存 |
| `code/tests/backtest/README_futures_raw_signal_strength_scan.md` | `archive/code_legacy/tests/backtest/README_futures_raw_signal_strength_scan.md` | 保留既有目的地，來源另封存 |
| `code/tests/backtest/tests/test_futures_raw_signal_strength_scan.py` | `archive/code_legacy/tests/backtest/tests/test_futures_raw_signal_strength_scan.py` | 保留既有目的地，來源另封存 |

根目錄 `futures_open_to_night_close_convergence.py` 保留入口；實作維持 `code/backtest/`。`repair_tx_tick_2019_2020.py` 保留入口，實作放在 `code/main/`。這兩項由 paths 程式補丁完成，不由搬移工具刪除。

所有移動前的檔案另保存到 `archive/organization_backups/<時間>/files/`。同名 FinMind 衝突另保存 `archive/organization_conflicts/`。
