# Yu關鍵點位候選產生與人工審核V3

本版只使用指定截止日及以前資料。目標是盡量找齊Yu可能注意的點位，並把三種性質
不同的點位分開，不再全部塞進同一張「前30名」報表。

## 執行指令

```powershell
python -m machine_learning.yu_candidate_review_system `
  --config machine_learning/configs/yu_candidate_review_v3.json `
  --date 2026-09-23 `
  --known-labels "Yu點位資料整理_2025_2026.xlsx"
```

若 Excel 內含「加權點位整理」工作表，程式會直接使用已整理好的 `點位分類`、
`點位類型` 與 `點位` 欄位，不再重新解析舊報告文字。舊版 CSV 仍可繼續使用。

若要同時入庫，在最後加上`--save-db`。V1建表及V2升級SQL若已執行過，不必因V3
再次執行；DBeaver可直接執行原有PostgreSQL語法。

## 最主要的三張表

- `yu_kbar_candidate_review_YYYYMMDD.csv`：最多30組K棒候選，供Yu人工標記。
- `yu_indicator_reference_levels_YYYYMMDD.csv`：均線、布林與均線±2%乖離點，全部列出，
  不占K棒的30個名額。
- `yu_external_reference_levels_YYYYMMDD.csv`：選擇權常用整數點位，全部列出，也不占名額。

報表中的「相近點位組」只是把非常接近的原始點合併，避免Yu重複審核。例如
47501、47506、47512會顯示為最低47501、最高47512、代表點47506；所有原始來源仍保留。

## 其他稽核表

- `yu_candidate_raw_levels_*`：每個尚未合併的原始點位與原因。
- `yu_all_candidate_groups_*`：全部相近點位組，不限30組。
- `yu_candidate_group_sources_*`：每組與原始點位的連結。
- `yu_candidate_missed_levels_*`：Yu補登程式漏掉的點位。
- `yu_candidate_known_coverage_*`：逐日只用當時資料，核對舊Yu點位。
- `yu_static_coverage_at_k_*`：分別列出每天選10、20、30、40、50組時的涵蓋率，
  用來決定30組是否足夠。

## V3修正重點

- Yu原文同時出現高點與MA時，改用每個點自己的子句分類，不再整段一起誤判。
- K棒候選、均線／指標、外部整數點位分開輸出。
- K棒排名降低「同一附近出現很多次」的加分，增加距離與目前有效性的影響。
- K棒前30組盡量平均保留現價上方與下方，避免只剩單邊。
- 輸出欄位改用「相近點位最低值、最高值、代表點位」等直白名稱。

## Yu填寫方式

`is_key_level`與`is_trade_candidate`要分開。重要但當天不拿來交易時：

```text
is_key_level = 1
is_trade_candidate = 0
```

`review_action`使用`KEEP / DROP / MERGE / EDIT / UNCERTAIN`。程式漏掉的點請填入
`yu_candidate_missed_levels_*`，不要硬塞進原候選列。
