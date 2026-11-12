# 台股關鍵點位整理 Skill

這是一個可移植的 Agent Skill，核心指令為 `SKILL.md`，精確計算腳本為 `scripts/format_levels.py`。

## 使用
將此資料夾安裝到支援 Agent Skills 的工具中，或將 `SKILL.md` 作為自訂指令。對話貼入原始 TAIEX（含選權）或 TX（日盤/夜盤/全日）資料即可。

## 計算腳本

```bash
python scripts/format_levels.py --input examples/taiex.json
python scripts/format_levels.py --input examples/tx.json
```

腳本使用 JSON 格式的 `close` 與 `records`；由 AI 按 `SKILL.md` 從原始貼文擷取價格與標籤，再由腳本計算、排序、合併。腳本本身不解析自然語言，也不自動推斷選權或轉折標籤。

## 驗證
```bash
python -m unittest discover -s tests -v
```
