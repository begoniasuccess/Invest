# 全交易日日盤振幅與路徑型態分類

## 研究目的與實作

以當日日盤高低差相對前30個完整交易日平均振幅分類，結合開盤上下行空間及收盤位置判定強單邊、偏向震盪、先殺後拉等型態，建立全交易日標籤。

## 結果判讀與限制

標籤屬事後結果，不可直接當作盤前特徵；可選資料庫更新，須保留人工標註。

## 程式來源

- 實作：`code/analysis/futures_daily_amplitude_pattern.py`
- 原有 README：`code/analysis/README_futures_daily_amplitude_pattern.md`
- 現有設定：命令列及程式內預設；見下方參數。

## 原始程式執行入口

```powershell
# 在 Invest/code 目錄執行；實際參數請見原始程式
python -m analysis.futures_daily_amplitude_pattern --help
```

### 程式宣告的命令列參數

- `parser.add_argument('--start', type=date.fromisoformat, default=date(2018, 1, 1))`
- `parser.add_argument('--end', type=date.fromisoformat)`
- `parser.add_argument('--futures-id', default='TX')`
- `parser.add_argument('--range-days', type=int, default=30)`
- `parser.add_argument('--minimum-bars', type=int, default=290)`
- `parser.add_argument('--low-range-threshold', type=float, default=0.75)`
- `parser.add_argument('--expanded-range-threshold', type=float, default=1.25)`
- `parser.add_argument('--violent-range-threshold', type=float, default=1.5)`
- `parser.add_argument('--extreme-range-threshold', type=float, default=2.0)`
- `parser.add_argument('--biased-share-threshold', type=float, default=0.65)`
- `parser.add_argument('--strong-share-threshold', type=float, default=0.8)`
- `parser.add_argument('--output-dir', type=Path, default=Path('../output/analysis/futures_daily_amplitude_pattern'))`
- `parser.add_argument('--save-db', action='store_true')`

### 輸出檔名線索

- `daily_amplitude_pattern_{settings.start:%Y%m%d}_{end_text}.csv`

## 結果與重現

歷史結果請切換到「歷史結果」選擇批次；網頁新執行會保存本次參數、程式雜湊、日誌與獨立輸出。舊批次未記錄的資訊不以目前預設值補填。

## 原始研究文件

下方保留原有 README 的研究設計與欄位定義（原文件內的舊路徑或指令僅供參考；實際網頁執行使用本次設定）。

# 全交易日日盤振幅與路徑型態資料表

這個模組不套用核心多空、散戶籌碼或任何既有策略條件。母體是資料庫內全部可用的 TX 日盤交易日，目的是先建立可查詢、可重算、可人工修正的客觀標籤資料集。

## 第一次安裝

先在 PostgreSQL 執行：

```text
db_migrate/20261105_add_futures_daily_amplitude_pattern.sql
```

再從 `code` 目錄執行：

```powershell
python -m analysis.futures_daily_amplitude_pattern `
  --start 2018-01-01 `
  --end 2026-11-05 `
  --save-db
```

不加 `--save-db` 時只會輸出 CSV，方便先檢查結果。重跑相同日期會更新自動計算欄位，但不會清掉 `manual_*` 人工標籤與備註。

## 預設分類

振幅以「當日日盤高低差／前30個完整交易日平均日盤振幅」標準化，而且前30日平均嚴格排除當日：

- `< 0.75R`：低振幅
- `0.75R～<1.25R`：一般振幅
- `1.25R～<1.50R`：明顯放大
- `1.50R～<2.00R`：劇烈振幅
- `>= 2.00R`：極端振幅

路徑型態同時參考相對日盤開盤的上／下行空間占比與收盤位置：

- 強單邊多／強單邊空
- 偏多震盪／偏空震盪
- 先殺後拉／先拉後殺
- 上下雙向震盪

另存 `excursion_bias_label`，只描述最大空間集中在哪一側，不受尾盤收回影響。

## 常用查詢

查劇烈以上且接近理想單邊多、單邊空的日期：

```sql
SELECT trading_date, range_ratio, effective_amplitude_label,
       effective_path_pattern_label, up_share, down_share,
       close_location, extreme_order
FROM research.v_futures_daily_amplitude_pattern
WHERE futures_id = 'TX'
  AND valid_for_research
  AND effective_amplitude_label IN ('劇烈振幅', '極端振幅')
  AND effective_path_pattern_label IN ('強單邊多', '強單邊空')
ORDER BY trading_date;
```

看各型態的樣本數與平均放大倍數：

```sql
SELECT effective_amplitude_label, effective_path_pattern_label,
       COUNT(*) AS sample_count,
       ROUND(AVG(range_ratio)::numeric, 3) AS average_range_ratio
FROM research.v_futures_daily_amplitude_pattern
WHERE futures_id = 'TX' AND valid_for_research
GROUP BY 1, 2
ORDER BY 1, sample_count DESC;
```

人工修正某一天的標籤：

```sql
UPDATE research.futures_daily_amplitude_pattern
SET manual_path_pattern_label = '先殺後拉',
    manual_note = '肉眼檢查後修正',
    manually_reviewed_at = CURRENT_TIMESTAMP
WHERE futures_id = 'TX' AND trading_date = DATE '2026-01-06';
```

查詢時請優先使用 `research.v_futures_daily_amplitude_pattern` 的 `effective_*` 欄位；它會自動優先採用人工覆寫值。

## 可調門檻

命令列可修改 `--range-days`、四段振幅門檻、`--biased-share-threshold` 與 `--strong-share-threshold`。每列都會保存當次規則版本與門檻，方便日後知道標籤是怎麼產生的。

