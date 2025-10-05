請幫我改寫 Python 函式來抓 TWSE API 的個股資料（STOCK_DAY 或 STOCK_DAY_AVG），需求如下：

1️⃣ 日期處理：
- 輸入的 datetime 只取月份，將 day 設為 1。
- API 預設會抓整個月資料。
- 資料中日期可能是民國年格式，例如 '114/10/01'，需要轉成西元。

2️⃣ 快取機制：
- 先檢查本地 CSV 是否已存在完整月份檔案 {stock_no}_YYYYMM.csv。
- 若存在，直接讀取 CSV 回傳，不呼叫 API。
- 若不存在，呼叫 API 取得資料。

3️⃣ 檔名規則：
- 過去完整月份：存成 {stock_no}_YYYYMM.csv。
- 本月尚未結束：存成 {stock_no}_YYYYMMDD_lastDate.csv，其中 lastDate 是 API 回傳的最新交易日。
- API 回傳資料最後一列可能不是日期（如「月平均收盤價」），必須排除非日期列。

4️⃣ 舊檔處理：
- 自動刪除多餘、不完整的舊檔，只保留最新完整或最新不完整檔。

5️⃣ 函式行為：
- 回傳 DataFrame。
- 保留 CSV 快取供下次使用。
- 支援 STOCK_DAY 與 STOCK_DAY_AVG 兩種 API。

請依上述規則，產生乾淨、可用、穩定的 Python 函式。
