# 級距統計 Dashboard

此 Dashboard 使用 FastAPI、Jinja2、Bootstrap 與 Plotly.js，以 PostgreSQL 中的
加權指數與台指期日盤、夜盤、全日 OHLC，計算七項區間統計、年度交叉表、週期統計與互動圖表，
並可在「原始資料」頁面瀏覽及匯出完整報表。

「K 線分析」頁面使用 Lightweight Charts™，支援加權指數、台指期日盤、夜盤、全日的
日 K／週 K／月 K、成交量、自訂均線及水平點位。圖表設定會依市場與週期分別保存在瀏覽器。

## 第一次使用

1. 確認專案根目錄的 `.env` 已設定 `POSTGRES_DSN`。
2. 雙擊 `install_dashboard.bat` 安裝所需套件。
3. 雙擊 `start_dashboard.bat`。
4. 瀏覽器會自動開啟 `http://127.0.0.1:8050`。

## 建立桌面啟動圖示

1. 在 `code` 資料夾內雙擊 `create_desktop_shortcut.bat`（只需執行一次）。
2. 桌面會出現 **Invest Dashboard** 捷徑。
3. 之後雙擊桌面捷徑即可啟動 Dashboard 並自動開啟瀏覽器。

若 Dashboard 已在執行，再次雙擊捷徑只會開啟網頁，不會重複啟動服務。

## 日常使用

- **重新整理統計**：只重新讀取 PostgreSQL，不呼叫外部行情來源。
- **更新最新資料**：更新 TAIEX 與 TX 行情、同步近期台指期資料；台指期全日會依 `v_futures_report_daily` 即時計算，完成後重新統計。
- **更新全部報表**（原始資料頁）：依序執行與 `python -m main.stock_report_taiex`、`python -m main.futures` 相同的完整流程；畫面會顯示目前步驟，完成後自動載入最新報表。
- 可切換市場、指標與日期區間；表格可下載成 CSV，圖表右上角可下載成 PNG。
- 各頁籤的市場、指標、區間等選項會保存在瀏覽器；原始資料的顯示欄位與欄位順序也會依報表分別保存。

Dashboard 僅監聽本機 `127.0.0.1`，不對外網開放。關閉命令視窗即可停止服務。

本版已由 Dash 改為 FastAPI 網站架構。若由舊版升級，請覆蓋檔案後重新執行一次
`install_dashboard.bat`，以安裝 FastAPI、Uvicorn 與 Jinja2。

FastAPI 自動 API 文件位於：`http://127.0.0.1:8050/api/docs`。

## 級距邊界

所有區間採「左含、右不含」：

- `0~25` 表示 `0 <= x < 25`。
- 25 歸入 `25~50`。
- 500 歸入 `>=500`。
- `-25~0` 不包含 0；0 歸入 `0~25`。

## 常見問題

- 若顯示資料庫連線失敗，確認 PostgreSQL 服務已啟動，並檢查 `.env` 的 `POSTGRES_DSN`。
- 若「更新最新資料」失敗但原有報表能開啟，通常是行情 API 或網路暫時不可用；可稍後重試。
- 預設連接埠是 8050；如被占用，可設定環境變數 `DASHBOARD_PORT`。
