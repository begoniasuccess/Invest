# 交易日記：列表／閱讀／編輯分離、標籤、日期排序

基礎版本：code(20261115-133018).zip。這是增量更新，不是完整 code。

## 安裝
1. 關閉 Dashboard；備份原有程式及 PostgreSQL。
2. 將 ZIP 解壓到 Invest/code，覆蓋同名檔案。
3. 在 Invest/code 執行 `python setup_journal_tags.py`。只新增 paper_trade.journals.tags（text[]，非空、預設空陣列），可重複執行；不安裝／升級 Python 套件，不執行其他 migration。
4. 確認 `Journal tags schema ready. Existing journals preserved.` 後，執行 `python -c "from dashboard.app import app; print('Dashboard import OK')"`。
5. 啟動 Dashboard，Ctrl+F5，打開交易日記。

## 功能
- 日記列表僅顯示標題及日期，點標題進入唯讀 Markdown 閱讀畫面；點編輯進入獨立編輯畫面。三個畫面互斥，不另開瀏覽器分頁；關閉編輯返回原閱讀畫面或列表。儲存後返回閱讀畫面。
- 可新增、重複引用、移除標籤（每篇 30 個，每個 50 字；英文大小寫去重）；既有日記預設無標籤，原內容、交易連結、媒體不受影響。
- 標籤下拉選單顯示已用標籤及日記數；可與既有關鍵字、帳號、場次、日期條件合用；切換排序／標籤後重新查詢第一頁。預設新到舊，同日以 ID 做穩定排序。
- 從交易／場次／媒體引用等既有入口開啟日記時，先進入閱讀畫面。從閱讀畫面點標籤可回列表篩選該標籤。
- 保留既有 Markdown、安全預覽、媒體庫、交易關聯、日記更新／刪除版本保護。

## 檢查
- 59 項交易／日記／媒體及標籤 Python 測試通過；另 5 項回放服務測試通過。
- 回放引擎與回放儲存 JS 測試通過；JS 語法及 Jinja2 模板檢查通過。
- Chromium 使用實際 HTML／JS 搭配模擬 API，驗證列表精簡、閱讀／編輯切換、標籤新增／去重／篩選、正序／倒序、儲存／新增與直接日記連結。瀏覽器環境限制 HTTP 導覽，測試使用內嵌頁面，非 Windows localhost 或實際 PostgreSQL。
- 尚未執行真實 PostgreSQL migration；請依上述安裝步驟進行。

## 回退
若需退版，停止 Dashboard，恢復此次覆蓋的原始程式。資料庫新增 tags 欄位可留著，舊版程式不會用到它；不必刪欄位。如果已建立標籤，請保留欄位以免遺失標籤資料。
