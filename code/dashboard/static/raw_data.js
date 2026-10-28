const $ = (id) => document.getElementById(id);
const SETTINGS_KEY = "investDashboard.rawData.v1";
const COLUMN_LAYOUT_VERSIONS = { taiex: "2026-10-23-turnover-after-lower-band" };
const state = { payload: null, offset: 0, allColumns: [], visibleColumns: [], sortColumn: "", sortDirection: "desc", searchColumn: "", searchText: "", pollTimer: null, expanded: false, boundsReport: null, columnsReport: null, tableReport: null, requestId: 0 };

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>'"]/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" }[char]));
}

function readSettings() {
  try { const saved = JSON.parse(localStorage.getItem(SETTINGS_KEY) || "{}"); return saved && typeof saved === "object" && !Array.isArray(saved) ? saved : {}; } catch { return {}; }
}

function saveSettings() {
  const saved = readSettings();
  saved.report = $("reportType").value;
  saved.startDate = $("startDate").value;
  saved.endDate = $("endDate").value;
  saved.periodMode = $("periodMode").value;
  saved.startYear = $("startYear").value;
  saved.endYear = $("endYear").value;
  saved.pageSize = $("pageSize").value;
  saved.reportUpdateModules = [
    $("updateTaiexReport").checked ? "taiex_report" : "",
    $("updateFuturesReport").checked ? "futures_report" : "",
  ].filter(Boolean);
  saved.columns = saved.columns || {};
  saved.columnSchemas = saved.columnSchemas || {};
  saved.columnLayoutVersions = saved.columnLayoutVersions || {};
  // 切換報表時 allColumns 會先清空；此時不可用空陣列覆蓋另一張報表
  // 已保存的欄位配置。只有資料已載入後才寫入欄位設定。
  if (state.allColumns.length && state.columnsReport) {
    saved.columns[state.columnsReport] = [...state.visibleColumns];
    saved.columnSchemas[state.columnsReport] = [...state.allColumns];
    if (COLUMN_LAYOUT_VERSIONS[state.columnsReport]) saved.columnLayoutVersions[state.columnsReport] = COLUMN_LAYOUT_VERSIONS[state.columnsReport];
  }
  saved.tables ||= {};
  saved.tables[state.tableReport || saved.report] = {
    sortColumn: state.sortColumn,
    sortDirection: state.sortDirection,
    searchColumn: state.searchColumn,
    searchText: state.searchText,
  };
  localStorage.setItem(SETTINGS_KEY, JSON.stringify(saved));
}

function restoreTableSettings(report) {
  state.tableReport = report;
  const table = readSettings().tables?.[report] || {};
  state.sortColumn = table.sortColumn || "";
  state.sortDirection = table.sortDirection === "asc" ? "asc" : "desc";
  state.searchColumn = table.searchColumn || "";
  state.searchText = table.searchText || "";
  $("tableSearch").value = state.searchText;
}

function restoreSettings() {
  const saved = readSettings();
  if (saved.report && [...$("reportType").options].some(option => option.value === saved.report)) $("reportType").value = saved.report;
  if (saved.startDate) $("startDate").value = saved.startDate;
  if (saved.endDate) $("endDate").value = saved.endDate;
  if (saved.periodMode && [...$("periodMode").options].some(option => option.value === saved.periodMode)) $("periodMode").value = saved.periodMode;
  if (saved.pageSize && [...$("pageSize").options].some(option => option.value === saved.pageSize)) $("pageSize").value = saved.pageSize;
  if (Array.isArray(saved.reportUpdateModules)) {
    $("updateTaiexReport").checked = saved.reportUpdateModules.includes("taiex_report");
    $("updateFuturesReport").checked = saved.reportUpdateModules.includes("futures_report");
  }
  return saved;
}

function fillYears(select, minYear, maxYear, preferred) {
  select.innerHTML = "";
  for (let year = minYear; year <= maxYear; year += 1) {
    const option = document.createElement("option"); option.value = String(year); option.textContent = `${year} 年`; select.appendChild(option);
  }
  select.value = String(Math.min(maxYear, Math.max(minYear, Number(preferred) || minYear)));
}

function bootstrapSavedYears(saved) {
  if ($("periodMode").value !== "year" || $("startYear").options.length) return;
  const start = saved.startYear || $("startDate").value.slice(0, 4);
  const end = saved.endYear || $("endDate").value.slice(0, 4);
  for (const [id, year] of [["startYear", start], ["endYear", end]]) {
    if (!/^\d{4}$/.test(year)) continue;
    const option = document.createElement("option"); option.value = year; option.textContent = `${year} 年`; $(id).appendChild(option); $(id).value = year;
  }
}

function togglePeriodMode() {
  const yearMode = $("periodMode").value === "year";
  $("startDateField").classList.toggle("d-none", yearMode); $("endDateField").classList.toggle("d-none", yearMode);
  $("startYearField").classList.toggle("d-none", !yearMode); $("endYearField").classList.toggle("d-none", !yearMode);
}

function selectedPeriod() {
  if ($("periodMode").value === "year" && $("startYear").value && $("endYear").value) {
    return { start_date: `${$("startYear").value}-01-01`, end_date: `${$("endYear").value}-12-31` };
  }
  return { start_date: $("startDate").value, end_date: $("endDate").value };
}

function applySourceBounds(payload) {
  const { min_date: minDate, max_date: maxDate } = payload.report;
  for (const id of ["startDate", "endDate"]) { $(id).min = minDate; $(id).max = maxDate; }
  if (!$("startDate").value || $("startDate").value < minDate || $("startDate").value > maxDate) $("startDate").value = minDate;
  if (!$("endDate").value || $("endDate").value > maxDate || $("endDate").value < minDate) $("endDate").value = maxDate;
  if (state.boundsReport !== payload.report.key || !$("startYear").options.length) {
    const saved = readSettings(); const minYear = Number(minDate.slice(0, 4)); const maxYear = Number(maxDate.slice(0, 4));
    fillYears($("startYear"), minYear, maxYear, saved.startYear || $("startDate").value.slice(0, 4));
    fillYears($("endYear"), minYear, maxYear, saved.endYear || $("endDate").value.slice(0, 4));
    state.boundsReport = payload.report.key;
  }
}

function showAlert(message, type = "success") { const box = $("alertBox"); box.className = `alert alert-${type} app-alert`; box.textContent = message; }
function hideAlert() { $("alertBox").classList.add("d-none"); }
function setLoading(show, text = "正在讀取報表…") { $("loadingText").textContent = text; $("loadingOverlay").classList.toggle("d-none", !show); }
async function requestJson(url, options = {}) { const response = await fetch(url, options); const data = await response.json().catch(() => ({})); if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`); return data; }

function queryParams(includeOffset = true) {
  const params = new URLSearchParams({ report: $("reportType").value, limit: $("pageSize").value });
  const period = selectedPeriod();
  if (period.start_date) params.set("start_date", period.start_date);
  if (period.end_date) params.set("end_date", period.end_date);
  if (state.sortColumn) params.set("sort_column", state.sortColumn);
  params.set("sort_direction", state.sortDirection);
  if (state.searchText && state.searchColumn) {
    params.set("search_column", state.searchColumn);
    params.set("search_text", state.searchText);
  }
  if (includeOffset) params.set("offset", String(state.offset));
  return params;
}

function normalizeCell(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}T00:00:00/.test(value)) return value.slice(0, 10);
  return value;
}

function restoreVisibleColumns(columns, report) {
  state.columnsReport = report;
  const settings = readSettings();
  const saved = settings.columns?.[report];
  const previousSchema = settings.columnSchemas?.[report];
  // 有保存過（包含使用者明確「全部取消」的空陣列）就尊重保存值；
  // 只有從未保存過這張報表時，才預設顯示全部欄位。
  state.visibleColumns = Array.isArray(saved)
    ? [...new Set(saved.filter(column => columns.includes(column)))]
    : [...columns];
  if (Array.isArray(saved) && saved.length && !state.visibleColumns.length) state.visibleColumns = [...columns];
  // 欄位位置規則改版時，只校正一次既有快取的順序；顯示／隱藏選擇仍保留。
  if (Array.isArray(saved) && COLUMN_LAYOUT_VERSIONS[report]
      && settings.columnLayoutVersions?.[report] !== COLUMN_LAYOUT_VERSIONS[report]) {
    const visible = new Set(state.visibleColumns);
    state.visibleColumns = columns.filter(column => visible.has(column));
  }
  // 舊設定並不知道新加入的欄位；只在 schema 第一次變更時補進顯示清單。
  // 保存新 schema 後，使用者再次手動隱藏欄位仍會被尊重。
  const newlyAdded = Array.isArray(previousSchema)
    ? columns.filter(column => !previousSchema.includes(column))
    : columns.filter(column => column === "總成交金額_億" && !state.visibleColumns.includes(column));
  for (const column of newlyAdded) {
    if (state.visibleColumns.includes(column)) continue;
    const anchor = column === "總成交金額_億" ? state.visibleColumns.indexOf("布林下軌") : -1;
    if (anchor >= 0) state.visibleColumns.splice(anchor + 1, 0, column);
    else state.visibleColumns.push(column);
  }
}

function renderTable() {
  const payload = state.payload; const columns = state.visibleColumns;
  const table = $("rawTable");
  table.querySelector("thead").innerHTML = `<tr>${columns.map(column => {
    const active = column === state.sortColumn;
    const icon = active ? (state.sortDirection === "asc" ? "bi-sort-up" : "bi-sort-down") : "bi-arrow-down-up";
    return `<th class="raw-sortable" aria-sort="${active ? (state.sortDirection === "asc" ? "ascending" : "descending") : "none"}"><button type="button" class="raw-sort-button ${active ? "active" : ""}" data-column="${escapeHtml(column)}" title="依 ${escapeHtml(column)} 排序"><span>${escapeHtml(column)}</span><i class="bi ${icon}"></i></button></th>`;
  }).join("")}</tr>`;
  table.querySelector("tbody").innerHTML = payload.rows.map(row => `<tr>${columns.map(column => `<td>${escapeHtml(normalizeCell(row[column]))}</td>`).join("")}</tr>`).join("");
  $("tableTitle").textContent = payload.report.name;
  const first = payload.report.total ? payload.report.offset + 1 : 0;
  const last = Math.min(payload.report.offset + payload.rows.length, payload.report.total);
  $("rowSummary").textContent = `共 ${payload.report.total.toLocaleString("zh-TW")} 筆，目前顯示第 ${first}～${last} 筆・${columns.length}/${state.allColumns.length} 欄`;
  const page = Math.floor(payload.report.offset / payload.report.limit) + 1;
  const pages = Math.max(1, Math.ceil(payload.report.total / payload.report.limit));
  $("pageLabel").textContent = `第 ${page} / ${pages} 頁`;
  $("prevBtn").disabled = payload.report.offset <= 0;
  $("nextBtn").disabled = payload.report.offset + payload.report.limit >= payload.report.total;
}

function renderSearchColumns() {
  const select = $("searchColumn");
  const preferred = state.allColumns.includes(state.searchColumn) ? state.searchColumn : (state.allColumns.includes("日期") ? "日期" : state.allColumns[0] || "");
  select.innerHTML = state.allColumns.map(column => `<option value="${escapeHtml(column)}">${escapeHtml(column)}</option>`).join("");
  select.value = preferred;
  state.searchColumn = preferred;
  $("clearSearchBtn").disabled = !state.searchText;
}

function renderColumnList() {
  const keyword = $("columnSearch").value.trim().toLowerCase();
  $("columnList").innerHTML = state.allColumns.map(column => {
    const checked = state.visibleColumns.includes(column);
    const hidden = keyword && !column.toLowerCase().includes(keyword);
    const order = state.visibleColumns.indexOf(column);
    return `<div class="column-option ${hidden ? "d-none" : ""}" data-column="${escapeHtml(column)}">
      <label><input class="form-check-input column-check" type="checkbox" ${checked ? "checked" : ""}> <span>${escapeHtml(column)}</span></label>
      <span class="column-order-actions ${checked ? "" : "invisible"}"><button class="column-up" title="往前"><i class="bi bi-arrow-up"></i></button><button class="column-down" title="往後"><i class="bi bi-arrow-down"></i></button><small>${order >= 0 ? order + 1 : ""}</small></span>
    </div>`;
  }).join("");
}

function updateColumns(mutator) {
  mutator(); saveSettings(); renderColumnList(); renderTable();
}

async function loadRawData(resetPage = false, successMessage = "") {
  const report = $("reportType").value;
  const requestId = ++state.requestId;
  if (state.tableReport !== report) restoreTableSettings(report);
  if (resetPage) state.offset = 0;
  hideAlert(); setLoading(true);
  try {
    const requestedPeriod = selectedPeriod();
    const payload = await requestJson(`/api/raw-data?${queryParams()}`);
    if (requestId !== state.requestId || report !== $("reportType").value) return;
    const reportChanged = state.columnsReport !== payload.report.key;
    state.payload = payload;
    const changedReport = state.allColumns.join("\u0000") !== payload.columns.join("\u0000");
    state.allColumns = payload.columns;
    if (changedReport || reportChanged) restoreVisibleColumns(payload.columns, payload.report.key);
    state.sortColumn = payload.report.sort_column;
    state.sortDirection = payload.report.sort_direction;
    state.searchColumn = payload.report.search_column || state.searchColumn;
    state.searchText = payload.report.search_text || "";
    $("tableSearch").value = state.searchText;
    renderSearchColumns();
    applySourceBounds(payload);
    const boundedPeriod = selectedPeriod();
    if (requestedPeriod.start_date !== boundedPeriod.start_date || requestedPeriod.end_date !== boundedPeriod.end_date) {
      saveSettings(); setLoading(false); return loadRawData(true, successMessage);
    }
    saveSettings(); renderColumnList(); renderTable();
    if (successMessage) showAlert(successMessage);
  } catch (error) { if (requestId === state.requestId) showAlert(`讀取失敗：${error.message}`, "danger"); }
  finally { if (requestId === state.requestId) setLoading(false); }
}

async function copyVisibleTable() {
  if (!state.payload || !state.visibleColumns.length) return showAlert("目前沒有可複製的表格內容。", "danger");
  const clean = value => String(normalizeCell(value) ?? "").replace(/[\t\r\n]+/g, " ");
  const lines = [state.visibleColumns.map(clean).join("\t")];
  state.payload.rows.forEach(row => lines.push(state.visibleColumns.map(column => clean(row[column])).join("\t")));
  const content = lines.join("\n");
  try {
    if (navigator.clipboard && window.isSecureContext) await navigator.clipboard.writeText(content);
    else {
      const area = document.createElement("textarea"); area.value = content; area.style.position = "fixed"; area.style.opacity = "0";
      document.body.appendChild(area); area.focus(); area.select();
      if (!document.execCommand("copy")) throw new Error("瀏覽器拒絕複製");
      area.remove();
    }
    showAlert(`已複製目前頁面的 ${state.payload.rows.length} 筆、${state.visibleColumns.length} 欄資料。`);
  } catch (error) { showAlert(`複製失敗：${error.message}`, "danger"); }
}

function toggleExpanded() {
  state.expanded = !state.expanded;
  $("rawTablePanel").classList.toggle("panel-expanded", state.expanded);
  document.body.classList.toggle("panel-open", state.expanded);
  const icon = $("tableExpandBtn").querySelector("i"); icon.className = state.expanded ? "bi bi-x-lg" : "bi bi-arrows-fullscreen";
}

async function pollUpdateStatus() {
  try {
    const status = await requestJson("/api/report-update/status");
    const statusIcon = status.running
      ? "bi-arrow-repeat spin"
      : status.step === "failed"
        ? "bi-exclamation-triangle"
        : status.step === "pending"
          ? "bi-info-circle"
          : "bi-database-check";
    $("updateStatus").innerHTML = `<i class="bi ${statusIcon}"></i> ${escapeHtml(status.message)}`;
    $("updateReportBtn").disabled = status.running;
    $("reportUpdateMenu").classList.toggle("update-menu-disabled", status.running);
    $("updateTaiexReport").disabled = status.running;
    $("updateFuturesReport").disabled = status.running;
    if (status.running) {
      if (!state.pollTimer) state.pollTimer = setInterval(pollUpdateStatus, 1500);
      return;
    }
    clearInterval(state.pollTimer); state.pollTimer = null;
    if (status.step === "completed") await loadRawData(true, `${status.message}，畫面已重新載入。`);
    if (status.step === "pending") {
      await loadRawData(false);
      showAlert(status.message, "info");
    }
    if (status.step === "failed") showAlert(status.message, "danger");
  } catch (error) { clearInterval(state.pollTimer); state.pollTimer = null; $("updateReportBtn").disabled = false; $("reportUpdateMenu").classList.remove("update-menu-disabled"); $("updateTaiexReport").disabled = false; $("updateFuturesReport").disabled = false; showAlert(`無法取得更新狀態：${error.message}`, "danger"); }
}

async function startUpdate() {
  const modules = [
    $("updateTaiexReport").checked ? "taiex_report" : "",
    $("updateFuturesReport").checked ? "futures_report" : "",
  ].filter(Boolean);
  if (!modules.length) { $("reportUpdateMenu").open = true; return showAlert("請至少選擇一個報表更新模組。", "danger"); }
  hideAlert(); $("updateReportBtn").disabled = true; $("reportUpdateMenu").open = false; $("reportUpdateMenu").classList.add("update-menu-disabled"); saveSettings();
  try {
    const status = await requestJson("/api/report-update", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ modules }),
    });
    $("updateStatus").textContent = status.message;
    if (!state.pollTimer) state.pollTimer = setInterval(pollUpdateStatus, 1500);
    await pollUpdateStatus();
  } catch (error) { $("updateReportBtn").disabled = false; $("reportUpdateMenu").classList.remove("update-menu-disabled"); showAlert(`啟動更新失敗：${error.message}`, "danger"); }
}

$("reportType").addEventListener("change", saveSettings);
for (const id of ["startDate", "endDate", "pageSize"]) $(id).addEventListener("change", () => { saveSettings(); });
$("periodMode").addEventListener("change", () => { togglePeriodMode(); saveSettings(); });
$("startYear").addEventListener("change", () => { if (+$("startYear").value > +$("endYear").value) $("endYear").value = $("startYear").value; saveSettings(); });
$("endYear").addEventListener("change", () => { if (+$("endYear").value < +$("startYear").value) $("startYear").value = $("endYear").value; saveSettings(); });
$("refreshBtn").addEventListener("click", () => loadRawData(true, "報表已重新整理。"));
$("prevBtn").addEventListener("click", () => { state.offset = Math.max(0, state.offset - Number($("pageSize").value)); loadRawData(false); });
$("nextBtn").addEventListener("click", () => { state.offset += Number($("pageSize").value); loadRawData(false); });
$("rawTable").querySelector("thead").addEventListener("click", event => {
  const button = event.target.closest(".raw-sort-button"); if (!button) return;
  const column = button.dataset.column;
  state.sortDirection = state.sortColumn === column && state.sortDirection === "asc" ? "desc" : "asc";
  state.sortColumn = column; saveSettings(); loadRawData(true);
});
$("searchColumn").addEventListener("change", () => { state.searchColumn = $("searchColumn").value; saveSettings(); });
function applyTableSearch() { state.searchColumn = $("searchColumn").value; state.searchText = $("tableSearch").value.trim(); saveSettings(); loadRawData(true); }
$("searchBtn").addEventListener("click", applyTableSearch);
$("tableSearch").addEventListener("keydown", event => { if (event.key === "Enter") { event.preventDefault(); applyTableSearch(); } });
$("clearSearchBtn").addEventListener("click", () => { $("tableSearch").value = ""; state.searchText = ""; saveSettings(); loadRawData(true); });
$("columnSearch").addEventListener("input", renderColumnList);
$("columnList").addEventListener("click", event => {
  const option = event.target.closest(".column-option"); if (!option) return;
  const column = option.dataset.column;
  if (event.target.closest(".column-check")) {
    updateColumns(() => { state.visibleColumns = event.target.checked ? [...state.visibleColumns, column] : state.visibleColumns.filter(item => item !== column); });
  } else if (event.target.closest(".column-up") || event.target.closest(".column-down")) {
    updateColumns(() => { const index = state.visibleColumns.indexOf(column); const delta = event.target.closest(".column-up") ? -1 : 1; const next = index + delta; if (index >= 0 && next >= 0 && next < state.visibleColumns.length) [state.visibleColumns[index], state.visibleColumns[next]] = [state.visibleColumns[next], state.visibleColumns[index]]; });
  }
});
$("selectAllBtn").addEventListener("click", () => updateColumns(() => { state.visibleColumns = [...state.allColumns]; }));
$("clearAllBtn").addEventListener("click", () => updateColumns(() => { state.visibleColumns = []; }));
$("resetColumnsBtn").addEventListener("click", () => updateColumns(() => { state.visibleColumns = [...state.allColumns]; }));
$("downloadBtn").addEventListener("click", () => {
  if (!state.visibleColumns.length) { showAlert("請至少選擇一個要下載的欄位。", "danger"); return; }
  const params = queryParams(false); params.delete("limit"); params.set("columns", state.visibleColumns.join("|"));
  window.location.href = `/api/raw-data/export.csv?${params}`;
});
$("copyTableBtn").addEventListener("click", copyVisibleTable);
$("tableExpandBtn").addEventListener("click", toggleExpanded);
$("updateTaiexReport").addEventListener("change", saveSettings);
$("updateFuturesReport").addEventListener("change", saveSettings);
$("updateReportBtn").addEventListener("click", startUpdate);
document.addEventListener("keydown", event => { if (event.key === "Escape" && state.expanded) toggleExpanded(); });

const restoredSettings = restoreSettings(); restoreTableSettings($("reportType").value); bootstrapSavedYears(restoredSettings); togglePeriodMode(); loadRawData(true);
