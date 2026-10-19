const $ = (id) => document.getElementById(id);
const state = { payload: null, boundsMarket: null };
let expandedPanelId = null;
const SETTINGS_KEY = "investDashboard.cycles.v1";

function readSettings() { try { return JSON.parse(localStorage.getItem(SETTINGS_KEY) || "{}"); } catch { return {}; } }
function saveSettings() {
  localStorage.setItem(SETTINGS_KEY, JSON.stringify({
    market: $("market").value, cycleType: $("cycleType").value, periodMode: $("periodMode").value,
    startDate: $("startDate").value, endDate: $("endDate").value,
    startYear: $("startYear").value, endYear: $("endYear").value,
  }));
}
function restoreSettings() {
  const saved = readSettings();
  for (const id of ["market", "cycleType", "periodMode"]) {
    if (saved[id] && [...$(id).options].some(option => option.value === saved[id])) $(id).value = saved[id];
  }
  if (saved.startDate) $("startDate").value = saved.startDate;
  if (saved.endDate) $("endDate").value = saved.endDate;
  return saved;
}

function bootstrapSavedYears(saved) {
  if ($("periodMode").value !== "year" || $("startYear").options.length) return;
  const start = saved.startYear || $("startDate").value.slice(0, 4);
  const end = saved.endYear || $("endDate").value.slice(0, 4);
  for (const [id, year] of [["startYear", start], ["endYear", end]]) {
    if (!/^\d{4}$/.test(year)) continue;
    const option = document.createElement("option");
    option.value = year; option.textContent = `${year} 年`; $(id).appendChild(option); $(id).value = year;
  }
}

const immersivePanels = {
  cycleTablePanel: { button: "cycleTableExpandBtn", kind: "table" },
  directionPanel: { button: "directionExpandBtn", kind: "chart", chart: "directionChart" },
  averagePanel: { button: "averageExpandBtn", kind: "chart", chart: "averageChart" },
};

function resizeCycleChart(panelId, expanded) {
  const config = immersivePanels[panelId];
  if (!config?.chart) return;
  const panel = $(panelId); const chart = $(config.chart);
  if (!chart?.data) return;
  const width = Math.max(320, panel.clientWidth - 2);
  const height = expanded ? Math.max(420, window.innerHeight - 115) : 430;
  Plotly.relayout(chart, { width, height, autosize: false }).then(() => Plotly.Plots.resize(chart));
}

function closeExpandedPanel() {
  if (!expandedPanelId) return;
  const panelId = expandedPanelId; const config = immersivePanels[panelId];
  $(panelId).classList.remove("panel-expanded");
  const button = $(config.button); button.querySelector("i").className = "bi bi-arrows-fullscreen";
  button.title = config.kind === "table" ? "放大表格" : "放大圖表";
  document.body.classList.remove("panel-open"); expandedPanelId = null;
  if (config.kind === "chart") setTimeout(() => resizeCycleChart(panelId, false), 200);
}

function toggleExpandedPanel(panelId) {
  if (expandedPanelId === panelId) { closeExpandedPanel(); return; }
  if (expandedPanelId) closeExpandedPanel();
  const config = immersivePanels[panelId]; $(panelId).classList.add("panel-expanded");
  const button = $(config.button); button.querySelector("i").className = "bi bi-x-lg"; button.title = "關閉沉浸模式";
  document.body.classList.add("panel-open"); expandedPanelId = panelId;
  if (config.kind === "chart") setTimeout(() => resizeCycleChart(panelId, true), 200);
}

function selectedPeriod() {
  if ($("periodMode").value === "year" && $("startYear").value && $("endYear").value) {
    return { start_date: `${$("startYear").value}-01-01`, end_date: `${$("endYear").value}-12-31` };
  }
  return { start_date: $("startDate").value, end_date: $("endDate").value };
}

function queryString() {
  return new URLSearchParams({ market: $("market").value, cycle_type: $("cycleType").value, ...selectedPeriod() }).toString();
}

function fillYears(select, minYear, maxYear, preferred) {
  select.innerHTML = "";
  for (let year = minYear; year <= maxYear; year += 1) {
    const option = document.createElement("option");
    option.value = String(year); option.textContent = `${year} 年`; select.appendChild(option);
  }
  select.value = String(Math.min(maxYear, Math.max(minYear, Number(preferred) || minYear)));
}

function applySourceBounds(payload) {
  const { min_date: minDate, max_date: maxDate } = payload.source;
  for (const id of ["startDate", "endDate"]) { $(id).min = minDate; $(id).max = maxDate; }
  if ($("startDate").value < minDate || $("startDate").value > maxDate) $("startDate").value = minDate;
  if ($("endDate").value > maxDate || $("endDate").value < minDate) $("endDate").value = maxDate;
  if (state.boundsMarket !== payload.market.key || !$("startYear").options.length) {
    const saved = readSettings();
    fillYears($("startYear"), Number(minDate.slice(0, 4)), Number(maxDate.slice(0, 4)), saved.startYear || $("startDate").value.slice(0, 4));
    fillYears($("endYear"), Number(minDate.slice(0, 4)), Number(maxDate.slice(0, 4)), saved.endYear || $("endDate").value.slice(0, 4));
    state.boundsMarket = payload.market.key;
  }
}

function togglePeriodMode() {
  const yearMode = $("periodMode").value === "year";
  $("startDateField").classList.toggle("d-none", yearMode); $("endDateField").classList.toggle("d-none", yearMode);
  $("startYearField").classList.toggle("d-none", !yearMode); $("endYearField").classList.toggle("d-none", !yearMode);
}

function setLoading(show, text = "正在整理週期資料…") { $("loadingText").textContent = text; $("loadingOverlay").classList.toggle("d-none", !show); }
function hideAlert() { $("alertBox").classList.add("d-none"); }
function showAlert(message, type = "success") { const box = $("alertBox"); box.className = `alert alert-${type} app-alert`; box.textContent = message; }
async function requestJson(url, options = {}) { const response = await fetch(url, options); const data = await response.json().catch(() => ({})); if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`); return data; }

function renderSummary(payload) {
  const s = payload.summary;
  const cards = [
    ["有效樣本", Number(s.count).toLocaleString("zh-TW"), "bi-database", "blue"],
    ["上漲次數", Number(s.up_count).toLocaleString("zh-TW"), "bi-arrow-up-right", "green"],
    ["下跌次數", Number(s.down_count).toLocaleString("zh-TW"), "bi-arrow-down-right", "red"],
    ["上漲機率", `${Number(s.up_rate).toFixed(2)}%`, "bi-percent", "cyan"],
    ["平均漲跌", `${Number(s.mean_points).toFixed(2)} 點`, "bi-activity", "violet"],
  ];
  $("cycleSummary").innerHTML = cards.map(([label, value, icon, color]) => `<article class="metric-card metric-${color}"><div class="metric-icon"><i class="bi ${icon}"></i></div><div><div class="metric-label">${label}</div><div class="metric-value">${value}</div></div></article>`).join("");
}

function renderTable(payload) {
  $("tableTitle").textContent = `${payload.market.name}｜${payload.cycle.name}`;
  const table = $("cycleTable");
  table.querySelector("thead").innerHTML = `<tr>${payload.columns.map(column => `<th>${column}</th>`).join("")}</tr>`;
  table.querySelector("tbody").innerHTML = payload.rows.map(row => `<tr>${payload.columns.map(column => `<td>${row[column] ?? ""}</td>`).join("")}</tr>`).join("");
}

const commonLayout = {
  margin: { l: 58, r: 64, t: 42, b: 70 }, paper_bgcolor: "transparent", plot_bgcolor: "transparent",
  font: { family: "Inter, Segoe UI, Microsoft JhengHei, sans-serif", color: "#475569" },
  legend: { orientation: "h", x: 0, y: 1.14 }, xaxis: { gridcolor: "transparent" },
};

function renderCharts(payload) {
  const rows = payload.chart_rows; const x = rows.map(row => row["週期"]);
  $("cycleCharts").classList.toggle("week-mode", payload.cycle.key === "week");
  Plotly.react("directionChart", [
    { type: "bar", name: "上漲次數", x, y: rows.map(r => r["上漲次數"]), marker: { color: "#3b82f6" } },
    { type: "bar", name: "下跌次數", x, y: rows.map(r => r["下跌次數"]), marker: { color: "#ef4444" } },
    { type: "scatter", mode: "lines+markers", name: "上漲機率", x, y: rows.map(r => r["上漲機率"]), yaxis: "y2", line: { color: "#f59e0b", width: 2.5 } },
    { type: "scatter", mode: "lines+markers", name: "下跌機率", x, y: rows.map(r => r["下跌機率"]), yaxis: "y2", line: { color: "#16a34a", width: 2.5 } },
  ], { ...commonLayout, height: 430, barmode: "stack", yaxis: { title: "次數", gridcolor: "#e8edf5", rangemode: "tozero" }, yaxis2: { title: "機率", overlaying: "y", side: "right", ticksuffix: "%", rangemode: "tozero" } }, { responsive: true, displaylogo: false });

  Plotly.react("averageChart", [
    { type: "bar", name: "平均漲跌點數", x, y: rows.map(r => r["平均漲跌點數"]), marker: { color: "#3b82f6" } },
    { type: "scatter", mode: "lines+markers", name: "平均漲跌幅", x, y: rows.map(r => r["平均漲跌幅"]), yaxis: "y2", line: { color: "#f05252", width: 2.5 } },
  ], { ...commonLayout, height: 430, yaxis: { title: "點", gridcolor: "#e8edf5", zerolinecolor: "#94a3b8" }, yaxis2: { title: "%", overlaying: "y", side: "right", ticksuffix: "%", zeroline: false } }, { responsive: true, displaylogo: false });
}

async function loadCycles(showSuccess = false) {
  hideAlert(); setLoading(true);
  try {
    const payload = await requestJson(`/api/cycles?${queryString()}`); state.payload = payload; applySourceBounds(payload);
    $("sourceBadge").innerHTML = `<i class="bi bi-database-check"></i> ${payload.source.min_date} ～ ${payload.source.max_date}`;
    renderSummary(payload); renderTable(payload); renderCharts(payload); saveSettings();
    if (showSuccess) showAlert(`週期統計已重新整理，共 ${Number(payload.summary.count).toLocaleString("zh-TW")} 筆有效樣本。`);
  } catch (error) { showAlert(`讀取失敗：${error.message}`, "danger"); } finally { setLoading(false); }
}

async function updateData() {
  hideAlert(); setLoading(true, "正在更新行情資料…");
  try { await requestJson("/api/update", { method: "POST" }); await saveSettings(); showAlert("行情更新完成，週期統計已重新整理。"); }
  catch (error) { showAlert(`更新失敗：${error.message}`, "danger"); } finally { setLoading(false); }
}

$("refreshBtn").addEventListener("click", () => loadCycles(true)); $("updateBtn").addEventListener("click", updateData);
$("market").addEventListener("change", () => saveSettings()); $("cycleType").addEventListener("change", () => saveSettings());
$("startDate").addEventListener("change", () => saveSettings()); $("endDate").addEventListener("change", () => saveSettings());
$("periodMode").addEventListener("change", () => { togglePeriodMode(); saveSettings(); });
$("startYear").addEventListener("change", () => { if (+$("startYear").value > +$("endYear").value) $("endYear").value = $("startYear").value; saveSettings(); });
$("endYear").addEventListener("change", () => { if (+$("endYear").value < +$("startYear").value) $("startYear").value = $("endYear").value; saveSettings(); });
$("downloadBtn").addEventListener("click", () => { window.location.href = `/api/cycles/export.csv?${queryString()}`; });
$("cycleTableExpandBtn").addEventListener("click", () => toggleExpandedPanel("cycleTablePanel"));
$("directionExpandBtn").addEventListener("click", () => toggleExpandedPanel("directionPanel"));
$("averageExpandBtn").addEventListener("click", () => toggleExpandedPanel("averagePanel"));
document.addEventListener("keydown", event => { if (event.key === "Escape") closeExpandedPanel(); });
window.addEventListener("resize", () => {
  if (expandedPanelId && immersivePanels[expandedPanelId].kind === "chart") resizeCycleChart(expandedPanelId, true);
});
const restoredSettings = restoreSettings(); bootstrapSavedYears(restoredSettings); togglePeriodMode(); saveSettings();
