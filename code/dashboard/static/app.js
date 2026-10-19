const $ = (id) => document.getElementById(id);
const state = { payload: null, boundsMarket: null };
let expandedPanelId = null;
const SETTINGS_KEY = "investDashboard.distribution.v1";

function readSettings() { try { return JSON.parse(localStorage.getItem(SETTINGS_KEY) || "{}"); } catch { return {}; } }
function saveSettings() {
  localStorage.setItem(SETTINGS_KEY, JSON.stringify({
    market: $("market").value, indicator: $("indicator").value, binWidth: $("binWidth").value,
    periodMode: $("periodMode").value, startDate: $("startDate").value, endDate: $("endDate").value,
    startYear: $("startYear").value, endYear: $("endYear").value,
  }));
}
function restoreSettings() {
  const saved = readSettings();
  for (const id of ["market", "indicator", "periodMode"]) {
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

function indicatorDefaults() {
  const percent = $("indicator").value.endsWith("_pct");
  return { width: percent ? 0.5 : 50, unit: percent ? "%" : "點" };
}

function applyIndicatorDefaults() {
  const defaults = indicatorDefaults();
  $("binWidth").value = String(defaults.width);
  $("binWidth").min = defaults.unit === "%" ? "0.01" : "1";
  $("binWidth").step = defaults.unit === "%" ? "0.1" : "1";
  $("binUnit").textContent = defaults.unit;
}

function resizeDistributionChart(expanded = expandedPanelId === "chartPanel") {
  const chart = $("distributionChart");
  const panel = $("chartPanel");
  if (!chart || !panel || !chart.data) return;

  const width = Math.max(320, panel.clientWidth - 2);
  const height = expanded ? Math.max(420, window.innerHeight - 115) : 510;
  Plotly.relayout(chart, { width, height, autosize: false })
    .then(() => Plotly.Plots.resize(chart));
}

function closeExpandedPanel() {
  if (!expandedPanelId) return;
  const panelId = expandedPanelId;
  const panel = $(panelId);
  const button = $(panelId === "chartPanel" ? "chartExpandBtn" : "tableExpandBtn");
  panel.classList.remove("panel-expanded");
  button.querySelector("i").className = "bi bi-arrows-fullscreen";
  button.title = panelId === "chartPanel" ? "放大圖表" : "放大表格";
  document.body.classList.remove("panel-open");
  expandedPanelId = null;
  if (panelId === "chartPanel") setTimeout(() => resizeDistributionChart(false), 200);
}

function toggleExpandedPanel(panelId) {
  if (expandedPanelId === panelId) {
    closeExpandedPanel();
    return;
  }
  if (expandedPanelId) closeExpandedPanel();
  const panel = $(panelId);
  const button = $(panelId === "chartPanel" ? "chartExpandBtn" : "tableExpandBtn");
  panel.classList.add("panel-expanded");
  button.querySelector("i").className = "bi bi-x-lg";
  button.title = "關閉放大模式";
  document.body.classList.add("panel-open");
  expandedPanelId = panelId;
  if (panelId === "chartPanel") setTimeout(() => resizeDistributionChart(true), 200);
}

function selectedPeriod() {
  if ($("periodMode").value === "year" && $("startYear").value && $("endYear").value) {
    return {
      start_date: `${$("startYear").value}-01-01`,
      end_date: `${$("endYear").value}-12-31`,
    };
  }
  return { start_date: $("startDate").value, end_date: $("endDate").value };
}

function queryString() {
  const period = selectedPeriod();
  return new URLSearchParams({
    market: $("market").value,
    indicator: $("indicator").value,
    start_date: period.start_date,
    end_date: period.end_date,
    bin_width: $("binWidth").value,
  }).toString();
}

function fillYears(select, minYear, maxYear, preferred) {
  select.innerHTML = "";
  for (let year = minYear; year <= maxYear; year += 1) {
    const option = document.createElement("option");
    option.value = String(year);
    option.textContent = `${year} 年`;
    select.appendChild(option);
  }
  const safe = Math.min(maxYear, Math.max(minYear, Number(preferred) || minYear));
  select.value = String(safe);
}

function applySourceBounds(payload) {
  const minDate = payload.source.min_date;
  const maxDate = payload.source.max_date;
  const minYear = Number(minDate.slice(0, 4));
  const maxYear = Number(maxDate.slice(0, 4));

  $("startDate").min = minDate;
  $("startDate").max = maxDate;
  $("endDate").min = minDate;
  $("endDate").max = maxDate;
  if ($("startDate").value < minDate) $("startDate").value = minDate;
  if ($("startDate").value > maxDate) $("startDate").value = maxDate;
  if ($("endDate").value > maxDate) $("endDate").value = maxDate;
  if ($("endDate").value < minDate) $("endDate").value = minDate;

  if (state.boundsMarket !== payload.market.key || !$("startYear").options.length) {
    const saved = readSettings();
    const preferredStart = saved.startYear || $("startYear").value || $("startDate").value.slice(0, 4);
    const preferredEnd = saved.endYear || $("endYear").value || $("endDate").value.slice(0, 4);
    fillYears($("startYear"), minYear, maxYear, preferredStart);
    fillYears($("endYear"), minYear, maxYear, preferredEnd);
    state.boundsMarket = payload.market.key;
  }
}

function togglePeriodMode() {
  const yearMode = $("periodMode").value === "year";
  $("startDateField").classList.toggle("d-none", yearMode);
  $("endDateField").classList.toggle("d-none", yearMode);
  $("startYearField").classList.toggle("d-none", !yearMode);
  $("endYearField").classList.toggle("d-none", !yearMode);
}

function setLoading(show, text = "正在整理資料…") {
  $("loadingText").textContent = text;
  $("loadingOverlay").classList.toggle("d-none", !show);
}

function showAlert(message, type = "success") {
  const box = $("alertBox");
  box.className = `alert alert-${type} app-alert`;
  box.innerHTML = `<i class="bi ${type === "danger" ? "bi-exclamation-triangle" : "bi-check-circle"} me-2"></i>${message}`;
}

function hideAlert() { $("alertBox").classList.add("d-none"); }

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`);
  return data;
}

function formatValue(value, unit) {
  return `${Number(value).toLocaleString("zh-TW", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}${unit}`;
}

function renderSummary(payload) {
  const s = payload.summary;
  const cards = [
    ["有效樣本", Number(s.count).toLocaleString("zh-TW"), "bi-database", "blue"],
    ["平均", formatValue(s.mean, payload.indicator.unit), "bi-calculator", "cyan"],
    ["中位數", formatValue(s.median, payload.indicator.unit), "bi-distribute-vertical", "violet"],
    ["標準差", formatValue(s.std, payload.indicator.unit), "bi-activity", "amber"],
    ["最小值", formatValue(s.min, payload.indicator.unit), "bi-arrow-down-right", "green"],
    ["最大值", formatValue(s.max, payload.indicator.unit), "bi-arrow-up-right", "red"],
    ["最常出現級距", s.mode_bucket, "bi-bullseye", "slate"],
  ];
  $("summaryGrid").innerHTML = cards.map(([label, value, icon, color]) => `
    <article class="metric-card metric-${color}">
      <div class="metric-icon"><i class="bi ${icon}"></i></div>
      <div><div class="metric-label">${label}</div><div class="metric-value" title="${value}">${value}</div></div>
    </article>`).join("");
}

function renderChart(payload) {
  $("chartTitle").textContent = `${payload.market.name}｜${payload.indicator.title}`;
  const trace = {
    type: "bar",
    x: payload.chart.labels,
    y: payload.chart.counts,
    customdata: payload.chart.percentages,
    text: payload.chart.percentages.map(v => `${v.toFixed(2)}%`),
    textposition: "outside",
    cliponaxis: false,
    marker: { color: "#2563eb", line: { color: "#1d4ed8", width: 1 }, cornerradius: 4 },
    hovertemplate: "級距：%{x}<br>次數：%{y:,}<br>占比：%{customdata:.2f}%<extra></extra>",
  };
  const layout = {
    height: 510,
    margin: { l: 55, r: 18, t: 30, b: 115 },
    paper_bgcolor: "transparent",
    plot_bgcolor: "transparent",
    font: { family: "Inter, Segoe UI, Microsoft JhengHei, sans-serif", color: "#475569" },
    xaxis: { tickangle: -45, gridcolor: "transparent", title: payload.indicator.unit },
    yaxis: { title: "出現次數", gridcolor: "#e8edf5", rangemode: "tozero" },
    bargap: .16,
  };
  Plotly.react("distributionChart", [trace], layout, { responsive: true, displaylogo: false, modeBarButtonsToRemove: ["lasso2d", "select2d"] });
}

function renderTable(payload) {
  $("tableTitle").textContent = `${payload.market.name}｜${payload.indicator.title}`;
  const table = $("distributionTable");
  table.querySelector("thead").innerHTML = `<tr>${payload.columns.map(c => `<th>${c}</th>`).join("")}</tr>`;
  table.querySelector("tbody").innerHTML = payload.rows.map(row => `<tr>${payload.columns.map(c => {
    const cls = c === "總計" ? "cell-total" : c === "分組占比" ? "cell-share" : "";
    return `<td class="${cls}">${row[c] ?? ""}</td>`;
  }).join("")}</tr>`).join("");
}

async function loadDashboard(showSuccess = false) {
  hideAlert();
  setLoading(true);
  try {
    const payload = await requestJson(`/api/dashboard?${queryString()}`);
    state.payload = payload;
    applySourceBounds(payload);
    $("sourceBadge").innerHTML = `<i class="bi bi-database-check"></i> ${payload.source.min_date} ～ ${payload.source.max_date}`;
    renderSummary(payload);
    renderChart(payload);
    renderTable(payload);
    saveSettings();
    if (showSuccess) showAlert(`統計已重新整理：每組 ${payload.binning.width}${payload.indicator.unit}，共 ${payload.binning.bucket_count} 組、${Number(payload.summary.count).toLocaleString("zh-TW")} 筆有效樣本。`);
  } catch (error) {
    showAlert(`讀取失敗：${error.message}`, "danger");
  } finally {
    setLoading(false);
  }
}

async function updateData() {
  hideAlert();
  setLoading(true, "正在更新加權指數與台指期資料…");
  try {
    const result = await requestJson("/api/update", { method: "POST" });
    await saveSettings();
    showAlert(`更新完成：加權至 ${result.taiex_max_date}；台指期至 ${result.tx_max_date}（同步 ${result.tx_rows_synced} 列）。`);
  } catch (error) {
    showAlert(`更新失敗：${error.message}`, "danger");
  } finally {
    setLoading(false);
  }
}

$("refreshBtn").addEventListener("click", () => loadDashboard(true));
$("updateBtn").addEventListener("click", updateData);
$("market").addEventListener("change", () => saveSettings());
$("indicator").addEventListener("change", () => { applyIndicatorDefaults(); saveSettings(); });
$("resetBinBtn").addEventListener("click", () => { applyIndicatorDefaults(); loadDashboard(true); });
$("binWidth").addEventListener("change", () => saveSettings());
$("startDate").addEventListener("change", () => saveSettings());
$("endDate").addEventListener("change", () => saveSettings());
$("periodMode").addEventListener("change", () => { togglePeriodMode(); saveSettings(); });
$("startYear").addEventListener("change", () => {
  if (Number($("startYear").value) > Number($("endYear").value)) $("endYear").value = $("startYear").value;
  saveSettings();
});
$("endYear").addEventListener("change", () => {
  if (Number($("endYear").value) < Number($("startYear").value)) $("startYear").value = $("endYear").value;
  saveSettings();
});
$("downloadBtn").addEventListener("click", () => { window.location.href = `/api/export.csv?${queryString()}`; });
$("saveChartBtn").addEventListener("click", () => Plotly.downloadImage("distributionChart", { format: "png", filename: "級距統計", width: 1400, height: 800 }));
$("chartExpandBtn").addEventListener("click", () => toggleExpandedPanel("chartPanel"));
$("tableExpandBtn").addEventListener("click", () => toggleExpandedPanel("tablePanel"));
document.addEventListener("keydown", (event) => { if (event.key === "Escape") closeExpandedPanel(); });
window.addEventListener("resize", () => {
  if (expandedPanelId === "chartPanel") resizeDistributionChart(true);
});

const restoredSettings = restoreSettings();
bootstrapSavedYears(restoredSettings);
togglePeriodMode();
applyIndicatorDefaults();
if (restoredSettings.binWidth && Number(restoredSettings.binWidth) > 0) $("binWidth").value = restoredSettings.binWidth;
saveSettings();
