const $ = (id) => document.getElementById(id);
const SETTINGS_KEY = "investDashboard.nightBucket.v1";
const state = { payload: null };

function readSettings() { try { return JSON.parse(localStorage.getItem(SETTINGS_KEY) || "{}"); } catch { return {}; } }
function saveSettings() { localStorage.setItem(SETTINGS_KEY, JSON.stringify({ metric: $("metric").value, startDate: $("startDate").value, endDate: $("endDate").value, selectedBucket: $("selectedBucket").value })); }
function restoreSettings() { const saved = readSettings(); for (const id of ["metric", "selectedBucket"]) if (saved[id] && [...$(id).options].some(o => o.value === saved[id])) $(id).value = saved[id]; if (saved.startDate) $("startDate").value = saved.startDate; if (saved.endDate) $("endDate").value = saved.endDate; }
function queryString() { return new URLSearchParams({ start_date: $("startDate").value, end_date: $("endDate").value, metric: $("metric").value }).toString(); }
function setLoading(show) { $("loadingOverlay").classList.toggle("d-none", !show); }
function hideAlert() { $("alertBox").classList.add("d-none"); }
function showAlert(message, type = "success") { const box = $("alertBox"); box.className = `alert alert-${type} app-alert`; box.textContent = message; }
async function requestJson(url) { const response = await fetch(url); const data = await response.json().catch(() => ({})); if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`); return data; }
function number(value, digits = 2) { return value == null ? "—" : Number(value).toLocaleString("zh-TW", { minimumFractionDigits: digits, maximumFractionDigits: digits }); }
function pct(value) { return value == null ? "—" : `${number(value)}%`; }

function renderSummary(payload) {
  const s = payload.overall;
  const cards = [
    ["有效樣本", Number(s.sample_count).toLocaleString("zh-TW"), "bi-database", "blue"],
    ["指標平均", `${number(s.mean)}%`, "bi-bar-chart", "violet"],
    ["指標中位數", `${number(s.median)}%`, "bi-distribute-vertical", "cyan"],
    ["正值比例", pct(s.positive_rate_pct), "bi-percent", "green"],
    ["夜盤方向延續率", pct(s.continuation_rate_pct), "bi-signpost-split", "red"],
    ["回補昨收率", pct(s.fill_rate_pct), "bi-arrow-return-left", "blue"],
  ];
  $("nightSummary").innerHTML = cards.map(([label, value, icon, color]) => `<article class="metric-card metric-${color}"><div class="metric-icon"><i class="bi ${icon}"></i></div><div><div class="metric-label">${label}</div><div class="metric-value">${value}</div></div></article>`).join("");
}

function renderBucketOptions(payload) {
  const previous = $("selectedBucket").value || readSettings().selectedBucket || "6";
  $("selectedBucket").innerHTML = payload.buckets.map(row => `<option value="${row.bucket_id}">${row.bucket_label}（${row.sample_count}）</option>`).join("");
  $("selectedBucket").value = [...$("selectedBucket").options].some(o => o.value === previous) ? previous : "6";
}

function renderTable(payload) {
  $("nightTable").querySelector("tbody").innerHTML = payload.buckets.map(row => `<tr class="${row.sample_warning ? "sample-warning" : ""}"><td>${row.bucket_id}</td><td>${row.bucket_label.replace(/^\d+\s/, "")}${row.sample_warning ? '<span class="warning-badge">樣本少</span>' : ""}</td><td>${row.sample_count.toLocaleString("zh-TW")}</td><td>${pct(row.mean)}</td><td>${pct(row.median)}</td><td>${pct(row.std)}</td><td>${pct(row.q10)}</td><td>${pct(row.q90)}</td><td>${pct(row.positive_rate_pct)}</td><td>${pct(row.continuation_rate_pct)}</td><td>${pct(row.fill_rate_pct)}</td></tr>`).join("");
}

const layout = { margin: { l: 62, r: 30, t: 30, b: 92 }, paper_bgcolor: "transparent", plot_bgcolor: "transparent", font: { family: "Inter, Segoe UI, Microsoft JhengHei, sans-serif", color: "#475569" }, xaxis: { tickangle: -28, gridcolor: "transparent" }, yaxis: { ticksuffix: "%", gridcolor: "#e8edf5", zerolinecolor: "#94a3b8" }, legend: { orientation: "h", x: 0, y: 1.14 } };

function renderTenBucketCharts(payload) {
  const labels = payload.buckets.map(row => row.bucket_label.replace(/^\d+\s/, ""));
  Plotly.react("comparisonChart", [
    { type: "bar", name: "平均數", x: labels, y: payload.buckets.map(r => r.mean), marker: { color: "#3b82f6" } },
    { type: "scatter", mode: "lines+markers", name: "中位數", x: labels, y: payload.buckets.map(r => r.median), line: { color: "#f05252", width: 2.5 } },
  ], { ...layout, height: 430 }, { responsive: true, displaylogo: false });

  Plotly.react("boxChart", payload.box_rows.map(row => ({ type: "box", name: row.bucket_label.replace(/^\d+\s/, ""), y: row.values, boxpoints: false, marker: { color: row.bucket_id <= 5 ? "#ef4444" : "#3b82f6" } })), { ...layout, height: 430, showlegend: false }, { responsive: true, displaylogo: false });

  const years = [...new Set(payload.yearly_rows.map(row => row.year))].sort();
  const z = years.map(year => Array.from({ length: 10 }, (_, index) => payload.yearly_rows.find(row => row.year === year && row.bucket_id === index + 1)?.mean ?? null));
  const text = years.map(year => Array.from({ length: 10 }, (_, index) => { const row = payload.yearly_rows.find(item => item.year === year && item.bucket_id === index + 1); return row ? `平均 ${number(row.mean)}%<br>n=${row.sample_count}` : "無樣本"; }));
  Plotly.react("yearlyChart", [{ type: "heatmap", x: labels, y: years, z, text, hovertemplate: "%{y}<br>%{x}<br>%{text}<extra></extra>", colorscale: [[0, "#dc2626"], [0.5, "#ffffff"], [1, "#2563eb"]], zmid: 0, colorbar: { title: "%" } }], { ...layout, height: 430, yaxis: { autorange: "reversed", dtick: 1, gridcolor: "transparent" } }, { responsive: true, displaylogo: false });
}

function renderSelectedBucketChart(payload) {
  const bucketId = Number($("selectedBucket").value || 6);
  const selected = payload.box_rows.find(row => row.bucket_id === bucketId) || payload.box_rows[0];
  $("histogramTitle").textContent = `${selected.bucket_label}｜${payload.metric.label}`;
  Plotly.react("histogramChart", [{ type: "histogram", x: selected.values, marker: { color: "#6366f1" }, nbinsx: Math.max(8, Math.min(35, Math.ceil(Math.sqrt(selected.values.length)))) }], { ...layout, height: 430, xaxis: { title: "%", gridcolor: "#e8edf5" }, yaxis: { title: "樣本數", gridcolor: "#e8edf5", ticksuffix: "" } }, { responsive: true, displaylogo: false });
}

async function loadData(showSuccess = false) {
  hideAlert(); setLoading(true);
  try {
    const payload = await requestJson(`/api/night-bucket?${queryString()}`); state.payload = payload;
    $("startDate").min = payload.run.start_date; $("startDate").max = payload.run.end_date;
    $("endDate").min = payload.run.start_date; $("endDate").max = payload.run.end_date;
    $("sourceBadge").innerHTML = `<i class="bi bi-database-check"></i> ${payload.run.start_date} ～ ${payload.run.end_date}｜v${payload.run.metric_version}`;
    renderSummary(payload); renderBucketOptions(payload); renderTable(payload); renderTenBucketCharts(payload); renderSelectedBucketChart(payload); saveSettings();
    if (showSuccess) showAlert(`分層統計已重新整理，共 ${payload.overall.sample_count.toLocaleString("zh-TW")} 筆有效樣本。`);
  } catch (error) { showAlert(`讀取失敗：${error.message}`, "danger"); } finally { setLoading(false); }
}

$("refreshBtn").addEventListener("click", () => loadData(true));
$("metric").addEventListener("change", () => saveSettings());
$("startDate").addEventListener("change", () => saveSettings());
$("endDate").addEventListener("change", () => saveSettings());
$("selectedBucket").addEventListener("change", () => { if (state.payload) { renderSelectedBucketChart(state.payload); saveSettings(); } });
document.querySelectorAll(".export-btn").forEach(button => button.addEventListener("click", () => { window.location.href = `/api/night-bucket/export.csv?dataset=${button.dataset.dataset}&${queryString()}`; }));
restoreSettings(); saveSettings();
