(() => {
  const $ = id => document.getElementById(id);
  const plotConfig = { responsive: true, displaylogo: false, modeBarButtonsToRemove: ["lasso2d", "select2d"] };
  const colors = { positive: "#0f9f75", negative: "#d45a64", neutral: "#64748b", blue: "#3676d8", amber: "#d99525" };
  let payload = null;

  const fmt = (value, digits = 2, suffix = "") => value == null ? "—" : `${Number(value).toFixed(digits)}${suffix}`;
  const signed = (value, digits = 3, suffix = "%") => value == null ? "—" : `${value > 0 ? "+" : ""}${Number(value).toFixed(digits)}${suffix}`;
  const tone = value => value > 0 ? "metric-positive" : value < 0 ? "metric-negative" : "";
  const scenarioLabel = row => `${row.core_direction}｜${row.night_change_bucket}｜${row.signal_night_trend}`;
  const layoutBase = { paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", font: { family: "Inter, 'Noto Sans TC', sans-serif", color: "#40516a", size: 12 }, margin: { l: 58, r: 28, t: 24, b: 58 }, hoverlabel: { bgcolor: "#172033", font: { color: "white" } } };

  function showAlert(message, type = "danger") {
    const box = $("alertBox"); box.className = `alert app-alert alert-${type}`; box.textContent = message;
  }
  function hideAlert() { $("alertBox").classList.add("d-none"); }
  function setLoading(active) { $("loadingOverlay").classList.toggle("d-none", !active); }

  function renderSummary() {
    const s = payload.selected;
    const title = [payload.filters.core, payload.filters.night_bucket, payload.filters.night_trend].filter(v => v !== "全部").join("・") || "全部情境";
    const cards = [
      ["目前情境", title, `${s.sample_count || 0} 筆樣本`],
      ["方向平均報酬", signed(s.directional_return_mean_pct), `中位數 ${signed(s.directional_return_median_pct)}`, tone(s.directional_return_mean_pct)],
      ["方向命中率", fmt(s.direction_hit_rate_pct, 2, "%"), `收漲 ${fmt(s.target_up_rate_pct, 2, "%")}`],
      ["方向報酬因子", fmt(s.directional_return_factor, 3), "大於 1 代表正報酬總和較高", tone((s.directional_return_factor || 0) - 1)],
      ["低收反彈 P50", fmt(s.low_rebound_p50_pct, 2, "%R30"), `平均 ${fmt(s.low_rebound_mean_pct, 2, "%R30")}`],
      ["高收回落 P50", fmt(s.high_pullback_p50_pct, 2, "%R30"), `平均 ${fmt(s.high_pullback_mean_pct, 2, "%R30")}`],
    ];
    $("summaryCards").innerHTML = cards.map(([label, value, note, cls = ""]) => `<article class="summary-card"><span>${label}</span><strong class="${cls}">${value}</strong><small>${note}</small></article>`).join("");
  }

  function renderOpportunity() {
    const rows = payload.matrix;
    const trace = {
      type: "scatter", mode: "markers", x: rows.map(r => r.sample_count), y: rows.map(r => r.directional_return_mean_pct),
      text: rows.map(scenarioLabel), customdata: rows.map(r => [r.core_direction, r.night_change_bucket, r.signal_night_trend, r.direction_hit_rate_pct, r.favorable_year_count, r.valid_year_count]),
      marker: { size: rows.map(r => 12 + Math.min(24, Math.sqrt(r.sample_count))), color: rows.map(r => r.direction_hit_rate_pct), colorscale: [[0, "#d35d66"], [.5, "#f1c768"], [1, "#148a70"]], cmin: 35, cmax: 65, colorbar: { title: "命中率", ticksuffix: "%" }, line: { color: "white", width: 1.5 }, opacity: .9 },
      hovertemplate: "%{text}<br>樣本 %{x}<br>方向平均 %{y:.3f}%<br>命中率 %{customdata[3]:.2f}%<br>有利年度 %{customdata[4]}/%{customdata[5]}<extra></extra>"
    };
    const layout = { ...layoutBase, xaxis: { title: "樣本數", gridcolor: "#e8edf5" }, yaxis: { title: "方向平均報酬 (%)", zeroline: true, zerolinecolor: "#7d8aa0", gridcolor: "#e8edf5" }, shapes: [{ type: "line", x0: 100, x1: 100, y0: 0, y1: 1, yref: "paper", line: { color: "#98a5b7", dash: "dot" } }] };
    Plotly.react("opportunityChart", [trace], layout, plotConfig);
    const chart = $("opportunityChart"); chart.removeAllListeners?.("plotly_click"); chart.on("plotly_click", event => {
      const d = event.points[0].customdata; $("coreFilter").value = d[0]; $("bucketFilter").value = d[1]; $("trendFilter").value = d[2]; load();
    });
  }

  function renderYearly() {
    const rows = payload.selected_yearly;
    const years = rows.map(r => String(r.year));
    Plotly.react("yearlyChart", [
      { type: "bar", name: "方向平均報酬", x: years, y: rows.map(r => r.directional_return_mean_pct), marker: { color: rows.map(r => r.directional_return_mean_pct >= 0 ? colors.positive : colors.negative) }, hovertemplate: "%{x}<br>方向平均 %{y:.3f}%<extra></extra>" },
      { type: "scatter", mode: "lines+markers", name: "方向命中率", x: years, y: rows.map(r => r.direction_hit_rate_pct), yaxis: "y2", line: { color: colors.amber, width: 2 }, hovertemplate: "%{x}<br>命中率 %{y:.2f}%<extra></extra>" },
    ], { ...layoutBase, margin: { ...layoutBase.margin, b: 48 }, legend: { orientation: "h", y: 1.16 }, xaxis: { gridcolor: "#edf1f6" }, yaxis: { title: "方向平均 (%)", zerolinecolor: "#8692a4", gridcolor: "#edf1f6" }, yaxis2: { title: "命中率", ticksuffix: "%", overlaying: "y", side: "right", range: [0, 100], showgrid: false } }, plotConfig);
  }

  function renderClosePosition() {
    const s = payload.selected;
    const names = ["低收反彈", "高收回落"];
    const prefixes = ["low_rebound", "high_pullback"];
    const median = prefixes.map(p => s[`${p}_p50_pct`]);
    const traces = [
      { type: "scatter", mode: "markers", name: "P10–P90", x: median, y: names, marker: { size: 2, color: "rgba(54,118,216,0)" }, error_x: { type: "data", symmetric: false, array: prefixes.map((p, i) => s[`${p}_p90_pct`] - median[i]), arrayminus: prefixes.map((p, i) => median[i] - s[`${p}_p10_pct`]), color: "#9eb7dc", thickness: 3, width: 0 }, hoverinfo: "skip" },
      { type: "scatter", mode: "markers", name: "P25–P75", x: median, y: names, marker: { size: 2, color: "rgba(54,118,216,0)" }, error_x: { type: "data", symmetric: false, array: prefixes.map((p, i) => s[`${p}_p75_pct`] - median[i]), arrayminus: prefixes.map((p, i) => median[i] - s[`${p}_p25_pct`]), color: colors.blue, thickness: 10, width: 0 }, hoverinfo: "skip" },
      { type: "scatter", mode: "markers", name: "中位數", x: median, y: names, marker: { size: 12, color: "white", line: { color: colors.blue, width: 3 } }, customdata: prefixes.map(p => [s[`${p}_p10_pct`], s[`${p}_p25_pct`], s[`${p}_p75_pct`], s[`${p}_p90_pct`]]), hovertemplate: "%{y}<br>P10 %{customdata[0]:.2f}<br>P25 %{customdata[1]:.2f}<br>P50 %{x:.2f}<br>P75 %{customdata[2]:.2f}<br>P90 %{customdata[3]:.2f}<extra></extra>" }
    ];
    Plotly.react("closePositionChart", traces, { ...layoutBase, legend: { orientation: "h", y: 1.15 }, xaxis: { title: "占30日平均振幅比例 (%R30)", rangemode: "tozero", gridcolor: "#edf1f6" }, yaxis: { categoryorder: "array", categoryarray: names } }, plotConfig);
  }

  function renderHeatmap() {
    const rows = payload.yearly_matrix;
    const years = [...new Set(rows.map(r => r.year))].sort();
    const scenarios = [...new Set(rows.map(scenarioLabel))];
    const lookup = new Map(rows.map(r => [`${scenarioLabel(r)}|${r.year}`, r.directional_return_mean_pct]));
    const z = scenarios.map(label => years.map(year => lookup.get(`${label}|${year}`) ?? null));
    Plotly.react("stabilityHeatmap", [{ type: "heatmap", x: years, y: scenarios, z, zmid: 0, colorscale: [[0, "#b84755"], [.5, "#f7f8fa"], [1, "#16886e"]], colorbar: { title: "方向平均%" }, hovertemplate: "%{y}<br>%{x}：%{z:.3f}%<extra></extra>" }], { ...layoutBase, margin: { l: 180, r: 35, t: 20, b: 45 }, xaxis: { dtick: 1 }, yaxis: { automargin: true, tickfont: { size: 11 } } }, plotConfig);
  }

  function renderTable() {
    const body = $("scenarioTable").querySelector("tbody");
    body.innerHTML = payload.matrix.map(r => `<tr data-core="${r.core_direction}" data-bucket="${r.night_change_bucket}" data-trend="${r.signal_night_trend}" class="${r.sample_status === "LOW_SAMPLE" ? "low-sample-row" : ""}">
      <td><span class="direction-chip ${r.core_direction === "多" ? "long" : "short"}">${r.core_direction}</span></td><td>${r.night_change_bucket}</td><td>${r.signal_night_trend}</td>
      <td>${r.sample_count}${r.sample_status === "LOW_SAMPLE" ? '<span class="sample-warning" title="樣本少於100筆">低樣本</span>' : ""}</td>
      <td>${fmt(r.direction_hit_rate_pct, 2, "%")}</td><td class="${tone(r.directional_return_mean_pct)}">${signed(r.directional_return_mean_pct)}</td><td class="${tone(r.directional_return_median_pct)}">${signed(r.directional_return_median_pct)}</td>
      <td>${fmt(r.directional_return_factor, 3)}</td><td>${r.favorable_year_count}/${r.valid_year_count}</td><td>${fmt(r.low_rebound_p50_pct, 2, "%")}</td><td>${fmt(r.high_pullback_p50_pct, 2, "%")}</td></tr>`).join("");
    body.querySelectorAll("tr").forEach(row => row.addEventListener("click", () => { $("coreFilter").value = row.dataset.core; $("bucketFilter").value = row.dataset.bucket; $("trendFilter").value = row.dataset.trend; load(); window.scrollTo({ top: 0, behavior: "smooth" }); }));
  }

  function render() { renderSummary(); renderOpportunity(); renderYearly(); renderClosePosition(); renderHeatmap(); renderTable(); }
  async function load() {
    hideAlert(); setLoading(true);
    const params = new URLSearchParams({ start_date: $("startDate").value, end_date: $("endDate").value, core: $("coreFilter").value, night_bucket: $("bucketFilter").value, night_trend: $("trendFilter").value });
    try {
      const response = await fetch(`/api/next-day-session?${params}`); const data = await response.json();
      if (!response.ok) throw new Error(data.detail || "讀取失敗"); payload = data; render();
      $("sourceBadge").innerHTML = `<i class="bi bi-database-check"></i> ${data.period.actual_start}～${data.period.actual_end}`;
    } catch (error) { showAlert(error.message); } finally { setLoading(false); }
  }

  $("refreshBtn").addEventListener("click", load);
  $("resetScenarioBtn").addEventListener("click", () => { $("coreFilter").value = "全部"; $("bucketFilter").value = "全部"; $("trendFilter").value = "全部"; load(); });
  window.addEventListener("resize", () => ["opportunityChart", "yearlyChart", "closePositionChart", "stabilityHeatmap"].forEach(id => Plotly.Plots.resize($(id))));
  load();
})();
