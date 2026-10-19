(() => {
  const $ = id => document.getElementById(id);
  const STORAGE_KEY = "investDashboard.yuKeyLevels.v1";
  const state = { chart: null, candle: null, volume: null, levelLabels: null, payload: null, bars: [], zones: [], actualZones: [], unmatchedYu: [], comparisonStats: null, priceLines: new Map(), verticals: [], observer: null };
  const STATUS_LABELS = {
    ACTIVE_SUPPORT: "支撐有效",
    ACTIVE_RESISTANCE: "壓力有效",
    BROKEN_UP: "向上突破",
    BROKEN_DOWN: "向下跌破",
    FLIPPED_TO_SUPPORT: "突破後轉支撐",
    FLIPPED_TO_RESISTANCE: "跌破後轉壓力",
    OVERTESTED: "反覆穿越",
    INVALIDATED: "已失效",
    "YU實際使用": "Yu實際使用",
  };

  class VerticalRenderer {
    constructor(view) { this.view = view; }
    draw(target) {
      if (this.view.x === null) return;
      target.useBitmapCoordinateSpace(scope => {
        const x = Math.round(this.view.x * scope.horizontalPixelRatio); const ctx = scope.context;
        ctx.save(); ctx.strokeStyle = "#7c3aed"; ctx.lineWidth = scope.horizontalPixelRatio;
        ctx.setLineDash([]); ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, scope.bitmapSize.height); ctx.stroke(); ctx.restore();
      });
    }
  }
  class VerticalView {
    constructor(source) { this.source = source; this.x = null; }
    update() { this.x = this.source.chart?.timeScale().timeToCoordinate(this.source.time) ?? null; }
    renderer() { return new VerticalRenderer(this); }
    zOrder() { return "top"; }
  }
  class VerticalAxisView {
    constructor(source) { this.source = source; this.x = null; }
    update() { this.x = this.source.chart?.timeScale().timeToCoordinate(this.source.time) ?? null; }
    visible() { return this.x !== null; }
    tickVisible() { return true; }
    coordinate() { return this.x ?? -1; }
    text() {
      const value = typeof this.source.time === "object"
        ? `${this.source.time.year}-${String(this.source.time.month).padStart(2, "0")}-${String(this.source.time.day).padStart(2, "0")}`
        : String(this.source.time ?? "").slice(0, 10);
      const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
      return match ? `${match[2]}/${match[3]}` : value;
    }
    textColor() { return "#ffffff"; }
    backColor() { return "#7c3aed"; }
  }
  class VerticalPrimitive {
    constructor(time, showAxisLabel = false) { this.time = time; this.chart = null; this.view = new VerticalView(this); this.axisView = showAxisLabel ? new VerticalAxisView(this) : null; }
    attached({ chart, requestUpdate }) { this.chart = chart; requestUpdate(); }
    detached() { this.chart = null; }
    updateAllViews() { this.view.update(); this.axisView?.update(); }
    paneViews() { return [this.view]; }
    timeAxisViews() { return this.axisView ? [this.axisView] : []; }
  }

  function escapeHtml(value) { return String(value ?? "").replace(/[&<>'"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" }[c])); }
  function number(value) { return Number(value).toLocaleString("zh-TW", { maximumFractionDigits: 0 }); }
  function statusLabel(value) { return STATUS_LABELS[String(value ?? "")] || String(value ?? ""); }
  function showAlert(message, type = "danger") { const box = $("alertBox"); box.className = `alert alert-${type} app-alert`; box.textContent = message; }
  function setLoading(value) { $("loadingOverlay").classList.toggle("d-none", !value); }
  async function json(url) { const response = await fetch(url); const data = await response.json().catch(() => ({})); if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`); return data; }
  function readSettings() { try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}"); } catch { return {}; } }
  function saveSettings() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({
      candidateMode: $("candidateMode").value,
      periodMode: $("periodMode").value,
      startDate: $("startDate").value,
      endDate: $("endDate").value,
      startYear: $("startYear").value,
      endYear: $("endYear").value,
      visibleGrades: [...document.querySelectorAll(".grade-filter:checked")].map(item => item.value),
      minimumScore: Number($("minimumScore").value || 0),
      yuCompareEnabled: $("yuCompareEnabled").checked,
      yuCompareTolerance: Number($("yuCompareTolerance").value || 35),
      chartHeight: $("yuLevelChartFrame")?.clientHeight || null,
    }));
  }
  function shiftMonths(dateText, months) {
    const [year, month, day] = dateText.split("-").map(Number);
    const date = new Date(Date.UTC(year, month - 1, day));
    date.setUTCMonth(date.getUTCMonth() + months);
    return date.toISOString().slice(0, 10);
  }
  function fillYears(minDate, maxDate, saved) {
    const minYear = Number(minDate.slice(0, 4)); const maxYear = Number(maxDate.slice(0, 4));
    for (const id of ["startYear", "endYear"]) {
      $(id).innerHTML = "";
      for (let year = minYear; year <= maxYear; year += 1) {
        const option = document.createElement("option"); option.value = String(year); option.textContent = `${year} 年`; $(id).appendChild(option);
      }
    }
    $("startYear").value = String(Math.min(maxYear, Math.max(minYear, Number(saved.startYear) || Number($("startDate").value.slice(0, 4)))));
    $("endYear").value = String(Math.min(maxYear, Math.max(minYear, Number(saved.endYear) || Number($("endDate").value.slice(0, 4)))));
  }
  function togglePeriodMode() {
    const yearMode = $("periodMode").value === "year";
    $("startDateField").classList.toggle("d-none", yearMode); $("endDateField").classList.toggle("d-none", yearMode);
    $("startYearField").classList.toggle("d-none", !yearMode); $("endYearField").classList.toggle("d-none", !yearMode);
  }
  function selectedPeriod() {
    if ($("periodMode").value === "year") {
      const minDate = $("startDate").min; const maxDate = $("endDate").max;
      const requestedStart = `${$("startYear").value}-01-01`; const requestedEnd = `${$("endYear").value}-12-31`;
      return { start: requestedStart < minDate ? minDate : requestedStart, end: requestedEnd > maxDate ? maxDate : requestedEnd };
    }
    return { start: $("startDate").value, end: $("endDate").value };
  }
  function restoreSettings() {
    const saved = readSettings(); const minDate = $("startDate").min; const maxDate = $("endDate").max;
    if (saved.candidateMode && [...$("candidateMode").options].some(option => option.value === saved.candidateMode)) $("candidateMode").value = saved.candidateMode;
    if (saved.periodMode && [...$("periodMode").options].some(option => option.value === saved.periodMode)) $("periodMode").value = saved.periodMode;
    const defaultStart = shiftMonths(maxDate, -3);
    $("startDate").value = saved.startDate && saved.startDate >= minDate && saved.startDate <= maxDate ? saved.startDate : (defaultStart < minDate ? minDate : defaultStart);
    $("endDate").value = saved.endDate && saved.endDate >= minDate && saved.endDate <= maxDate ? saved.endDate : maxDate;
    if (Number(saved.chartHeight) >= 480 && Number(saved.chartHeight) <= 1200) $("yuLevelChartFrame").style.height = `${Number(saved.chartHeight)}px`;
    const visibleGrades = new Set(Array.isArray(saved.visibleGrades) ? saved.visibleGrades : ["S", "A", "B"]);
    document.querySelectorAll(".grade-filter").forEach(item => { item.checked = visibleGrades.has(item.value); });
    $("minimumScore").value = Number.isFinite(Number(saved.minimumScore)) ? Number(saved.minimumScore) : 55;
    $("yuCompareEnabled").checked = Boolean(saved.yuCompareEnabled);
    $("yuCompareTolerance").value = Number.isFinite(Number(saved.yuCompareTolerance)) ? Number(saved.yuCompareTolerance) : 35;
    fillYears(minDate, maxDate, saved); togglePeriodMode();
  }

  function destroyChart() {
    state.observer?.disconnect(); state.levelLabels?.destroy(); state.chart?.remove();
    Object.assign(state, { chart: null, candle: null, volume: null, levelLabels: null, payload: null, priceLines: new Map(), verticals: [], observer: null });
  }

  function createChart(payload) {
    destroyChart(); const LC = window.LightweightCharts; const container = $("yuLevelChart");
    if (!LC) throw new Error("Lightweight Charts 載入失敗");
    state.chart = LC.createChart(container, {
      width: container.clientWidth, height: container.clientHeight,
      layout: { background: { type: "solid", color: "#fff" }, textColor: "#526178", fontFamily: 'Inter, "Microsoft JhengHei", sans-serif', panes: { separatorColor: "#dbe3ee", enableResize: true } },
      grid: { vertLines: { color: "#edf1f6" }, horzLines: { color: "#edf1f6" } },
      rightPriceScale: { borderColor: "#d7dee9", scaleMargins: { top: .07, bottom: .07 } },
      timeScale: { borderColor: "#d7dee9", rightOffset: 3, barSpacing: 8, minBarSpacing: 2 },
      localization: { locale: "zh-TW", priceFormatter: price => number(price) },
    });
    state.candle = state.chart.addSeries(LC.CandlestickSeries, { upColor: "#ef4444", downColor: "#22c55e", borderUpColor: "#ef4444", borderDownColor: "#22c55e", wickUpColor: "#ef4444", wickDownColor: "#22c55e", priceLineVisible: false, lastValueVisible: false, priceFormat: { type: "price", precision: 0, minMove: 1 } });
    state.volume = state.chart.addSeries(LC.HistogramSeries, { priceFormat: { type: "volume" }, priceLineVisible: false, lastValueVisible: false }, 1);
    state.levelLabels = new PriceLevelLabels(state.chart, state.candle, container, $("yuLevelLabels"));
    state.payload = payload;
    state.bars = payload.bars;
    state.candle.setData(payload.bars.map(({ time, open, high, low, close }) => ({ time, open, high, low, close })));
    state.volume.setData(payload.bars.map(bar => ({ time: bar.time, value: bar.volume, color: bar.close >= bar.open ? "rgba(239,68,68,.65)" : "rgba(34,197,94,.65)" })));
    const panes = state.chart.panes(); if (panes[1]) panes[1].setHeight(130);
    state.chart.subscribeCrosshairMove(param => {
      const bar = param?.seriesData?.get(state.candle); if (!bar) return;
      const change = bar.close - bar.open;
      $("ohlcLegend").innerHTML = `<strong>${escapeHtml(param.time)}</strong><span>開 ${number(bar.open)}</span><span>高 ${number(bar.high)}</span><span>低 ${number(bar.low)}</span><span>收 ${number(bar.close)}</span><span class="${change >= 0 ? "up" : "down"}">${change >= 0 ? "+" : ""}${number(change)}</span>`;
    });
    state.observer = new ResizeObserver(entries => { const box = entries[0].contentRect; state.chart?.resize(Math.floor(box.width), Math.floor(box.height)); });
    state.observer.observe(container);
  }

  function fitRange(start, end) {
    const visible = state.bars.filter(bar => bar.time >= start && bar.time <= end);
    if (visible.length) state.chart.timeScale().setVisibleRange({ from: visible[0].time, to: visible.at(-1).time }); else state.chart.timeScale().fitContent();
  }

  function computeYuComparison() {
    const enabled = $("yuCompareEnabled").checked && state.actualZones.length > 0;
    const tolerance = Math.max(0, Math.min(500, Number($("yuCompareTolerance").value || 35)));
    state.zones.forEach(zone => { zone.yuMatches = []; zone.yuMatched = false; });
    state.unmatchedYu = [];
    state.comparisonStats = null;
    if (!enabled) {
      $("yuComparisonSummary").classList.add("d-none");
      return;
    }
    const candidates = state.zones.filter(zone => zone.filteredVisible);
    for (const zone of candidates) {
      zone.yuMatches = state.actualZones.filter(actual =>
        Number(actual.level) >= Number(zone.lower) - tolerance
        && Number(actual.level) <= Number(zone.upper) + tolerance
      );
      zone.yuMatched = zone.yuMatches.length > 0;
    }
    state.unmatchedYu = state.actualZones.filter(actual => !candidates.some(zone =>
      Number(actual.level) >= Number(zone.lower) - tolerance
      && Number(actual.level) <= Number(zone.upper) + tolerance
    ));
    const total = state.actualZones.length;
    const covered = total - state.unmatchedYu.length;
    state.comparisonStats = { total, covered, missed: state.unmatchedYu.length, coverage: total ? covered / total * 100 : 0, tolerance };
    const box = $("yuComparisonSummary");
    box.classList.remove("d-none");
    box.innerHTML = `<span class="yu-compare-stat"><i class="bi bi-check2-circle me-1"></i>Yu實際點位 ${total} 個</span><span>已涵蓋 ${covered} 個</span><span>未涵蓋 ${state.unmatchedYu.length} 個</span><span>涵蓋率 ${state.comparisonStats.coverage.toFixed(1)}%</span><span>容許誤差 ±${number(tolerance)} 點</span><span class="yu-compare-legend">紫色虛線＝Yu有選、目前候選未涵蓋</span>`;
  }

  function drawZones(start, end) {
    computeYuComparison();
    for (const line of state.priceLines.values()) state.candle.removePriceLine(line);
    state.priceLines.clear();
    const labelItems = [];
    for (const item of state.verticals) item.series.detachPrimitive(item.primitive);
    state.verticals = [];
    for (const zone of state.zones) {
      if (!zone.visible || !zone.filteredVisible) continue;
      const alpha = ({ S: "", A: "", B: "cc", C: "88", D: "55" })[zone.importance_grade] ?? "88";
      const baseColor = zone.distance <= 0 ? "#16a34a" : "#dc2626";
      const matchPrefix = $("yuCompareEnabled").checked && zone.yuMatched ? "✓Yu " : "";
      const line = state.candle.createPriceLine({ price: zone.level, color: `${baseColor}${alpha}`, lineWidth: 1, lineStyle: 0, axisLabelVisible: false, title: "" });
      state.priceLines.set(zone.id, line);
      labelItems.push({ price: zone.level, color: baseColor, name: `${matchPrefix}${zone.importance_grade}${Math.round(zone.importance_score)} #${zone.rank}` });
    }
    if ($("yuCompareEnabled").checked) {
      for (const actual of state.unmatchedYu) {
        const line = state.candle.createPriceLine({ price: Number(actual.level), color: "#7c3aed", lineWidth: 1, lineStyle: 2, axisLabelVisible: false, title: "" });
        state.priceLines.set(`YU-MISSED-${actual.id}`, line);
        labelItems.push({ price: Number(actual.level), color: "#7c3aed", name: "Yu未涵蓋" });
      }
    }
    state.levelLabels?.setItems(labelItems);
    const dates = [...new Set(state.zones.filter(zone => zone.visible && zone.filteredVisible).flatMap(zone => zone.source_dates))].filter(date => date >= start && date <= end && state.bars.some(bar => bar.time === date));
    for (const date of dates) {
      const priceLine = new VerticalPrimitive(date, true); const volumeLine = new VerticalPrimitive(date, false);
      state.candle.attachPrimitive(priceLine); state.volume.attachPrimitive(volumeLine);
      state.verticals.push({ series: state.candle, primitive: priceLine }, { series: state.volume, primitive: volumeLine });
    }
    renderRows();
  }

  function renderRows() {
    const rows = state.zones.filter(zone => zone.filteredVisible);
    $("candidateRows").innerHTML = rows.map(zone => {
      const dateText = zone.analysis_dates?.length
        ? `使用：${zone.analysis_dates.join("、")}｜參考：${zone.source_dates.join("、")}`
        : zone.source_dates.join("、");
      const direction = `${zone.direction_bias || "中性"} ${Number(zone.direction_strength || 0).toFixed(0)}`;
      const reasons = [zone.candle_shapes, zone.reasons].filter(Boolean).join("｜").replaceAll("|", "、");
      const comparison = !$("yuCompareEnabled").checked
        ? "—"
        : zone.yuMatched
          ? `<span class="yu-match-badge" title="${escapeHtml(zone.yuMatches.map(item => number(item.level)).join("、"))}"><i class="bi bi-check2 me-1"></i>Yu重疊</span>`
          : '<span class="yu-no-match-badge">未重疊</span>';
      return `<tr><td><input class="form-check-input zone-visible" type="checkbox" data-id="${escapeHtml(zone.id)}" ${zone.visible ? "checked" : ""}></td><td>${zone.rank}</td><td><span class="badge text-bg-${zone.importance_grade === "S" ? "danger" : zone.importance_grade === "A" ? "warning" : zone.importance_grade === "B" ? "primary" : "secondary"}">${escapeHtml(zone.importance_grade)}</span></td><td>${Number(zone.importance_score || 0).toFixed(1)}</td><td><strong>${number(zone.level)}</strong></td><td>${number(zone.lower)} ～ ${number(zone.upper)}</td><td>${comparison}</td><td>${escapeHtml(reasons)}</td><td>${escapeHtml(direction)}</td><td>${escapeHtml(dateText)}</td><td>${zone.source_count}</td><td title="${escapeHtml(zone.status)}">${escapeHtml(statusLabel(zone.status))}</td></tr>`;
    }).join("") || '<tr><td colspan="12" class="text-center text-secondary py-4">目前篩選條件下沒有點位</td></tr>';
  }

  function applyGradeFilter() {
    const grades = new Set([...document.querySelectorAll(".grade-filter:checked")].map(item => item.value));
    const minimum = Math.max(0, Math.min(100, Number($("minimumScore").value || 0)));
    state.zones.forEach(zone => { zone.filteredVisible = grades.has(zone.importance_grade) && Number(zone.importance_score || 0) >= minimum; });
    if (state.chart) drawZones($("startDate").value, $("endDate").value); else { computeYuComparison(); renderRows(); }
    if (state.payload && $("candidateSummary").textContent !== "尚未產生候選") {
      const shown = state.zones.filter(zone => zone.filteredVisible).length;
      $("candidateSummary").textContent = $("candidateSummary").textContent.replace(/目前分級篩選顯示 \d+ 組/, `目前分級篩選顯示 ${shown} 組`);
    }
    saveSettings();
  }

  function downloadChartScreenshot() {
    if (!state.chart || !state.payload) return showAlert("目前沒有可下載的 K 線圖");
    try {
      // addTopLayer=true：將Yu形成日垂直線等 primitives 一併輸出；截圖不保留滑鼠十字線。
      const chartCanvas = state.chart.takeScreenshot(true, false);
      const compareEnabled = $("yuCompareEnabled").checked && state.comparisonStats;
      const headerHeight = compareEnabled ? 82 : 66;
      const canvas = document.createElement("canvas");
      const includeLevelLabels = (state.levelLabels?.items.length ?? 0) > 0;
      const railWidth = includeLevelLabels ? Math.round($("yuLevelLabels").clientWidth * chartCanvas.width / $("yuLevelChart").clientWidth) : 0;
      canvas.width = chartCanvas.width + railWidth; canvas.height = chartCanvas.height + headerHeight;
      const context = canvas.getContext("2d");
      context.fillStyle = "#ffffff"; context.fillRect(0, 0, canvas.width, canvas.height);
      context.fillStyle = "#162033"; context.font = '700 15px Inter, "Microsoft JhengHei", sans-serif';
      context.fillText($("chartTitle").textContent, 14, 22);
      context.fillStyle = "#526178"; context.font = '12px Inter, "Microsoft JhengHei", sans-serif';
      context.fillText($("ohlcLegend").innerText.replace(/\s+/g, "  "), 14, 43);
      const modeText = $("candidateMode").selectedOptions[0]?.textContent || "Yu點位";
      context.fillStyle = "#64748b";
      context.fillText(`${modeText}｜${$("startDate").value}～${$("endDate").value}`, 14, 59);
      if (compareEnabled) context.fillText(`Yu對照｜涵蓋 ${state.comparisonStats.covered}/${state.comparisonStats.total}｜容許誤差 ±${number(state.comparisonStats.tolerance)} 點`, 14, 75);
      context.drawImage(chartCanvas, 0, headerHeight);
      if (includeLevelLabels) state.levelLabels?.drawScreenshot(context, chartCanvas.width, headerHeight, chartCanvas.width / $("yuLevelChart").clientWidth);
      const link = document.createElement("a");
      const modeFileName = ({
        strict: "嚴格關鍵K",
        broad: "寬鬆候選全集",
        actual: "Yu實際使用點位",
      })[$("candidateMode").value] || modeText;
      const compareSuffix = compareEnabled ? "_Yu對照" : "";
      link.download = `加權指數_日K_Yu點位_${modeFileName}${compareSuffix}_${$("startDate").value}_${$("endDate").value}.png`.replace(/[\\/:*?"<>|\s]+/g, "_");
      link.href = canvas.toDataURL("image/png");
      link.click();
      showAlert("K 線截圖已下載。", "success");
    } catch (error) { showAlert(`截圖下載失敗：${error.message}`); }
  }

  async function load() {
    const { start, end } = selectedPeriod();
    const mode = $("candidateMode").value;
    if (!start || !end || start > end) return showAlert("請確認開始與結束日期");
    saveSettings();
    setLoading(true); $("alertBox").classList.add("d-none");
    try {
      const compareEnabled = $("yuCompareEnabled").checked;
      const [kline, levels, actual] = await Promise.all([
        json(`/api/kline?${new URLSearchParams({ market: "taiex", interval: "day", start_date: start, end_date: end })}`),
        json(`/api/yu-key-levels?${new URLSearchParams({ start_date: start, as_of_date: end, mode })}`),
        compareEnabled ? json(`/api/yu-key-levels?${new URLSearchParams({ start_date: start, as_of_date: end, mode: "actual" })}`) : Promise.resolve({ zones: [] }),
      ]);
      createChart(kline); state.zones = levels.zones.map(zone => ({ ...zone, visible: true, filteredVisible: true })); state.actualZones = actual.zones || [];
      applyGradeFilter(); fitRange(kline.meta.display_start_date, kline.meta.display_end_date);
      $("startDate").value = kline.meta.display_start_date; $("endDate").value = kline.meta.display_end_date;
      if ($("periodMode").value === "date") saveSettings();
      const modeName = ({ strict: "嚴格關鍵K", broad: "寬鬆候選全集", actual: "Yu實際使用點位" })[levels.mode] || levels.mode;
      const shown = state.zones.filter(zone => zone.filteredVisible).length;
      $("candidateSummary").textContent = `${modeName}｜${levels.start_date}～${levels.as_of_date} 共產生 ${levels.raw_count} 個原始點位，合併為 ${levels.count} 組；目前分級篩選顯示 ${shown} 組。紅線在收盤上方、綠線在下方；紫色垂直線為形成日。`;
      $("sourceBadge").innerHTML = `<i class="bi bi-database-check"></i> ${kline.meta.source_min_date} ～ ${kline.meta.source_max_date}`;
      showAlert(`已載入區間內 ${levels.count} 組歷史候選點位。`, "success");
    } catch (error) { showAlert(`載入失敗：${error.message}`); }
    finally { setLoading(false); }
  }

  $("loadBtn").addEventListener("click", load);
  $("periodMode").addEventListener("change", () => { togglePeriodMode(); saveSettings(); });
  $("candidateMode").addEventListener("change", saveSettings);
  document.querySelectorAll(".grade-filter").forEach(item => item.addEventListener("change", applyGradeFilter));
  $("minimumScore").addEventListener("change", applyGradeFilter);
  $("yuCompareEnabled").addEventListener("change", saveSettings);
  $("yuCompareTolerance").addEventListener("change", () => { saveSettings(); if (state.chart) drawZones($("startDate").value, $("endDate").value); });
  for (const id of ["startDate", "endDate"]) $(id).addEventListener("change", saveSettings);
  $("startYear").addEventListener("change", () => { if (+$("startYear").value > +$("endYear").value) $("endYear").value = $("startYear").value; saveSettings(); });
  $("endYear").addEventListener("change", () => { if (+$("endYear").value < +$("startYear").value) $("startYear").value = $("endYear").value; saveSettings(); });
  $("yuLevelChartFrame").addEventListener("mouseup", saveSettings);
  $("yuLevelChartFrame").addEventListener("touchend", saveSettings);
  $("downloadChartBtn").addEventListener("click", downloadChartScreenshot);
  $("fitBtn").addEventListener("click", () => fitRange($("startDate").value, $("endDate").value));
  $("candidateRows").addEventListener("change", event => {
    if (!event.target.matches(".zone-visible")) return;
    const zone = state.zones.find(item => item.id === event.target.dataset.id); if (!zone) return;
    zone.visible = event.target.checked; drawZones($("startDate").value, $("endDate").value); renderRows();
  });
  $("showAllBtn").addEventListener("click", () => {
    state.zones.forEach(zone => { zone.visible = true; });
    renderRows(); drawZones($("startDate").value, $("endDate").value);
  });
  $("hideAllBtn").addEventListener("click", () => {
    state.zones.forEach(zone => { zone.visible = false; });
    renderRows(); drawZones($("startDate").value, $("endDate").value);
  });
  restoreSettings(); load();
})();
