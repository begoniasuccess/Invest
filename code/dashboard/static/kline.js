const $ = id => document.getElementById(id);
const LEGACY_STORAGE_KEY = "investDashboard.kline.v1";
const SCALE_STORAGE_KEY = "investDashboard.kline.scale.v2";
let activeScale = 'daily';
let chartRequestId = 0;
function storageObject(key) { try { const value = JSON.parse(localStorage.getItem(key) || '{}'); return value && typeof value === 'object' && !Array.isArray(value) ? value : {}; } catch { return {}; } }
const oldSettings = storageObject(LEGACY_STORAGE_KEY);
const scaleSettings = storageObject(SCALE_STORAGE_KEY);
if (!scaleSettings.migrated) {
  const oldScale = ['day', 'week', 'month', 'year'].includes(oldSettings.interval || 'day') ? 'daily' : 'intraday';
  for (const scale of ['daily', 'intraday']) {
    const copy = structuredClone(oldSettings);
    copy.interval = scale === oldScale ? (copy.interval || 'day') : (scale === 'daily' ? 'day' : '5m');
    localStorage.setItem(`${SCALE_STORAGE_KEY}.${scale}`, JSON.stringify(copy));
  }
  scaleSettings.active = oldScale; scaleSettings.migrated = true;
  localStorage.setItem(SCALE_STORAGE_KEY, JSON.stringify(scaleSettings));
}
activeScale = scaleSettings.active === 'intraday' ? 'intraday' : 'daily';
const initialSelectors = {};
const allIntervalOptions = [];
const DEFAULT_MAS = [
  { period: 5, color: "#f59e0b", visible: true },
  { period: 10, color: "#38bdf8", visible: true },
  { period: 20, color: "#22c55e", visible: true },
  { period: 60, color: "#eab308", visible: true },
];
const DEFAULT_DRAW_SETTINGS = {
  horizontal: { color: "#ef4444", style: 2, width: 2 },
  vertical: { color: "#8b5cf6", style: 2, width: 2 },
};
const state = { payload: null, chart: null, candle: null, volume: null, dif: null, macd: null, osc: null, levelLabels: null, maSeries: new Map(), priceLines: new Map(), verticalPrimitives: [], editingLineId: null, editingVerticalId: null, drawMode: null, expanded: false, settingsCollapsed: false, observer: null, updatePollTimer: null, updateStartedHere: false, nextLineSortDirection: "desc", nextVerticalSortDirection: "asc" };

class VerticalLineRenderer {
  constructor(view) { this.view = view; }
  draw(target) {
    const x = this.view.x;
    if (x === null) return;
    target.useBitmapCoordinateSpace(scope => {
      const ctx = scope.context;
      const scaledX = Math.round(x * scope.horizontalPixelRatio);
      ctx.save();
      ctx.strokeStyle = this.view.item.color;
      ctx.lineWidth = Math.max(1, Number(this.view.item.width || 2) * scope.horizontalPixelRatio);
      ctx.setLineDash(this.view.item.style === 0 ? [] : this.view.item.style === 1 ? [2, 4] : [8, 5]);
      ctx.beginPath(); ctx.moveTo(scaledX, 0); ctx.lineTo(scaledX, scope.bitmapSize.height); ctx.stroke(); ctx.restore();
    });
  }
}

class VerticalLinePaneView {
  constructor(source) { this.source = source; this.item = source.item; this.x = null; }
  update() { this.x = this.source.chart?.timeScale().timeToCoordinate(this.item.time) ?? null; }
  renderer() { return new VerticalLineRenderer(this); }
  zOrder() { return "top"; }
}

class VerticalLineAxisView {
  constructor(source) { this.source = source; this.x = null; }
  update() { this.x = this.source.chart?.timeScale().timeToCoordinate(this.source.item.time) ?? null; }
  visible() { return this.x !== null; }
  tickVisible() { return true; }
  coordinate() { return this.x ?? -1; }
  text() { return this.source.item.name || this.source.item.requestedDate; }
  textColor() { return "#ffffff"; }
  backColor() { return this.source.item.color; }
}

class VerticalLinePrimitive {
  constructor(item, showAxisLabel = false) {
    this.item = item; this.chart = null; this.requestUpdate = null;
    this.paneView = new VerticalLinePaneView(this);
    this.axisView = showAxisLabel ? new VerticalLineAxisView(this) : null;
  }
  attached({ chart, requestUpdate }) { this.chart = chart; this.requestUpdate = requestUpdate; requestUpdate(); }
  detached() { this.chart = null; this.requestUpdate = null; }
  updateAllViews() { this.paneView.update(); this.axisView?.update(); }
  paneViews() { return [this.paneView]; }
  timeAxisViews() { return this.axisView ? [this.axisView] : []; }
}

function escapeHtml(value) { return String(value ?? "").replace(/[&<>'"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" }[c])); }
function readStorage() { return storageObject(`${SCALE_STORAGE_KEY}.${activeScale}`); }
function writeStorage(value) { localStorage.setItem(`${SCALE_STORAGE_KEY}.${activeScale}`, JSON.stringify(value)); }
function drawSettings(kind) {
  const saved = readStorage(); saved.drawSettings ||= {};
  return { ...DEFAULT_DRAW_SETTINGS[kind], ...(saved.drawSettings[kind] || {}) };
}
function rememberDrawSettings(kind, values) {
  const saved = readStorage(); saved.drawSettings ||= {};
  saved.drawSettings[kind] = { color: values.color, style: Number(values.style), width: Number(values.width) };
  writeStorage(saved);
}
function formDrawSettings(kind) {
  const prefix = kind === "horizontal" ? "line" : "vertical";
  return { color: $(`${prefix}Color`).value, style: Number($(`${prefix}Style`).value), width: Number($(`${prefix}Width`).value) };
}
function applyDrawSettings(kind) {
  const prefix = kind === "horizontal" ? "line" : "vertical";
  const settings = drawSettings(kind);
  $(`${prefix}Color`).value = settings.color;
  $(`${prefix}Style`).value = String(settings.style);
  $(`${prefix}Width`).value = String(settings.width);
}
function layoutKey() { return `${state.payload?.meta.market_key || $("market").value}:${state.payload?.meta.interval_key || $("interval").value}`; }
function currentLayout() {
  const saved = readStorage(); saved.layouts ||= {};
  saved.layouts[layoutKey()] ||= { mas: structuredClone(DEFAULT_MAS), lines: [], verticals: [] };
  saved.layouts[layoutKey()].mas ||= structuredClone(DEFAULT_MAS);
  saved.layouts[layoutKey()].lines ||= [];
  saved.layouts[layoutKey()].verticals ||= [];
  for (const ma of saved.layouts[layoutKey()].mas) delete ma.horizontal;
  for (const line of saved.layouts[layoutKey()].lines) { line.price = Math.round(Number(line.price)); line.width ??= 2; line.visible ??= true; }
  for (const line of saved.layouts[layoutKey()].verticals) { line.width ??= 2; line.visible ??= true; }
  return { saved, layout: saved.layouts[layoutKey()] };
}
function saveSelectors() { const saved = readStorage(); saved.market = $("market").value; saved.interval = $("interval").value; saved.startDate = $("startDate").value; saved.endDate = $("endDate").value; saved.updateModules = [$("updateTaiexKbar").checked ? "taiex_kbar" : "", $("updateFuturesKbar").checked ? "futures_kbar" : ""].filter(Boolean); writeStorage(saved); }
function saveLayout(layout) { const saved = readStorage(); saved.layouts ||= {}; saved.layouts[layoutKey()] = layout; writeStorage(saved); }
function restoreSelectors() {
  const saved = readStorage();
  $('interval').replaceChildren(...allIntervalOptions.filter(option =>
    ['day', 'week', 'month', 'year'].includes(option.value) === (activeScale === 'daily')).map(option => option.cloneNode(true)));
  for (const id of ['market', 'interval']) {
    const value = saved[id] || (id === 'interval' ? (activeScale === 'daily' ? 'day' : '5m') : initialSelectors.market);
    if ([...$(id).options].some(option => option.value === value)) $(id).value = value;
  }
  for (const id of ['startDate', 'endDate']) {
    $(id).removeAttribute('min'); $(id).removeAttribute('max');
    $(id).type = activeScale === 'intraday' ? 'datetime-local' : 'date';
    $(id).step = activeScale === 'intraday' ? '60' : '1';
    const value = saved[id] || initialSelectors[id];
    $(id).value = activeScale === 'daily' ? value.slice(0, 10) : value.includes('T') ? value.slice(0, 16) : `${value.slice(0, 10)}T${id === 'startDate' ? '00:00' : '23:59'}`;
    document.querySelector(`label[for="${id}"]`).textContent = activeScale === 'daily' ? (id === 'startDate' ? '開始日期' : '結束日期') : (id === 'startDate' ? '開始日期時間' : '結束日期時間');
  }
  if (Array.isArray(saved.updateModules)) {
    $('updateTaiexKbar').checked = saved.updateModules.includes('taiex_kbar');
    $('updateFuturesKbar').checked = saved.updateModules.includes('futures_kbar');
  } else { $('updateTaiexKbar').checked = true; $('updateFuturesKbar').checked = true; }
  for (const scale of ['daily', 'intraday']) $(`${scale}ScaleTab`).setAttribute('aria-selected', String(scale === activeScale));
}
function switchScale(scale) {
  if (scale === activeScale) return;
  saveSelectors(); ++chartRequestId; activeScale = scale;
  const saved = storageObject(SCALE_STORAGE_KEY); saved.active = scale;
  localStorage.setItem(SCALE_STORAGE_KEY, JSON.stringify(saved));
  setLoading(false); setDrawMode(null);
  state.levelLabels?.destroy();
  state.chart?.remove(); state.chart = null; state.payload = null;
  state.candle = null; state.volume = null; state.dif = null; state.macd = null; state.osc = null;
  state.levelLabels = null; state.maSeries.clear(); state.priceLines.clear(); state.verticalPrimitives = [];
  for (const id of ['klineLevelLabels', 'maLegend', 'maList', 'lineList', 'verticalList']) $(id).replaceChildren();
  $('macdLegend').classList.add('d-none'); $('ohlcLegend').textContent = '請按重新整理載入 K 線';
  $('chartTitle').textContent = scale === 'daily' ? '日 K 以上' : '日 K 以下';
  $('sourceBadge').textContent = '等待重新整理';
  restoreSelectors(); applyDrawSettings('horizontal'); applyDrawSettings('vertical');
  resetLineEditor(); resetVerticalEditor();
  $('volumeVisible').checked = readStorage().volumeVisible !== false;
  $('difVisible').checked = Boolean(readStorage().difVisible);
  selectSettingsTab(readStorage().settingsTab === 'annotation' ? 'annotation' : 'subplot');
  setSettingsCollapsed(Boolean(readStorage().settingsCollapsed), false);
  const height = Number(readStorage().chartHeight);
  $('klineChartFrame').style.height = height >= 480 && height <= 1200 ? `${height}px` : '';
  restoreSettingSections(); updatePresetControls(); hideAlert();
}

function setLoading(show) { $("loadingOverlay").classList.toggle("d-none", !show); }
function showAlert(message, type = "danger") { const box = $("alertBox"); box.className = `alert alert-${type} app-alert`; box.textContent = message; }
function hideAlert() { $("alertBox").classList.add("d-none"); }
async function requestJson(url, options = {}) { const response = await fetch(url, options); const data = await response.json().catch(() => ({})); if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`); return data; }
function formatNumber(value, digits = 0) { return Number(value).toLocaleString("zh-TW", { minimumFractionDigits: digits, maximumFractionDigits: digits }); }
function lineStyleName(style) { return ({ 0: "實線", 1: "點線", 2: "虛線" })[Number(style)] || "虛線"; }

function formatTaipeiTime(time, includeDate = true) {
  if (typeof time !== "number") {
    if (time && typeof time === "object") {
      const month = String(time.month).padStart(2, "0");
      const day = String(time.day).padStart(2, "0");
      return `${time.year}/${month}/${day}`;
    }
    return String(time ?? "");
  }
  const options = {
    timeZone: "Asia/Taipei",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  };
  if (includeDate) Object.assign(options, { year: "numeric", month: "2-digit", day: "2-digit" });
  return new Intl.DateTimeFormat("zh-TW", options).format(new Date(time * 1000));
}

function formatTaipeiTick(time, tickMarkType) {
  // Lightweight Charts 的 Unix timestamp 刻度預設以 UTC 呈現；分 K 必須固定轉回臺北時間。
  if (Number(tickMarkType) <= 2) {
    return new Intl.DateTimeFormat("zh-TW", {
      timeZone: "Asia/Taipei",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date(Number(time) * 1000));
  }
  return formatTaipeiTime(time, false);
}

function calculateMa(bars, period) {
  let sum = 0;
  return bars.map((bar, index) => {
    sum += bar.close;
    if (index >= period) sum -= bars[index - period].close;
    return index >= period - 1 ? { time: bar.time, value: sum / period } : null;
  }).filter(Boolean);
}

// EMA uses an SMA seed; the first DIF is available after 26 bars.
function calculateDif(bars, fast = 12, slow = 26) {
  let fastSum = 0, slowSum = 0, fastEma = null, slowEma = null;
  const result = [];
  bars.forEach((bar, index) => {
    const close = Number(bar.close);
    if (index < fast) fastSum += close;
    if (index < slow) slowSum += close;
    if (index === fast - 1) fastEma = fastSum / fast;
    else if (index >= fast) fastEma += (close - fastEma) * 2 / (fast + 1);
    if (index === slow - 1) slowEma = slowSum / slow;
    else if (index >= slow) slowEma += (close - slowEma) * 2 / (slow + 1);
    if (slowEma !== null) result.push({ time: bar.time, value: fastEma - slowEma });
  });
  return result;
}

function calculateMacd(bars) {
  const dif = calculateDif(bars);
  let sum = 0, signal = null;
  return dif.map((point, index) => {
    if (index < 9) sum += point.value;
    if (index === 8) signal = sum / 9;
    else if (index > 8) signal += (point.value - signal) * 2 / 10;
    return { ...point, signal, osc: signal === null ? null : point.value - signal,
      direction: index === 0 ? '' : point.value > dif[index - 1].value ? '↑' : point.value < dif[index - 1].value ? '↓' : '→' };
  });
}

function updateDifLegend(time) {
  const point = state.macdPoints?.find(item => item.time === time);
  $('macdLegend').textContent = point
    ? `DIF ${formatNumber(point.value, 2)} ${point.direction}　MACD9 ${point.signal === null ? '—' : formatNumber(point.signal, 2)}　OSC ${point.osc === null ? '—' : formatNumber(point.osc, 2)}`
    : '資料不足 26 根 K 棒';
}

function renderDif() {
  if (!state.chart) return;
  // Remove companions before DIF so the shared pane remains available.
  for (const key of ['osc', 'macd', 'dif']) {
    if (state[key]) state.chart.removeSeries(state[key]);
    state[key] = null;
  }
  $('macdLegend').classList.toggle('d-none', !$('difVisible').checked);
  if (!$('difVisible').checked) { $('macdLegend').textContent = 'DIF12−26 / MACD9 / OSC'; state.levelLabels?.schedule(); return; }
  state.macdPoints = calculateMacd(state.payload?.bars || []);
  const common = { priceLineVisible: false, priceFormat: { type: 'price', precision: 2, minMove: .01 } };
  state.osc = state.chart.addSeries(LightweightCharts.HistogramSeries, { ...common, title: 'OSC' }, state.volume ? 2 : 1);
  state.osc.setData(state.macdPoints.filter(point => point.osc !== null).map(point => ({ time: point.time, value: point.osc, color: point.osc >= 0 ? '#ef4444' : '#22c55e' })));
  state.dif = state.chart.addSeries(LightweightCharts.LineSeries, { ...common, color: '#f97316', lineWidth: 2, title: 'DIF' }, state.volume ? 2 : 1);
  state.dif.setData(state.macdPoints.map(point => ({ time: point.time, value: point.value })));
  state.macd = state.chart.addSeries(LightweightCharts.LineSeries, { ...common, color: '#eab308', lineWidth: 2, title: 'MACD9' }, state.volume ? 2 : 1);
  state.macd.setData(state.macdPoints.filter(point => point.signal !== null).map(point => ({ time: point.time, value: point.signal })));
  state.dif.createPriceLine({ price: 0, color: '#94a3b8', lineWidth: 1, lineStyle: 2, axisLabelVisible: false, title: '' });
  const pane = state.chart.panes()[state.volume ? 2 : 1];
  if (pane) pane.setHeight(Math.max(100, Math.round($('klineChart').clientHeight * .2)));
  updateDifLegend(state.macdPoints.at(-1)?.time);
  state.levelLabels?.schedule();
}

function destroyChart() {
  state.observer?.disconnect(); state.observer = null;
  state.levelLabels?.destroy(); state.levelLabels = null;
  state.chart?.remove(); state.chart = null; state.candle = null; state.volume = null; state.dif = null; state.macd = null; state.osc = null; state.macdPoints = [];
  state.maSeries.clear(); state.priceLines.clear(); state.verticalPrimitives = [];
}

function createChart(payload) {
  destroyChart();
  if (!window.LightweightCharts) throw new Error("Lightweight Charts 載入失敗，請檢查網路連線後重新整理");
  const LC = window.LightweightCharts; const container = $("klineChart");
  state.chart = LC.createChart(container, {
    width: container.clientWidth, height: container.clientHeight,
    layout: { background: { type: "solid", color: "#ffffff" }, textColor: "#526178", fontFamily: 'Inter, "Segoe UI", "Microsoft JhengHei", sans-serif', panes: { separatorColor: "#dbe3ee", separatorHoverColor: "#93c5fd", enableResize: true } },
    grid: { vertLines: { color: "#edf1f6" }, horzLines: { color: "#edf1f6" } },
    crosshair: { mode: LC.CrosshairMode.Normal },
    rightPriceScale: { borderColor: "#d7dee9", scaleMargins: { top: 0.08, bottom: 0.08 } },
    timeScale: {
      borderColor: "#d7dee9",
      timeVisible: Boolean(payload.meta.intraday),
      secondsVisible: false,
      rightOffset: 3,
      barSpacing: 8,
      minBarSpacing: 2,
      tickMarkFormatter: payload.meta.intraday ? formatTaipeiTick : undefined,
    },
    localization: {
      locale: "zh-TW",
      priceFormatter: price => formatNumber(price, 0),
      timeFormatter: payload.meta.intraday ? time => formatTaipeiTime(time, true) : undefined,
    },
  });
  state.candle = state.chart.addSeries(LC.CandlestickSeries, { upColor: "#ef4444", downColor: "#22c55e", borderUpColor: "#ef4444", borderDownColor: "#22c55e", wickUpColor: "#ef4444", wickDownColor: "#22c55e", priceLineVisible: false, lastValueVisible: false, priceFormat: { type: "price", precision: 0, minMove: 1 } });
  if ($("volumeVisible").checked) state.volume = state.chart.addSeries(LC.HistogramSeries, { priceFormat: { type: "volume" }, priceLineVisible: false, lastValueVisible: false }, 1);
  state.levelLabels = new PriceLevelLabels(state.chart, state.candle, container, $("klineLevelLabels"));
  state.candle.setData(payload.bars.map(({ time, open, high, low, close }) => ({ time, open, high, low, close })));
  state.volume?.setData(payload.bars.map(bar => ({ time: bar.time, value: bar.volume, color: bar.close >= bar.open ? "rgba(239,68,68,.72)" : "rgba(34,197,94,.72)" })));
  const panes = state.chart.panes(); if (panes[1]) panes[1].setHeight(Math.max(120, Math.round(container.clientHeight * .24)));
  state.chart.subscribeCrosshairMove(updateLegend);
  state.chart.subscribeClick(handleChartDrawClick);
  state.observer = new ResizeObserver(entries => { const rect = entries[0].contentRect; if (rect.width && rect.height) state.chart?.resize(Math.floor(rect.width), Math.floor(rect.height)); });
  state.observer.observe(container);
}

function updateLegend(param) {
  if (!param?.time || !state.candle) return;
  const bar = param.seriesData.get(state.candle); if (!bar) return;
  const volume = (state.volume ? param.seriesData.get(state.volume)?.value : null) ?? state.payload?.bars.find(item => item.time === param.time)?.volume ?? 0; const change = bar.close - bar.open; const pct = bar.open ? change / bar.open * 100 : 0;
  if (state.dif) {
    updateDifLegend(param.time);
  }
  const timeText = state.payload?.meta.intraday
    ? formatTaipeiTime(param.time, true)
    : String(param.time);
  $("ohlcLegend").innerHTML = `<strong>${escapeHtml(timeText)}</strong><span>開 ${formatNumber(bar.open)}</span><span>高 ${formatNumber(bar.high)}</span><span>低 ${formatNumber(bar.low)}</span><span>收 ${formatNumber(bar.close)}</span><span class="${change >= 0 ? "quote-up" : "quote-down"}">${change >= 0 ? "+" : ""}${formatNumber(change)}（${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%）</span><span>量 ${formatNumber(volume, 0)}</span>`;
  const { layout } = currentLayout();
  $("maLegend").innerHTML = layout.mas.filter(ma => ma.visible && state.maSeries.has(ma.period)).map(ma => {
    const series = state.maSeries.get(ma.period);
    const value = param.seriesData.get(series)?.value ?? calculateMa(state.payload.bars.filter(item => item.time <= param.time), ma.period).at(-1)?.value;
    return Number.isFinite(value) ? `<span><i style="background:${ma.color}"></i><strong>MA${ma.period}</strong> ${formatNumber(value)}</span>` : "";
  }).join("");
}

function applyVisibleRange() {
  const bars = state.payload?.bars || []; if (!bars.length) return;
  const visible = bars.filter(bar => {
    if (!state.payload.meta.intraday) return bar.time >= state.payload.meta.display_start_date && bar.time <= state.payload.meta.display_end_date;
    const start = Date.parse(`${state.payload.meta.display_start_date}+08:00`) / 1000;
    const end = Date.parse(`${state.payload.meta.display_end_date}+08:00`) / 1000;
    return Number(bar.time) >= start && Number(bar.time) <= end;
  });
  if (visible.length) state.chart.timeScale().setVisibleRange({ from: visible[0].time, to: visible[visible.length - 1].time }); else state.chart.timeScale().fitContent();
}

function renderMas() {
  if (!state.chart || !state.payload) return;
  for (const series of state.maSeries.values()) state.chart.removeSeries(series);
  state.maSeries.clear(); const { layout } = currentLayout();
  for (const ma of layout.mas) {
    if (!ma.visible) continue;
    const maData = calculateMa(state.payload.bars, ma.period);
    const series = state.chart.addSeries(LightweightCharts.LineSeries, { color: ma.color, lineWidth: 2, title: "", priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false });
    series.setData(maData);
    series.applyOptions({ title: "", priceLineVisible: false, lastValueVisible: false });
    state.maSeries.set(ma.period, series);
  }
  $("maList").innerHTML = layout.mas.map(ma => `<div class="setting-item ma-setting-item"><label><input class="form-check-input ma-visible" type="checkbox" data-period="${ma.period}" ${ma.visible ? "checked" : ""}><span class="setting-swatch" style="background:${ma.color}"></span><strong>MA${ma.period}</strong></label><div class="ma-item-actions"><button class="setting-add-line ma-add-line" data-period="${ma.period}" title="將最右側 K 棒的 MA${ma.period} 新增為水平點位" ${ma.visible ? "" : "disabled"}><i class="bi bi-plus-lg"></i></button><button class="setting-delete ma-delete" data-period="${ma.period}" title="刪除"><i class="bi bi-trash3"></i></button></div></div>`).join("");
  updateLatestLegend();
}

function updateLatestLegend() {
  const last = state.payload?.bars?.at(-1);
  if (!last || !state.candle) return;
  const seriesData = new Map([[state.candle, last], [state.volume, { value: last.volume }]]);
  for (const [period, series] of state.maSeries) {
    const point = calculateMa(state.payload.bars, period).at(-1);
    if (point) seriesData.set(series, point);
  }
  if (state.dif) {
    const point = calculateDif(state.payload.bars).at(-1);
    if (point) seriesData.set(state.dif, point);
  }
  updateLegend({ time: last.time, seriesData });
}

function setSettingsCollapsed(collapsed, persist = true) {
  state.settingsCollapsed = collapsed;
  $("klinePanel").classList.toggle("settings-collapsed", collapsed);
  const button = $("settingsToggleBtn");
  button.title = collapsed ? "展開右側設定" : "收合右側設定";
  button.querySelector("i").className = `bi ${collapsed ? "bi-layout-sidebar" : "bi-layout-sidebar-inset-reverse"}`;
  if (persist) { const saved = readStorage(); saved.settingsCollapsed = collapsed; writeStorage(saved); }
  setTimeout(() => state.chart?.resize($("klineChart").clientWidth, $("klineChart").clientHeight), 80);
}

function downloadChartScreenshot() {
  if (!state.chart || !state.payload) return showAlert("目前沒有可下載的 K 線圖");
  try {
    // addTopLayer=true：將自訂垂直線等 primitives 一併輸出；截圖不保留滑鼠十字線。
    const chartCanvas = state.chart.takeScreenshot(true, false);
    const includeMacd = Boolean(state.dif);
    const headerHeight = includeMacd ? 96 : 74;
    const canvas = document.createElement("canvas");
    const includeLevelLabels = (state.levelLabels?.items.length ?? 0) > 0;
    const railWidth = includeLevelLabels ? Math.round($("klineLevelLabels").clientWidth * chartCanvas.width / $("klineChart").clientWidth) : 0;
    canvas.width = chartCanvas.width + railWidth; canvas.height = chartCanvas.height + headerHeight;
    const context = canvas.getContext("2d");
    context.fillStyle = "#ffffff"; context.fillRect(0, 0, canvas.width, canvas.height);
    context.fillStyle = "#162033"; context.font = '700 15px Inter, "Microsoft JhengHei", sans-serif';
    context.fillText($("chartTitle").textContent, 14, 22);
    context.fillStyle = "#526178"; context.font = '12px Inter, "Microsoft JhengHei", sans-serif';
    context.fillText($("ohlcLegend").innerText.replace(/\s+/g, "  "), 14, 43);
    let x = 14;
    for (const item of $("maLegend").querySelectorAll("span")) {
      const text = item.innerText.replace(/\s+/g, " ");
      context.fillStyle = item.querySelector("i")?.style.background || "#64748b";
      context.beginPath(); context.arc(x + 4, 60, 4, 0, Math.PI * 2); context.fill();
      context.fillStyle = "#344258"; context.fillText(text, x + 13, 64);
      x += context.measureText(text).width + 32;
    }
    if (includeMacd) {
      context.fillStyle = "#344258";
      context.fillText($("macdLegend").textContent, 14, 86);
    }
    context.drawImage(chartCanvas, 0, headerHeight);
    if (includeLevelLabels) state.levelLabels?.drawScreenshot(context, chartCanvas.width, headerHeight, chartCanvas.width / $("klineChart").clientWidth);
    const link = document.createElement("a");
    const meta = state.payload.meta;
    link.download = `${meta.market_name}_${meta.interval_name}_${meta.display_start_date}_${meta.display_end_date}.png`.replace(/[\\/:*?"<>|\s]+/g, "_");
    link.href = canvas.toDataURL("image/png");
    link.click();
    showAlert("K 線截圖已下載。", "success");
  } catch (error) { showAlert(`截圖下載失敗：${error.message}`); }
}

function rightmostVisibleMaPoint(period) {
  const bars = state.payload?.bars || [];
  if (!bars.length || !state.chart) return null;
  const range = state.chart.timeScale().getVisibleLogicalRange();
  const rightIndex = Math.min(bars.length - 1, Math.max(0, Math.floor(range?.to ?? bars.length - 1)));
  if (rightIndex < period - 1) return null;
  let sum = 0;
  for (let index = rightIndex - period + 1; index <= rightIndex; index += 1) sum += Number(bars[index].close);
  return { time: bars[rightIndex].time, value: sum / period };
}

function addMaAsPriceLine(period) {
  const { layout } = currentLayout();
  const ma = layout.mas.find(item => item.period === period);
  if (!ma?.visible) return showAlert(`請先開啟 MA${period} 均線`);
  const point = rightmostVisibleMaPoint(period);
  if (!point) return showAlert(`最右側 K 棒之前的資料不足，無法計算 MA${period}`);
  layout.lines.unshift({
    id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
    price: Math.round(point.value),
    name: `MA${period}（${point.time}）`,
    color: ma.color,
    style: 2,
    width: 2,
    visible: true,
  });
  saveLayout(layout); renderPriceLines(); hideAlert();
  const latestItem = $("lineList").firstElementChild;
  latestItem?.classList.add("setting-item-added");
  latestItem?.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

function renderPriceLines() {
  if (!state.chart || !state.payload) return;
  for (const line of state.priceLines.values()) state.candle.removePriceLine(line);
  state.priceLines.clear(); const { layout } = currentLayout();
  for (const item of layout.lines) {
    if (!item.visible) continue;
    const line = state.candle.createPriceLine({ price: item.price, color: item.color, lineWidth: Number(item.width || 2), lineStyle: Number(item.style), axisLabelVisible: false, title: "" });
    state.priceLines.set(item.id, line);
  }
  state.levelLabels?.setItems(layout.lines.filter(item => item.visible).map(item => ({ price: item.price, name: item.name || "點位", color: item.color })));
  $("lineList").innerHTML = layout.lines.map(item => `<div class="setting-item ${item.visible ? "" : "setting-item-muted"}"><label class="setting-visibility" title="${item.visible ? "隱藏" : "顯示"}這條水平線"><input class="form-check-input line-visible" type="checkbox" data-id="${escapeHtml(item.id)}" ${item.visible ? "checked" : ""}></label><div><span class="setting-swatch" style="background:${item.color}"></span><span class="setting-item-copy"><strong>${escapeHtml(item.name || "點位")}</strong><small>${formatNumber(item.price)} · ${Number(item.width || 2)}px · ${lineStyleName(item.style)}</small></span></div><div class="setting-item-actions"><button class="setting-edit line-edit" data-id="${escapeHtml(item.id)}" title="編輯"><i class="bi bi-pencil"></i></button><button class="setting-delete line-delete" data-id="${escapeHtml(item.id)}" title="刪除"><i class="bi bi-trash3"></i></button></div></div>`).join("") || `<div class="setting-empty">尚未加入水平點位</div>`;
}

function resetLineEditor() {
  state.editingLineId = null; $("lineForm").reset(); applyDrawSettings("horizontal");
  $("lineSubmitText").textContent = "加入點位"; $("cancelLineEditBtn").classList.add("d-none");
}

function editPriceLine(id) {
  const { layout } = currentLayout(); const item = layout.lines.find(line => line.id === id); if (!item) return;
  state.editingLineId = id; $("linePrice").value = item.price; $("lineName").value = item.name || ""; $("lineColor").value = item.color; $("lineStyle").value = String(item.style); $("lineWidth").value = String(item.width || 2);
  $("lineSubmitText").textContent = "儲存變更"; $("cancelLineEditBtn").classList.remove("d-none"); $("linePrice").focus();
}

function sortPriceLines() {
  const { layout } = currentLayout();
  const direction = state.nextLineSortDirection;
  layout.lines.sort((a, b) => direction === "asc" ? Number(a.price) - Number(b.price) : Number(b.price) - Number(a.price));
  state.nextLineSortDirection = direction === "asc" ? "desc" : "asc";
  const button = $("sortLinesBtn");
  const nextDescending = state.nextLineSortDirection === "desc";
  button.title = nextDescending ? "依點位由高到低排序" : "依點位由低到高排序";
  button.querySelector("i").className = nextDescending ? "bi bi-sort-numeric-down-alt" : "bi bi-sort-numeric-up";
  saveLayout(layout); renderPriceLines();
}

function utcDate(dateText) { const [year, month, day] = dateText.split("-").map(Number); return new Date(Date.UTC(year, month - 1, day)); }
function weekKey(dateText) { const date = utcDate(dateText); const day = date.getUTCDay() || 7; date.setUTCDate(date.getUTCDate() + 4 - day); const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1)); return `${date.getUTCFullYear()}-${String(Math.ceil((((date - yearStart) / 86400000) + 1) / 7)).padStart(2, "0")}`; }
function barDateText(time) {
  if (typeof time === "number") return new Date(time * 1000).toLocaleDateString("sv-SE", { timeZone: "Asia/Taipei" });
  return String(time).slice(0, 10);
}
function resolveVerticalTime(requestedDate) {
  const bars = state.payload?.bars || []; const interval = $("interval").value;
  if (state.payload?.meta.intraday) return bars.find(bar => barDateText(bar.time) === requestedDate)?.time || null;
  if (interval === "day") return bars.find(bar => bar.time === requestedDate)?.time || null;
  if (interval === "week") { const key = weekKey(requestedDate); return bars.find(bar => weekKey(bar.time) === key)?.time || null; }
  if (interval === "month") { const month = requestedDate.slice(0, 7); return bars.find(bar => bar.time.slice(0, 7) === month)?.time || null; }
  const year = requestedDate.slice(0, 4); return bars.find(bar => bar.time.slice(0, 4) === year)?.time || null;
}

function chartTimeKey(time) { return typeof time === "object" ? JSON.stringify(time) : String(time); }

function resolveVerticalItemTime(item) {
  const bars = state.payload?.bars || [];
  if (item.exactTime !== undefined && item.exactTime !== null) {
    const exactKey = chartTimeKey(item.exactTime);
    const exact = bars.find(bar => chartTimeKey(bar.time) === exactKey);
    if (exact) return exact.time;
  }
  return resolveVerticalTime(item.requestedDate);
}

function clearVerticalPrimitives() {
  for (const { series, primitive } of state.verticalPrimitives) series?.detachPrimitive(primitive);
  state.verticalPrimitives = [];
}

function renderVerticalLines() {
  if (!state.chart || !state.payload) return;
  clearVerticalPrimitives(); const { layout } = currentLayout();
  for (const item of layout.verticals) {
    if (!item.visible) continue;
    const resolvedTime = resolveVerticalItemTime(item);
    if (!resolvedTime) continue;
    const renderedItem = { ...item, time: resolvedTime };
    const pricePrimitive = new VerticalLinePrimitive(renderedItem, true);
    const volumePrimitive = new VerticalLinePrimitive(renderedItem, false);
    state.candle.attachPrimitive(pricePrimitive);
    state.verticalPrimitives.push({ series: state.candle, primitive: pricePrimitive });
    if (state.volume) {
      state.volume.attachPrimitive(volumePrimitive);
      state.verticalPrimitives.push({ series: state.volume, primitive: volumePrimitive });
    }
  }
  $("verticalList").innerHTML = layout.verticals.map(item => {
    const resolvedTime = resolveVerticalItemTime(item);
    const detail = resolvedTime && resolvedTime !== item.requestedDate ? `${item.requestedDate} → ${resolvedTime}` : item.requestedDate;
    return `<div class="setting-item ${item.visible ? "" : "setting-item-muted"}"><label class="setting-visibility" title="${item.visible ? "隱藏" : "顯示"}這條垂直線"><input class="form-check-input vertical-visible" type="checkbox" data-id="${escapeHtml(item.id)}" ${item.visible ? "checked" : ""}></label><div><span class="setting-swatch" style="background:${item.color}"></span><span class="setting-item-copy"><strong>${escapeHtml(item.name || "時間")}</strong><small>${escapeHtml(detail)} · ${Number(item.width || 2)}px · ${lineStyleName(item.style)}${resolvedTime ? "" : "（區間內無對應 K 棒）"}</small></span></div><div class="setting-item-actions"><button class="setting-edit vertical-edit" data-id="${escapeHtml(item.id)}" title="編輯"><i class="bi bi-pencil"></i></button><button class="setting-delete vertical-delete" data-id="${escapeHtml(item.id)}" title="刪除"><i class="bi bi-trash3"></i></button></div></div>`;
  }).join("") || `<div class="setting-empty">尚未加入垂直時間</div>`;
}

function resetVerticalEditor() {
  state.editingVerticalId = null; $("verticalForm").reset(); applyDrawSettings("vertical");
  $("verticalDate").value = state.payload?.meta.display_end_date?.slice(0, 10) || ""; $("verticalSubmitText").textContent = "加入時間"; $("cancelVerticalEditBtn").classList.add("d-none");
}

function editVerticalLine(id) {
  const { layout } = currentLayout(); const item = layout.verticals.find(line => line.id === id); if (!item) return;
  state.editingVerticalId = id; $("verticalDate").value = item.requestedDate; $("verticalName").value = item.name || ""; $("verticalColor").value = item.color; $("verticalStyle").value = String(item.style); $("verticalWidth").value = String(item.width || 2);
  $("verticalSubmitText").textContent = "儲存變更"; $("cancelVerticalEditBtn").classList.remove("d-none"); $("verticalDate").focus();
}

function sortVerticalLines() {
  const { layout } = currentLayout();
  const direction = state.nextVerticalSortDirection;
  layout.verticals.sort((a, b) => direction === "asc"
    ? String(a.requestedDate).localeCompare(String(b.requestedDate))
    : String(b.requestedDate).localeCompare(String(a.requestedDate)));
  state.nextVerticalSortDirection = direction === "asc" ? "desc" : "asc";
  const button = $("sortVerticalsBtn");
  const nextAscending = state.nextVerticalSortDirection === "asc";
  button.title = nextAscending ? "依日期由舊到新排序" : "依日期由新到舊排序";
  button.querySelector("i").className = nextAscending ? "bi bi-sort-down" : "bi bi-sort-up";
  saveLayout(layout); renderVerticalLines();
}

function hideDrawModeHint() {
  $("drawModeHint").classList.add("d-none");
}

function setDrawMode(mode) {
  hideDrawModeHint();
  state.drawMode = state.drawMode === mode ? null : mode;
  const active = state.drawMode;
  $("drawHorizontalBtn").classList.toggle("active", active === "horizontal");
  $("drawVerticalBtn").classList.toggle("active", active === "vertical");
  $("drawHorizontalBtn").setAttribute("aria-pressed", String(active === "horizontal"));
  $("drawVerticalBtn").setAttribute("aria-pressed", String(active === "vertical"));
  $("klineChart").classList.toggle("chart-draw-active", Boolean(active));
  const hint = $("drawModeHint");
  hint.classList.toggle("d-none", !active);
  if (active) hint.innerHTML = active === "horizontal"
    ? '<i class="bi bi-magnet"></i> 水平畫線：吸附最近 K 棒的開／高／低／收'
    : '<i class="bi bi-magnet"></i> 垂直畫線：吸附最近一根 K 棒的時間';
}

function nearestBarFromClick(param) {
  const bars = state.payload?.bars || [];
  if (!bars.length) return null;
  if (param?.time !== undefined) {
    const key = chartTimeKey(param.time);
    const exact = bars.find(bar => chartTimeKey(bar.time) === key);
    if (exact) return exact;
  }
  const logical = state.chart?.timeScale().coordinateToLogical(param?.point?.x);
  if (!Number.isFinite(logical)) return null;
  return bars[Math.max(0, Math.min(bars.length - 1, Math.round(logical)))] || null;
}

function highlightLatestItem(listId) {
  const item = $(listId).firstElementChild;
  item?.classList.add("setting-item-added");
  item?.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

function handleChartDrawClick(param) {
  if (!state.drawMode || !param?.point || !state.candle) return;
  const bar = nearestBarFromClick(param);
  if (!bar) return showAlert("游標附近找不到可吸附的 K 棒");
  const { layout } = currentLayout();
  const id = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  if (state.drawMode === "horizontal") {
    const cursorPrice = Number(state.candle.coordinateToPrice(param.point.y));
    if (!Number.isFinite(cursorPrice)) return showAlert("請點擊價格圖表區域");
    const candidates = [["開", Number(bar.open)], ["高", Number(bar.high)], ["低", Number(bar.low)], ["收", Number(bar.close)]].filter(([, value]) => Number.isFinite(value));
    const [label, snappedPrice] = candidates.reduce((best, item) => Math.abs(item[1] - cursorPrice) < Math.abs(best[1] - cursorPrice) ? item : best);
    const settings = formDrawSettings("horizontal");
    rememberDrawSettings("horizontal", settings);
    layout.lines.unshift({ id, price: Math.round(snappedPrice), name: `${label}（${barDateText(bar.time)}）`, visible: true, ...settings });
    saveLayout(layout); renderPriceLines(); highlightLatestItem("lineList");
    showAlert(`水平線已吸附至 ${barDateText(bar.time)} ${label}價 ${formatNumber(snappedPrice)}。`, "success");
    return;
  }
  const settings = formDrawSettings("vertical");
  rememberDrawSettings("vertical", settings);
  layout.verticals.unshift({ id, requestedDate: barDateText(bar.time), exactTime: bar.time, name: "", visible: true, ...settings });
  saveLayout(layout); renderVerticalLines(); highlightLatestItem("verticalList");
  showAlert(`垂直線已吸附至 ${state.payload?.meta.intraday ? formatTaipeiTime(bar.time, true) : barDateText(bar.time)}。`, "success");
}

async function loadChart(success = false) {
  hideAlert(); setLoading(true); saveSelectors();
  const requestId = ++chartRequestId; const requestedScale = activeScale;
  try {
    const qs = new URLSearchParams({ market: $("market").value, interval: $("interval").value, start_date: $("startDate").value, end_date: $("endDate").value });
    const payload = await requestJson(`/api/kline?${qs}`);
    if (requestId !== chartRequestId || requestedScale !== activeScale) return;
    state.payload = payload;
    for (const id of ['startDate', 'endDate']) {
      $(id).min = payload.meta.source_min_date + (payload.meta.intraday ? 'T00:00' : '');
      $(id).max = payload.meta.source_max_date + (payload.meta.intraday ? 'T23:59' : '');
    }
    $('verticalDate').min = payload.meta.source_min_date; $('verticalDate').max = payload.meta.source_max_date;
    $("startDate").value = payload.meta.display_start_date;
    $("endDate").value = payload.meta.display_end_date;
    resetLineEditor(); resetVerticalEditor();
    saveSelectors(); createChart(payload); renderDif(); renderMas(); renderPriceLines(); renderVerticalLines(); applyVisibleRange();
    $("sourceBadge").innerHTML = `<i class="bi bi-database-check"></i> ${payload.meta.source_min_date} ～ ${payload.meta.source_max_date}`;
    $("chartTitle").textContent = `${payload.meta.market_name}｜${payload.meta.interval_name}`;
    updateLatestLegend();
    updatePresetControls();
    if (success) showAlert("K 線圖已重新整理。", "success");
  } catch (error) { if (requestId === chartRequestId) showAlert(`讀取失敗：${error.message}`); }
  finally { if (requestId === chartRequestId) setLoading(false); }
}

function setKbarUpdateDisabled(disabled) {
  $("updateKbarBtn").disabled = disabled;
  $("kbarUpdateMenu").classList.toggle("update-menu-disabled", disabled);
  $("updateTaiexKbar").disabled = disabled;
  $("updateFuturesKbar").disabled = disabled;
}

async function pollKbarUpdateStatus() {
  try {
    const status = await requestJson("/api/report-update/status");
    $("kbarUpdateStatus").innerHTML = `<i class="bi ${status.running ? "bi-arrow-repeat spin" : status.step === "failed" ? "bi-exclamation-triangle" : "bi-database-check"}"></i> ${escapeHtml(status.message)}`;
    setKbarUpdateDisabled(status.running);
    if (status.running) {
      if (!state.updatePollTimer) state.updatePollTimer = setInterval(pollKbarUpdateStatus, 1500);
      return;
    }
    if (state.updatePollTimer) { clearInterval(state.updatePollTimer); state.updatePollTimer = null; }
    if (!state.updateStartedHere) return;
    state.updateStartedHere = false;
    if (status.step === "completed") await loadChart(false).then(() => showAlert(`${status.message}，K 線圖已重新載入。`, "success"));
    if (status.step === "failed") showAlert(status.message);
  } catch (error) {
    if (state.updatePollTimer) { clearInterval(state.updatePollTimer); state.updatePollTimer = null; }
    state.updateStartedHere = false; setKbarUpdateDisabled(false);
    showAlert(`無法取得更新狀態：${error.message}`);
  }
}

async function startKbarUpdate() {
  const modules = [
    $("updateTaiexKbar").checked ? "taiex_kbar" : "",
    $("updateFuturesKbar").checked ? "futures_kbar" : "",
  ].filter(Boolean);
  if (!modules.length) { $("kbarUpdateMenu").open = true; return showAlert("請至少選擇一個分 K 更新模組。"); }
  hideAlert(); setKbarUpdateDisabled(true);
  $("kbarUpdateMenu").open = false; saveSelectors();
  try {
    const status = await requestJson("/api/report-update", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ modules }),
    });
    state.updateStartedHere = true;
    $("kbarUpdateStatus").textContent = status.message;
    if (!state.updatePollTimer) state.updatePollTimer = setInterval(pollKbarUpdateStatus, 1500);
    await pollKbarUpdateStatus();
  } catch (error) {
    state.updateStartedHere = false; setKbarUpdateDisabled(false);
    showAlert(`啟動更新失敗：${error.message}`);
  }
}

$("maForm").addEventListener("submit", event => { event.preventDefault(); const period = Number($("maPeriod").value); if (!Number.isInteger(period) || period < 2 || period > 500) return showAlert("均線週期必須是 2～500 的整數"); const { layout } = currentLayout(); const existing = layout.mas.find(ma => ma.period === period); if (existing) { existing.color = $("maColor").value; existing.visible = true; } else layout.mas.push({ period, color: $("maColor").value, visible: true }); layout.mas.sort((a,b) => a.period-b.period); saveLayout(layout); renderMas(); });
$("maList").addEventListener("change", event => { if (!event.target.matches(".ma-visible")) return; const { layout } = currentLayout(); const ma = layout.mas.find(item => item.period === Number(event.target.dataset.period)); ma.visible = event.target.checked; saveLayout(layout); renderMas(); });
$("maList").addEventListener("click", event => { const addButton = event.target.closest(".ma-add-line"); if (addButton) return addMaAsPriceLine(Number(addButton.dataset.period)); const deleteButton = event.target.closest(".ma-delete"); if (!deleteButton) return; const { layout } = currentLayout(); layout.mas = layout.mas.filter(ma => ma.period !== Number(deleteButton.dataset.period)); saveLayout(layout); renderMas(); });
$("resetMaBtn").addEventListener("click", () => { const { layout } = currentLayout(); layout.mas = structuredClone(DEFAULT_MAS); saveLayout(layout); renderMas(); });
$("lineForm").addEventListener("submit", event => { event.preventDefault(); const rawPrice = Number($("linePrice").value); if (!Number.isFinite(rawPrice) || rawPrice <= 0) return showAlert("請輸入正確的水平點位"); const price = Math.round(rawPrice); const { layout } = currentLayout(); const values = { price, name: $("lineName").value.trim(), ...formDrawSettings("horizontal") }; rememberDrawSettings("horizontal", values); if (state.editingLineId) { const item = layout.lines.find(line => line.id === state.editingLineId); if (item) Object.assign(item, values); } else layout.lines.unshift({ id: `${Date.now()}-${Math.random().toString(16).slice(2)}`, visible: true, ...values }); saveLayout(layout); renderPriceLines(); resetLineEditor(); hideAlert(); });
$("lineList").addEventListener("change", event => { if (!event.target.matches(".line-visible")) return; const { layout } = currentLayout(); const item = layout.lines.find(line => line.id === event.target.dataset.id); if (!item) return; item.visible = event.target.checked; saveLayout(layout); renderPriceLines(); });
$("lineList").addEventListener("click", event => { const editButton = event.target.closest(".line-edit"); if (editButton) return editPriceLine(editButton.dataset.id); const deleteButton = event.target.closest(".line-delete"); if (!deleteButton) return; const { layout } = currentLayout(); layout.lines = layout.lines.filter(line => line.id !== deleteButton.dataset.id); saveLayout(layout); if (state.editingLineId === deleteButton.dataset.id) resetLineEditor(); renderPriceLines(); });
$("cancelLineEditBtn").addEventListener("click", resetLineEditor);
$("sortLinesBtn").addEventListener("click", sortPriceLines);
$("showAllLinesBtn").addEventListener("click", () => { const { layout } = currentLayout(); layout.lines.forEach(line => { line.visible = true; }); saveLayout(layout); renderPriceLines(); });
$("hideAllLinesBtn").addEventListener("click", () => { const { layout } = currentLayout(); layout.lines.forEach(line => { line.visible = false; }); saveLayout(layout); renderPriceLines(); });
$("clearLinesBtn").addEventListener("click", () => { const { layout } = currentLayout(); layout.lines = []; saveLayout(layout); resetLineEditor(); renderPriceLines(); });
$("verticalForm").addEventListener("submit", event => { event.preventDefault(); const requestedDate = $("verticalDate").value; if (!requestedDate) return showAlert("請輸入要標示的日期"); const resolvedTime = resolveVerticalTime(requestedDate); if (!resolvedTime) return showAlert("目前載入區間內找不到對應的 K 棒日期"); const { layout } = currentLayout(); const values = { requestedDate, exactTime: null, name: $("verticalName").value.trim(), ...formDrawSettings("vertical") }; rememberDrawSettings("vertical", values); if (state.editingVerticalId) { const item = layout.verticals.find(line => line.id === state.editingVerticalId); if (item) Object.assign(item, values); } else layout.verticals.unshift({ id: `${Date.now()}-${Math.random().toString(16).slice(2)}`, visible: true, ...values }); saveLayout(layout); renderVerticalLines(); resetVerticalEditor(); hideAlert(); });
$("verticalList").addEventListener("change", event => { if (!event.target.matches(".vertical-visible")) return; const { layout } = currentLayout(); const item = layout.verticals.find(line => line.id === event.target.dataset.id); if (!item) return; item.visible = event.target.checked; saveLayout(layout); renderVerticalLines(); });
$("verticalList").addEventListener("click", event => { const editButton = event.target.closest(".vertical-edit"); if (editButton) return editVerticalLine(editButton.dataset.id); const deleteButton = event.target.closest(".vertical-delete"); if (!deleteButton) return; const { layout } = currentLayout(); layout.verticals = layout.verticals.filter(line => line.id !== deleteButton.dataset.id); saveLayout(layout); if (state.editingVerticalId === deleteButton.dataset.id) resetVerticalEditor(); renderVerticalLines(); });
$("cancelVerticalEditBtn").addEventListener("click", resetVerticalEditor);
$("sortVerticalsBtn").addEventListener("click", sortVerticalLines);
$("showAllVerticalsBtn").addEventListener("click", () => { const { layout } = currentLayout(); layout.verticals.forEach(line => { line.visible = true; }); saveLayout(layout); renderVerticalLines(); });
$("hideAllVerticalsBtn").addEventListener("click", () => { const { layout } = currentLayout(); layout.verticals.forEach(line => { line.visible = false; }); saveLayout(layout); renderVerticalLines(); });
$("clearVerticalsBtn").addEventListener("click", () => { const { layout } = currentLayout(); layout.verticals = []; saveLayout(layout); resetVerticalEditor(); renderVerticalLines(); });
$("refreshBtn").addEventListener("click", () => loadChart(true));
$("market").addEventListener("change", saveSelectors); $("interval").addEventListener("change", saveSelectors);
$("startDate").addEventListener("change", saveSelectors); $("endDate").addEventListener("change", saveSelectors);
$("fitBtn").addEventListener("click", applyVisibleRange);
$("downloadChartBtn").addEventListener("click", downloadChartScreenshot);
$("drawHorizontalBtn").addEventListener("click", () => setDrawMode("horizontal"));
$("drawVerticalBtn").addEventListener("click", () => setDrawMode("vertical"));
$("updateTaiexKbar").addEventListener("change", saveSelectors);
$("updateFuturesKbar").addEventListener("change", saveSelectors);
$("updateKbarBtn").addEventListener("click", startKbarUpdate);
$("settingsToggleBtn").addEventListener("click", () => setSettingsCollapsed(!state.settingsCollapsed));
$("expandBtn").addEventListener("click", () => { state.expanded = !state.expanded; $("klinePanel").classList.toggle("panel-expanded", state.expanded); document.body.classList.toggle("panel-open", state.expanded); $("expandBtn").querySelector("i").className = state.expanded ? "bi bi-x-lg" : "bi bi-arrows-fullscreen"; setTimeout(() => state.chart?.resize($("klineChart").clientWidth, $("klineChart").clientHeight), 220); });
document.addEventListener("keydown", event => { if (event.key !== "Escape") return; if (state.drawMode) setDrawMode(state.drawMode); else if (state.expanded) $("expandBtn").click(); });

function selectSettingsTab(selected) {
  for (const [name, tab, panel] of [['subplot', 'subplotTab', 'subplotSettings'], ['annotation', 'annotationTab', 'annotationSettings']]) {
    $(tab).setAttribute('aria-selected', String(name === selected));
    $(panel).hidden = name !== selected;
  }
  const saved = readStorage(); saved.settingsTab = selected; writeStorage(saved);
}
function restoreSettingSections() {
  const sections = readStorage().settingSections || {};
  for (const element of document.querySelectorAll('[data-setting-section]')) element.open = Boolean(sections[element.dataset.settingSection]);
}
for (const element of document.querySelectorAll('[data-setting-section]')) element.addEventListener('toggle', () => {
  const saved = readStorage(); saved.settingSections ||= {};
  saved.settingSections[element.dataset.settingSection] = element.open; writeStorage(saved);
});
restoreSettingSections();
$('subplotTab').addEventListener('click', () => selectSettingsTab('subplot'));
$('annotationTab').addEventListener('click', () => selectSettingsTab('annotation'));
selectSettingsTab(readStorage().settingsTab === 'annotation' ? 'annotation' : 'subplot');
$('volumeVisible').checked = readStorage().volumeVisible !== false;
$('volumeVisible').addEventListener('change', () => {
  const saved = readStorage(); saved.volumeVisible = $('volumeVisible').checked; writeStorage(saved);
  if (!state.payload || !state.chart) return;
  const range = state.chart.timeScale().getVisibleLogicalRange();
  createChart(state.payload); renderDif(); renderMas(); renderPriceLines(); renderVerticalLines();
  if (range) state.chart.timeScale().setVisibleLogicalRange(range); else applyVisibleRange();
  updateLatestLegend();
});

$('difVisible').checked = Boolean(readStorage().difVisible);
$('difVisible').addEventListener('change', () => {
  const saved = readStorage(); saved.difVisible = $('difVisible').checked; writeStorage(saved);
  renderDif(); updateLatestLegend();
});
for (const id of ['market', 'startDate', 'endDate']) initialSelectors[id] = $(id).value;
allIntervalOptions.push(...[...$('interval').options].map(option => option.cloneNode(true)));
restoreSelectors();
$('dailyScaleTab').addEventListener('click', () => switchScale('daily'));
$('intradayScaleTab').addEventListener('click', () => switchScale('intraday'));
const chartFrame = $("klineChartFrame");
const savedChartHeight = Number(readStorage().chartHeight);
if (savedChartHeight >= 480 && savedChartHeight <= 1200) chartFrame.style.height = `${savedChartHeight}px`;
let lastChartHeight = Math.round(chartFrame.getBoundingClientRect().height);
new ResizeObserver(() => {
  if (state.expanded) return;
  const height = Math.round(chartFrame.getBoundingClientRect().height);
  if (height < 480 || height > 1200 || Math.abs(height - lastChartHeight) < 2) return;
  lastChartHeight = height;
  const saved = readStorage(); saved.chartHeight = height; writeStorage(saved);
  state.levelLabels?.schedule();
}).observe(chartFrame);
setSettingsCollapsed(Boolean(readStorage().settingsCollapsed), false); loadChart(); pollKbarUpdateStatus();

function updatePresetControls() {
  const enabled = $('market').value === 'tx_full' && $('interval').value === 'day';
  $('presetLevelControls').classList.toggle('d-none', !enabled);
  const meta = state.payload?.meta;
  const ready = enabled && meta?.market_key === 'tx_full' && meta?.interval_key === 'day';
  const select = $('presetLevelDate'), previous = select.value;
  select.replaceChildren();
  if (ready) {
    for (const bar of state.payload.bars.filter(bar => bar.time >= meta.display_start_date && bar.time <= meta.display_end_date)) {
      const option = document.createElement('option'); option.value = bar.time; option.textContent = bar.time;
      select.appendChild(option);
    }
    select.value = [...select.options].some(option => option.value === previous) ? previous : select.options[select.options.length - 1]?.value || '';
  }
  $('importPresetLevelsBtn').disabled = !ready || !select.value;
  $('importPresetLevelsBtn').title = ready ? '匯入指定日期的9條水平點位' : '請先按重新整理載入台指期全日日K';
}
async function importPresetLevels() {
  const payload = state.payload, meta = payload?.meta;
  if ($('market').value !== 'tx_full' || $('interval').value !== 'day' || meta?.market_key !== 'tx_full' || meta?.interval_key !== 'day') return showAlert('請先重新整理台指期全日日 K');
  const date = $('presetLevelDate').value;
  if (!date) return showAlert('請選擇點位日期');
  $('importPresetLevelsBtn').disabled = true;
  try {
    const qs = new URLSearchParams({ date, start_date: meta.display_start_date, end_date: meta.display_end_date });
    const result = await requestJson(`/api/kline/preset-levels?${qs}`);
    if (state.payload !== payload || $('market').value !== 'tx_full' || $('interval').value !== 'day') return;
    const { layout } = currentLayout();
    const prefix = `preset:${date}:`;
    // Update only this date's imported lines; retain manual and other-date lines.
    layout.lines = [...result.levels, ...layout.lines.filter(line => !String(line.id).startsWith(prefix))];
    saveLayout(layout); renderPriceLines();
    showAlert(`已匯入 ${date} 的 ${result.levels.length} 條水平線。${result.missing.length ? '缺少資料，略過：' + result.missing.join('、') : ''}`, result.missing.length ? 'warning' : 'success');
  } catch (error) { showAlert(`匯入失敗：${error.message}`); }
  finally { updatePresetControls(); }
}
$('importPresetLevelsBtn').addEventListener('click', importPresetLevels);
for (const id of ['market', 'interval']) $(id).addEventListener('change', updatePresetControls);
updatePresetControls();
