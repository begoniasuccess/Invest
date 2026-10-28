(() => {
  const STORAGE_KEY = "investDashboard.usQuickView.v2";
  const LEGACY_STORAGE_KEY = "investDashboard.usQuickView.v1";
  const markets = Array.isArray(window.US_QUICK_MARKETS) ? window.US_QUICK_MARKETS : [];
  const validMarketKeys = new Set(markets.map(item => item.key));
  const validIntervals = new Set(["day", "week", "month", "year"]);
  const validColumns = new Set([1, 2, 3]);
  const charts = new Map();
  const responseCache = new Map();
  let requestId = 0;
  let payload = null;
  let dragMarketKey = null;

  const $ = id => document.getElementById(id);
  const formatPrice = value => Number(value).toLocaleString("zh-TW", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const formatPct = value => `${value >= 0 ? "+" : ""}${Number(value).toFixed(2)}%`;
  const today = () => new Date().toISOString().slice(0, 10);

  function parseStored() {
    for (const key of [STORAGE_KEY, LEGACY_STORAGE_KEY]) {
      try {
        const value = JSON.parse(localStorage.getItem(key) || "null");
        if (value && typeof value === "object") return value;
      } catch { /* ignore */ }
    }
    return {};
  }

  function normalizeOrder(order) {
    const result = [...new Set(Array.isArray(order) ? order.filter(key => validMarketKeys.has(key)) : [])];
    markets.forEach(item => { if (!result.includes(item.key)) result.push(item.key); });
    return result;
  }

  function readState() {
    const raw = parseStored();
    const selected = Array.isArray(raw.markets) ? raw.markets.filter(key => validMarketKeys.has(key)) : markets.map(item => item.key);
    const columns = Number(raw.columns);
    return {
      interval: validIntervals.has(raw.interval) ? raw.interval : "day",
      markets: [...new Set(selected)],
      order: normalizeOrder(raw.order),
      columns: validColumns.has(columns) ? columns : 3,
      startDate: /^\d{4}-\d{2}-\d{2}$/.test(raw.startDate || "") ? raw.startDate : (window.US_QUICK_DEFAULT_START_DATE || "2025-01-01"),
      endDate: /^\d{4}-\d{2}-\d{2}$/.test(raw.endDate || "") ? raw.endDate : (window.US_QUICK_DEFAULT_END_DATE || today()),
    };
  }

  const state = readState();
  function saveState() { localStorage.setItem(STORAGE_KEY, JSON.stringify(state)); }
  function cacheKey() { return `${state.interval}|${state.startDate}|${state.endDate}`; }

  function showAlert(message, danger = true) {
    const box = $("alertBox");
    box.className = `alert app-alert ${danger ? "alert-danger" : "alert-success"}`;
    box.textContent = message;
  }

  function hideAlert() { $("alertBox").className = "alert app-alert d-none"; }

  function setLoading(loading) {
    $("refreshQuickBtn").disabled = loading;
    $("applyQuickDates").disabled = loading;
    $("refreshQuickBtn").innerHTML = loading
      ? '<span class="spinner-border spinner-border-sm me-2"></span>讀取中'
      : '<i class="bi bi-arrow-clockwise me-2"></i>重新整理';
    $("quickGrid").classList.toggle("is-loading", loading);
  }

  function reorderSettingsOptions() {
    const grid = $("marketOptionsGrid");
    if (!grid) return;
    state.order.forEach(key => {
      const item = grid.querySelector(`[data-market-option="${CSS.escape(key)}"]`);
      if (item) grid.append(item);
    });
    state.order.forEach((key, index) => {
      const item = grid.querySelector(`[data-market-option="${CSS.escape(key)}"]`);
      item?.querySelector('[data-order-move="up"]')?.toggleAttribute("disabled", index === 0);
      item?.querySelector('[data-order-move="down"]')?.toggleAttribute("disabled", index === state.order.length - 1);
    });
  }

  function applyControlState() {
    document.querySelectorAll(".us-quick-interval-btn").forEach(button => {
      const active = button.dataset.interval === state.interval;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
    });
    document.querySelectorAll(".us-quick-market-check").forEach(input => {
      input.checked = state.markets.includes(input.value);
    });
    document.querySelectorAll("[data-columns]").forEach(button => {
      const active = Number(button.dataset.columns) === state.columns;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
    });
    $("quickStartDate").value = state.startDate;
    $("quickEndDate").value = state.endDate;
    $("selectionSummary").textContent = `顯示 ${state.markets.length} / ${markets.length}｜每列最多 ${state.columns}`;
    $("quickGrid").dataset.columns = String(state.columns);
    reorderSettingsOptions();
  }

  function destroyCharts() {
    charts.forEach(item => {
      item.observer?.disconnect();
      item.chart?.remove();
    });
    charts.clear();
  }

  function latestClass(change) {
    if (!Number.isFinite(Number(change)) || Number(change) === 0) return "flat";
    return Number(change) > 0 ? "up" : "down";
  }

  function moveMarket(key, direction) {
    const index = state.order.indexOf(key);
    const target = direction === "up" ? index - 1 : index + 1;
    if (index < 0 || target < 0 || target >= state.order.length) return;
    [state.order[index], state.order[target]] = [state.order[target], state.order[index]];
    saveState();
    applyControlState();
    render();
  }

  function moveMarketBefore(sourceKey, targetKey) {
    if (!sourceKey || !targetKey || sourceKey === targetKey) return;
    const next = state.order.filter(key => key !== sourceKey);
    const targetIndex = next.indexOf(targetKey);
    if (targetIndex < 0) return;
    next.splice(targetIndex, 0, sourceKey);
    state.order = normalizeOrder(next);
    saveState();
    applyControlState();
    render();
  }

  function createCard(market, dataset) {
    const meta = dataset.meta;
    const changeClass = latestClass(meta.change);
    const changeText = Number.isFinite(Number(meta.change_pct)) ? formatPct(meta.change_pct) : "—";
    const card = document.createElement("article");
    card.className = "panel-card us-quick-card";
    card.dataset.market = market.key;
    card.innerHTML = `
      <header class="us-quick-card-head" title="可拖曳調整圖表順序">
        <div class="us-quick-card-title-wrap">
          <span class="us-quick-drag-handle" aria-hidden="true"><i class="bi bi-grip-vertical"></i></span>
          <div class="us-quick-card-title"><span class="panel-kicker">${market.symbol}</span><h2>${market.name}</h2></div>
        </div>
        <div class="us-quick-last ${changeClass}"><strong>${formatPrice(meta.latest_close)}</strong><span>${changeText}</span><small>${meta.latest_date}</small></div>
      </header>
      <div class="us-quick-ohlc">最新：O ${formatPrice(dataset.bars.at(-1).open)}　H ${formatPrice(dataset.bars.at(-1).high)}　L ${formatPrice(dataset.bars.at(-1).low)}　C ${formatPrice(dataset.bars.at(-1).close)}</div>
      <div class="us-quick-chart" data-chart></div>`;

    const dragHeader = card.querySelector(".us-quick-card-head");
    dragHeader.draggable = true;
    dragHeader.addEventListener("dragstart", event => {
      dragMarketKey = market.key;
      card.classList.add("is-dragging");
      event.dataTransfer.effectAllowed = "move";
      event.dataTransfer.setData("text/plain", market.key);
    });
    dragHeader.addEventListener("dragend", () => {
      dragMarketKey = null;
      document.querySelectorAll(".us-quick-card").forEach(item => item.classList.remove("is-dragging", "is-drag-target"));
    });
    card.addEventListener("dragover", event => {
      if (!dragMarketKey || dragMarketKey === market.key) return;
      event.preventDefault();
      event.dataTransfer.dropEffect = "move";
      card.classList.add("is-drag-target");
    });
    card.addEventListener("dragleave", () => card.classList.remove("is-drag-target"));
    card.addEventListener("drop", event => {
      event.preventDefault();
      card.classList.remove("is-drag-target");
      moveMarketBefore(dragMarketKey || event.dataTransfer.getData("text/plain"), market.key);
    });

    $("quickGrid").append(card);

    const container = card.querySelector("[data-chart]");
    const LC = window.LightweightCharts;
    const chart = LC.createChart(container, {
      width: container.clientWidth,
      height: container.clientHeight,
      layout: { background: { type: "solid", color: "#ffffff" }, textColor: "#64748b", fontFamily: "Inter, system-ui, sans-serif", fontSize: 11 },
      grid: { vertLines: { color: "#f1f5f9" }, horzLines: { color: "#eef2f7" } },
      rightPriceScale: { borderColor: "#e2e8f0", scaleMargins: { top: .08, bottom: .22 } },
      timeScale: { borderColor: "#e2e8f0", timeVisible: false, secondsVisible: false, rightOffset: 2, barSpacing: 6, minBarSpacing: 2 },
      crosshair: { mode: LC.CrosshairMode.Normal },
      handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
      handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
    });
    const candle = chart.addSeries(LC.CandlestickSeries, {
      upColor: "#ef4444", downColor: "#22c55e", borderUpColor: "#ef4444", borderDownColor: "#22c55e", wickUpColor: "#ef4444", wickDownColor: "#22c55e",
      priceLineVisible: false, lastValueVisible: true, priceFormat: { type: "price", precision: 2, minMove: .01 },
    });
    candle.setData(dataset.bars.map(bar => ({ time: bar.time, open: bar.open, high: bar.high, low: bar.low, close: bar.close })));
    const volume = chart.addSeries(LC.HistogramSeries, {
      priceFormat: { type: "volume" }, priceScaleId: "", priceLineVisible: false, lastValueVisible: false,
    });
    volume.priceScale().applyOptions({ scaleMargins: { top: .82, bottom: 0 } });
    volume.setData(dataset.bars.map(bar => ({ time: bar.time, value: bar.volume, color: bar.close >= bar.open ? "rgba(239,68,68,.35)" : "rgba(34,197,94,.35)" })));
    chart.timeScale().fitContent();

    const ohlc = card.querySelector(".us-quick-ohlc");
    chart.subscribeCrosshairMove(param => {
      if (!param?.time) {
        const last = dataset.bars.at(-1);
        ohlc.textContent = `最新：O ${formatPrice(last.open)}　H ${formatPrice(last.high)}　L ${formatPrice(last.low)}　C ${formatPrice(last.close)}`;
        return;
      }
      const row = param.seriesData.get(candle);
      if (!row) return;
      ohlc.textContent = `${param.time}：O ${formatPrice(row.open)}　H ${formatPrice(row.high)}　L ${formatPrice(row.low)}　C ${formatPrice(row.close)}`;
    });

    const observer = new ResizeObserver(entries => {
      const rect = entries[0].contentRect;
      if (rect.width && rect.height) chart.resize(Math.floor(rect.width), Math.floor(rect.height));
    });
    observer.observe(container);
    charts.set(market.key, { chart, observer });
  }

  function selectedMarketsInOrder() {
    const byKey = new Map(markets.map(item => [item.key, item]));
    return state.order.filter(key => state.markets.includes(key)).map(key => byKey.get(key)).filter(Boolean);
  }

  function render() {
    destroyCharts();
    $("quickGrid").innerHTML = "";
    const selected = selectedMarketsInOrder();
    $("quickGrid").dataset.count = String(selected.length);
    $("quickGrid").dataset.columns = String(state.columns);
    $("quickEmptyState").classList.toggle("d-none", selected.length > 0);
    if (!selected.length || !payload) return;

    selected.forEach(market => {
      const dataset = payload.charts?.[market.key];
      if (dataset?.bars?.length) createCard(market, dataset);
    });
  }

  function validateDates(startDate, endDate) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(startDate) || !/^\d{4}-\d{2}-\d{2}$/.test(endDate)) return "請輸入有效日期";
    if (startDate > endDate) return "開始日期不可晚於結束日期";
    return "";
  }

  async function loadData(force = false) {
    const error = validateDates(state.startDate, state.endDate);
    if (error) return showAlert(error);

    const key = cacheKey();
    if (!force && responseCache.has(key)) {
      payload = responseCache.get(key);
      $("sourceBadge").innerHTML = `<i class="bi bi-lightning-charge"></i> ${payload.interval_name}｜頁面快取｜${state.startDate}～${state.endDate}`;
      hideAlert();
      render();
      return;
    }

    const currentRequest = ++requestId;
    setLoading(true); hideAlert();
    try {
      const params = new URLSearchParams({
        interval: state.interval,
        start_date: state.startDate,
        end_date: state.endDate,
        force: force ? "true" : "false",
      });
      const response = await fetch(`/api/us/quick-view?${params}`);
      const data = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`);
      if (currentRequest !== requestId) return;
      payload = data;
      responseCache.set(key, data);
      $("sourceBadge").innerHTML = `<i class="bi bi-database-check"></i> ${data.interval_name}｜${data.start_date}～${data.end_date}｜更新 ${data.generated_at}`;
      render();
    } catch (error) {
      if (currentRequest === requestId) showAlert(`讀取失敗：${error.message}`);
    } finally {
      if (currentRequest === requestId) setLoading(false);
    }
  }

  document.querySelectorAll(".us-quick-interval-btn").forEach(button => {
    button.addEventListener("click", () => {
      if (state.interval === button.dataset.interval) return;
      state.interval = button.dataset.interval;
      saveState(); applyControlState(); loadData(false);
    });
  });

  $("marketSettingsToggle").addEventListener("click", () => {
    const panel = $("marketSettingsPanel");
    panel.hidden = !panel.hidden;
    $("marketSettingsToggle").setAttribute("aria-expanded", String(!panel.hidden));
  });

  document.querySelectorAll(".us-quick-market-check").forEach(input => {
    input.addEventListener("change", () => {
      state.markets = [...document.querySelectorAll(".us-quick-market-check:checked")].map(item => item.value);
      saveState(); applyControlState(); render();
    });
  });

  $("columnButtons").addEventListener("click", event => {
    const button = event.target.closest("button[data-columns]");
    if (!button) return;
    const columns = Number(button.dataset.columns);
    if (!validColumns.has(columns)) return;
    state.columns = columns;
    saveState(); applyControlState(); render();
  });

  $("marketOptionsGrid").addEventListener("click", event => {
    const button = event.target.closest("button[data-order-move]");
    if (!button) return;
    moveMarket(button.dataset.market, button.dataset.orderMove);
  });

  $("selectAllMarkets").addEventListener("click", () => {
    state.markets = markets.map(item => item.key); saveState(); applyControlState(); render();
  });
  $("clearAllMarkets").addEventListener("click", () => {
    state.markets = []; saveState(); applyControlState(); render();
  });

  function applyDateInputs() {
    const startDate = $("quickStartDate").value;
    const endDate = $("quickEndDate").value;
    const error = validateDates(startDate, endDate);
    if (error) return showAlert(error);
    state.startDate = startDate;
    state.endDate = endDate;
    saveState(); applyControlState(); loadData(false);
  }

  $("applyQuickDates").addEventListener("click", applyDateInputs);
  $("quickStartDate").addEventListener("keydown", event => { if (event.key === "Enter") applyDateInputs(); });
  $("quickEndDate").addEventListener("keydown", event => { if (event.key === "Enter") applyDateInputs(); });
  $("refreshQuickBtn").addEventListener("click", () => loadData(true));

  applyControlState();
  loadData(false);
})();
