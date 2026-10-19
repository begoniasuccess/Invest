/* Shared outside-the-plot labels for both Lightweight Charts pages. */
window.PriceLevelLabels = class PriceLevelLabels {
  constructor(chart, series, chartElement, rail) {
    this.chart = chart;
    this.series = series;
    this.chartElement = chartElement;
    this.rail = rail;
    this.storageKey = `investDashboard.levelLabelWidth.${rail.id}`;
    this.handle = document.createElement("div");
    this.handle.className = "level-label-resizer";
    this.handle.setAttribute("role", "separator");
    this.handle.setAttribute("aria-label", "調整水平點位標籤欄寬度");
    this.handle.setAttribute("aria-orientation", "vertical");
    this.handle.tabIndex = 0;
    this.handle.title = "左右拖曳調整標籤欄寬度；雙擊恢復預設";
    rail.before(this.handle);
    const stored = Number(localStorage.getItem(this.storageKey));
    this.desiredWidth = Number.isFinite(stored) && stored > 0 ? stored : 190;
    this.pointerDown = event => {
      if (event.button !== 0) return;
      event.preventDefault();
      this.dragStartX = event.clientX;
      this.dragStartWidth = this.rail.getBoundingClientRect().width;
      this.handle.classList.add("is-dragging");
      this.handle.setPointerCapture(event.pointerId);
    };
    this.pointerMove = event => {
      if (!this.handle.hasPointerCapture(event.pointerId)) return;
      this.setWidth(this.dragStartWidth + this.dragStartX - event.clientX, false);
    };
    this.pointerUp = event => {
      if (!this.handle.hasPointerCapture(event.pointerId)) return;
      this.handle.releasePointerCapture(event.pointerId);
      this.handle.classList.remove("is-dragging");
      localStorage.setItem(this.storageKey, String(Math.round(this.rail.getBoundingClientRect().width)));
    };
    this.keyDown = event => {
      if (!["ArrowLeft", "ArrowRight"].includes(event.key)) return;
      event.preventDefault();
      this.setWidth(this.rail.getBoundingClientRect().width + (event.key === "ArrowLeft" ? 20 : -20), true);
    };
    this.resetWidth = () => this.setWidth(190, true);
    this.handle.addEventListener("pointerdown", this.pointerDown);
    this.handle.addEventListener("pointermove", this.pointerMove);
    this.handle.addEventListener("pointerup", this.pointerUp);
    this.handle.addEventListener("pointercancel", this.pointerUp);
    this.handle.addEventListener("keydown", this.keyDown);
    this.handle.addEventListener("dblclick", this.resetWidth);
    this.items = [];
    this.placed = [];
    this.raf = 0;
    this.scrollHandler = () => this.schedule();
    rail.addEventListener("scroll", this.scrollHandler);
    this.rangeHandler = () => this.schedule();
    chart.timeScale().subscribeVisibleLogicalRangeChange(this.rangeHandler);
    this.resizeObserver = new ResizeObserver(() => {
      if (this.rail.parentElement.clientWidth !== this.layoutWidth) {
        this.layoutWidth = this.rail.parentElement.clientWidth;
        this.setWidth(this.desiredWidth, false);
      }
      this.schedule();
    });
    this.resizeObserver.observe(rail);
    this.resizeObserver.observe(chartElement);
    this.resizeObserver.observe(rail.parentElement);
    this.moveHandler = () => this.schedule();
    chartElement.addEventListener("wheel", this.moveHandler, { passive: true });
    chartElement.addEventListener("pointermove", this.moveHandler, { passive: true });
    this.setWidth(this.desiredWidth, false);
  }

  setWidth(requested, persist = true) {
    this.desiredWidth = requested;
    const available = this.rail.parentElement.clientWidth - this.handle.offsetWidth - 250;
    const width = Math.round(Math.max(115, Math.min(420, Math.max(115, available), requested)));
    this.rail.style.flexBasis = `${width}px`;
    this.rail.style.width = `${width}px`;
    this.handle.setAttribute("aria-valuenow", String(width));
    this.handle.setAttribute("aria-valuemin", "115");
    this.handle.setAttribute("aria-valuemax", String(Math.max(115, Math.min(420, available))));
    if (persist) localStorage.setItem(this.storageKey, String(width));
    this.schedule();
  }

  setItems(items) {
    this.items = items.filter(item => Number.isFinite(Number(item.price))).map(item => ({
      price: Number(item.price), name: String(item.name || "點位"), color: item.color || "#64748b",
    })).sort((a, b) => b.price - a.price);
    this.schedule();
  }

  schedule() {
    if (!this.raf) this.raf = requestAnimationFrame(() => { this.raf = 0; this.render(); });
  }

  layout() {
    const height = this.rail.clientHeight;
    const paneHeight = this.chart.panes()[0]?.getHeight?.() || Math.round(height * .72);
    const visible = this.items.map(item => ({ ...item, y: this.series.priceToCoordinate(item.price) }))
      .filter(item => item.y !== null && Number.isFinite(item.y) && item.y >= 0 && item.y <= paneHeight);
    const gap = 20, top = 4;
    const contentHeight = Math.max(height, visible.length * gap + top * 2);
    let rows = visible.map(item => ({ ...item, labelY: Math.max(top, Math.min(contentHeight - gap, item.y - gap / 2)) }));
    for (let i = 1; i < rows.length; i++) rows[i].labelY = Math.max(rows[i].labelY, rows[i - 1].labelY + gap);
    if (rows.length && rows.at(-1).labelY > contentHeight - gap - top) {
      rows[rows.length - 1].labelY = contentHeight - gap - top;
      for (let i = rows.length - 2; i >= 0; i--) rows[i].labelY = Math.min(rows[i].labelY, rows[i + 1].labelY - gap);
    }
    return { rows, contentHeight };
  }

  render() {
    if (!this.chartElement.isConnected) return;
    const { rows, contentHeight } = this.layout();
    const scroll = this.rail.scrollTop;
    this.rail.replaceChildren();
    const content = document.createElement("div");
    content.style.height = `${contentHeight}px`;
    content.style.position = "relative";
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("width", String(this.rail.clientWidth));
    svg.setAttribute("height", String(contentHeight));
    svg.setAttribute("viewBox", `0 0 ${this.rail.clientWidth} ${contentHeight}`);
    svg.style.height = `${contentHeight}px`;
    content.appendChild(svg);
    for (const item of rows) {
      const line = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
      line.setAttribute("points", `0,${item.y + scroll} 9,${item.y + scroll} 19,${item.labelY + 9}`);
      line.setAttribute("stroke", item.color);
      line.setAttribute("stroke-width", "1");
      line.setAttribute("fill", "none");
      svg.appendChild(line);
      const label = document.createElement("div");
      label.className = "level-label-item";
      label.style.top = `${item.labelY}px`;
      label.style.color = item.color;
      label.title = `${item.price.toLocaleString("zh-TW")} ${item.name}`;
      const price = document.createElement("strong");
      price.textContent = item.price.toLocaleString("zh-TW");
      const name = document.createElement("span");
      name.textContent = item.name;
      label.append(price, name);
      content.appendChild(label);
    }
    this.rail.appendChild(content);
    this.rail.scrollTop = scroll;
    this.placed = rows;
  }

  drawScreenshot(context, x, y, scale = 1) {
    this.render();
    const width = this.rail.clientWidth * scale, height = this.rail.clientHeight * scale;
    context.fillStyle = "#fff";
    context.fillRect(x, y, width, height);
    context.strokeStyle = "#e2e8f0";
    context.beginPath(); context.moveTo(x, y); context.lineTo(x, y + height); context.stroke();
    context.save(); context.beginPath(); context.rect(x, y, width, height); context.clip();
    context.font = `${11 * scale}px Inter, "Microsoft JhengHei", sans-serif`;
    const scroll = this.rail.scrollTop;
    for (const item of this.placed) {
      const labelY = item.labelY - scroll, cy = item.y;
      if (labelY < -20 || labelY > this.rail.clientHeight) continue;
      context.strokeStyle = item.color; context.lineWidth = Math.max(1, scale);
      context.beginPath(); context.moveTo(x, y + cy * scale); context.lineTo(x + 9 * scale, y + cy * scale);
      context.lineTo(x + 19 * scale, y + (labelY + 9) * scale); context.stroke();
      context.fillStyle = "#fff"; context.fillRect(x + 20 * scale, y + labelY * scale, width - 22 * scale, 19 * scale);
      context.fillStyle = item.color;
      const price = item.price.toLocaleString("zh-TW");
      context.fillText(price, x + 24 * scale, y + (labelY + 14) * scale);
      const offset = Math.max(62, context.measureText(price).width / scale + 30);
      const room = width - (offset + 28) * scale;
      let name = item.name;
      while (name && context.measureText(name).width > room) name = name.slice(0, -1);
      context.fillText(name, x + offset * scale, y + (labelY + 14) * scale);
    }
    context.restore();
  }

  destroy() {
    cancelAnimationFrame(this.raf);
    this.resizeObserver.disconnect();
    this.chart.timeScale().unsubscribeVisibleLogicalRangeChange(this.rangeHandler);
    this.chartElement.removeEventListener("wheel", this.moveHandler);
    this.chartElement.removeEventListener("pointermove", this.moveHandler);
    this.rail.removeEventListener("scroll", this.scrollHandler);
    this.handle.removeEventListener("pointerdown", this.pointerDown);
    this.handle.removeEventListener("pointermove", this.pointerMove);
    this.handle.removeEventListener("pointerup", this.pointerUp);
    this.handle.removeEventListener("pointercancel", this.pointerUp);
    this.handle.removeEventListener("keydown", this.keyDown);
    this.handle.removeEventListener("dblclick", this.resetWidth);
    this.handle.remove();
    this.rail.replaceChildren();
  }
};
