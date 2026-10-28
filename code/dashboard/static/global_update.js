(() => {
  const STORAGE_KEY = "investDashboard.globalUpdate.v1";
  const POLL_MS = 1500;
  const $ = id => document.getElementById(id);
  let catalog = null;
  let pollTimer = null;
  let settings = { sectionOrder: [], selected: {} };

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, ch => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  }

  function readSettings() {
    try {
      const raw = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}");
      return {
        sectionOrder: Array.isArray(raw.sectionOrder) ? raw.sectionOrder : [],
        selected: raw.selected && typeof raw.selected === "object" ? raw.selected : {},
      };
    } catch {
      return { sectionOrder: [], selected: {} };
    }
  }

  function saveSettings() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
  }

  async function requestJson(url, options = {}) {
    const response = await fetch(url, options);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || payload.message || `HTTP ${response.status}`);
    return payload;
  }

  function ensureShell() {
    if (!$("globalUpdateLauncher")) {
      const sidebar = $("appSidebar");
      const footer = sidebar?.querySelector(".sidebar-foot");
      const launcherHtml = `
        <button id="globalUpdateLauncher" class="global-update-launcher" type="button" aria-label="開啟資料更新中心" title="資料更新中心">
          <i class="bi bi-cloud-arrow-down"></i><span>資料更新</span><span id="globalUpdateLauncherDot" class="global-update-launcher-dot"></span>
        </button>`;
      if (footer) footer.insertAdjacentHTML("beforebegin", launcherHtml);
      else if (sidebar) sidebar.insertAdjacentHTML("beforeend", launcherHtml);
    }
    if ($("globalUpdateBackdrop")) return;
    document.body.insertAdjacentHTML("beforeend", `
      <div id="globalUpdateBackdrop" class="global-update-backdrop" hidden>
        <section id="globalUpdateModal" class="global-update-modal" role="dialog" aria-modal="true" aria-labelledby="globalUpdateTitle">
          <header class="global-update-header">
            <div><div class="global-update-kicker">DATA UPDATE CENTER</div><h2 id="globalUpdateTitle">資料更新中心</h2></div>
            <button id="globalUpdateClose" class="global-update-icon-btn" type="button" aria-label="關閉"><i class="bi bi-x-lg"></i></button>
          </header>
          <div class="global-update-body">
            <div id="globalUpdateStatus" class="global-update-status"><i class="bi bi-database-check"></i><span>讀取更新狀態…</span></div>
            <div class="global-update-help">勾選要執行的項目；拖曳區塊或用上下箭頭調整執行優先順序。</div>
            <div id="globalUpdateSections" class="global-update-sections"></div>
          </div>
          <footer class="global-update-footer">
            <label class="global-update-force"><input id="globalUpdateForce" class="form-check-input" type="checkbox"><span><strong>強制重新抓取</strong><small>忽略快取；只有懷疑資料尚未更新時再勾。</small></span></label>
            <div class="global-update-footer-actions">
              <span id="globalUpdateSelectionCount" class="global-update-selection-count"></span>
              <button id="globalUpdateStart" class="btn btn-primary" type="button"><i class="bi bi-play-fill me-1"></i>開始更新</button>
            </div>
          </footer>
        </section>
      </div>`);
  }

  function normalizeSettings() {
    if (!catalog) return;
    const validSections = new Set(catalog.sections.map(section => section.id));
    const normalized = [...new Set(settings.sectionOrder.filter(id => validSections.has(id)))];
    catalog.sections.forEach(section => { if (!normalized.includes(section.id)) normalized.push(section.id); });
    settings.sectionOrder = normalized;
    catalog.sections.forEach(section => section.items.forEach(item => {
      if (!(item.id in settings.selected)) settings.selected[item.id] = true;
    }));
    saveSettings();
  }

  function formatCache(item) {
    const refreshed = item.cache?.refreshed_at;
    if (!refreshed) return "尚無更新紀錄";
    const date = new Date(refreshed);
    if (Number.isNaN(date.getTime())) return `最近更新 ${refreshed.replace("T", " ")}`;
    return `最近更新 ${date.toLocaleString("zh-TW", { hour12: false })}`;
  }

  function orderedSections() {
    if (!catalog) return [];
    return settings.sectionOrder.map(id => catalog.sections.find(section => section.id === id)).filter(Boolean);
  }

  function renderSections() {
    const host = $("globalUpdateSections");
    if (!host || !catalog) return;
    const sections = orderedSections();
    host.innerHTML = sections.map((section, index) => {
      const checkedCount = section.items.filter(item => settings.selected[item.id]).length;
      const sectionChecked = checkedCount === section.items.length;
      const sectionMixed = checkedCount > 0 && !sectionChecked;
      return `<article class="global-update-section" draggable="true" data-section-id="${escapeHtml(section.id)}">
        <div class="global-update-section-head">
          <span class="global-update-drag" title="拖曳排序"><i class="bi bi-grip-vertical"></i></span>
          <label class="global-update-section-check">
            <input class="form-check-input" type="checkbox" data-section-toggle="${escapeHtml(section.id)}" ${sectionChecked ? "checked" : ""} data-mixed="${sectionMixed ? "1" : "0"}>
            <span class="global-update-section-icon"><i class="bi ${escapeHtml(section.icon)}"></i></span>
            <span><strong>${escapeHtml(section.name)}</strong><small>${escapeHtml(section.description)}</small></span>
          </label>
          <div class="global-update-order-buttons">
            <button type="button" data-move="up" data-section="${escapeHtml(section.id)}" ${index === 0 ? "disabled" : ""} title="往前執行"><i class="bi bi-chevron-up"></i></button>
            <button type="button" data-move="down" data-section="${escapeHtml(section.id)}" ${index === sections.length - 1 ? "disabled" : ""} title="往後執行"><i class="bi bi-chevron-down"></i></button>
          </div>
        </div>
        <div class="global-update-items">
          ${section.items.map(item => `<label class="global-update-item">
            <input class="form-check-input" type="checkbox" data-module="${escapeHtml(item.id)}" ${settings.selected[item.id] ? "checked" : ""}>
            <span><strong>${escapeHtml(item.name)}</strong><small>${escapeHtml(formatCache(item))} · 快取 ${escapeHtml(item.ttl_minutes)} 分鐘</small></span>
          </label>`).join("")}
        </div>
      </article>`;
    }).join("");

    host.querySelectorAll("[data-section-toggle]").forEach(input => {
      input.indeterminate = input.dataset.mixed === "1";
    });
    bindSectionEvents();
    updateSelectionCount();
  }

  function bindSectionEvents() {
    const host = $("globalUpdateSections");
    if (!host) return;

    host.querySelectorAll("input[data-module]").forEach(input => input.addEventListener("change", () => {
      settings.selected[input.dataset.module] = input.checked;
      saveSettings(); renderSections();
    }));

    host.querySelectorAll("input[data-section-toggle]").forEach(input => input.addEventListener("change", () => {
      const section = catalog.sections.find(candidate => candidate.id === input.dataset.sectionToggle);
      section?.items.forEach(item => { settings.selected[item.id] = input.checked; });
      saveSettings(); renderSections();
    }));

    host.querySelectorAll("button[data-move]").forEach(button => button.addEventListener("click", () => {
      moveSection(button.dataset.section, button.dataset.move === "up" ? -1 : 1);
    }));

    let dragging = null;
    host.querySelectorAll(".global-update-section").forEach(card => {
      card.addEventListener("dragstart", event => {
        dragging = card.dataset.sectionId;
        card.classList.add("is-dragging");
        event.dataTransfer.effectAllowed = "move";
      });
      card.addEventListener("dragend", () => { dragging = null; card.classList.remove("is-dragging"); });
      card.addEventListener("dragover", event => { event.preventDefault(); card.classList.add("is-drag-over"); });
      card.addEventListener("dragleave", () => card.classList.remove("is-drag-over"));
      card.addEventListener("drop", event => {
        event.preventDefault(); card.classList.remove("is-drag-over");
        const target = card.dataset.sectionId;
        if (!dragging || dragging === target) return;
        const order = settings.sectionOrder.filter(id => id !== dragging);
        order.splice(order.indexOf(target), 0, dragging);
        settings.sectionOrder = order; saveSettings(); renderSections();
      });
    });
  }

  function moveSection(sectionId, delta) {
    const index = settings.sectionOrder.indexOf(sectionId);
    const target = index + delta;
    if (index < 0 || target < 0 || target >= settings.sectionOrder.length) return;
    [settings.sectionOrder[index], settings.sectionOrder[target]] = [settings.sectionOrder[target], settings.sectionOrder[index]];
    saveSettings(); renderSections();
  }

  function selectedModules() {
    const modules = [];
    orderedSections().forEach(section => section.items.forEach(item => {
      if (settings.selected[item.id]) modules.push(item.id);
    }));
    return modules;
  }

  function updateSelectionCount() {
    const modules = selectedModules();
    if ($("globalUpdateSelectionCount")) $("globalUpdateSelectionCount").textContent = `已選 ${modules.length} 項`;
  }

  function openModal() {
    $("globalUpdateBackdrop").hidden = false;
    document.body.classList.add("global-update-open");
  }

  function closeModal() {
    $("globalUpdateBackdrop").hidden = true;
    document.body.classList.remove("global-update-open");
  }

  function renderStatus(status) {
    if (!status) return;
    const box = $("globalUpdateStatus");
    const launcher = $("globalUpdateLauncher");
    const dot = $("globalUpdateLauncherDot");
    const start = $("globalUpdateStart");
    if (!box || !launcher || !dot || !start) return;

    const running = Boolean(status.running);
    const failed = status.step === "failed";
    const pending = status.step === "pending";
    const completed = status.step === "completed";
    const icon = running ? "bi-arrow-repeat global-update-spin" : failed ? "bi-exclamation-triangle" : completed ? "bi-check-circle" : pending ? "bi-clock-history" : "bi-database-check";
    let progress = "";
    if (running && status.total_modules) progress = ` <small>${Math.min(status.current_index || 0, status.total_modules)}/${status.total_modules}</small>`;
    box.className = `global-update-status${failed ? " is-error" : completed ? " is-success" : running ? " is-running" : ""}`;
    box.innerHTML = `<i class="bi ${icon}"></i><span>${escapeHtml(status.message || "待命")}${progress}</span>`;
    launcher.classList.toggle("is-running", running);
    launcher.classList.toggle("is-error", failed);
    launcher.classList.toggle("is-success", completed && !running);
    dot.hidden = !(running || failed || completed);
    start.disabled = running;
    const sectionHost = $("globalUpdateSections");
    sectionHost?.classList.toggle("is-locked", running);
    sectionHost?.querySelectorAll("input").forEach(node => { node.disabled = running; });
    $("globalUpdateForce").disabled = running;
  }

  async function loadCatalog() {
    catalog = await requestJson("/api/report-update/catalog");
    normalizeSettings();
    renderSections();
    renderStatus(catalog.status);
    if (catalog.status?.running) startPolling();
  }

  async function pollStatus() {
    try {
      const status = await requestJson("/api/report-update/status");
      renderStatus(status);
      if (!status.running) {
        stopPolling();
        catalog = await requestJson("/api/report-update/catalog");
        normalizeSettings(); renderSections(); renderStatus(catalog.status);
        document.dispatchEvent(new CustomEvent("invest:update-finished", { detail: status }));
      }
    } catch (error) {
      stopPolling();
      renderStatus({ running: false, step: "failed", message: `無法取得更新狀態：${error.message}` });
    }
  }

  function startPolling() {
    if (!pollTimer) pollTimer = setInterval(pollStatus, POLL_MS);
  }
  function stopPolling() { if (pollTimer) clearInterval(pollTimer); pollTimer = null; }

  async function startUpdate() {
    const modules = selectedModules();
    if (!modules.length) {
      renderStatus({ running: false, step: "failed", message: "請至少勾選一個更新項目。" });
      return;
    }
    try {
      const status = await requestJson("/api/report-update", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ modules, force: $("globalUpdateForce").checked }),
      });
      renderStatus(status); startPolling(); await pollStatus();
    } catch (error) {
      renderStatus({ running: false, step: "failed", message: `啟動更新失敗：${error.message}` });
    }
  }

  async function init() {
    settings = readSettings();
    ensureShell();
    $("globalUpdateLauncher").addEventListener("click", openModal);
    $("globalUpdateClose").addEventListener("click", closeModal);
    $("globalUpdateBackdrop").addEventListener("click", event => { if (event.target === $("globalUpdateBackdrop")) closeModal(); });
    $("globalUpdateStart").addEventListener("click", startUpdate);
    document.addEventListener("keydown", event => { if (event.key === "Escape" && !$("globalUpdateBackdrop").hidden) closeModal(); });
    try { await loadCatalog(); }
    catch (error) { renderStatus({ running: false, step: "failed", message: `更新中心載入失敗：${error.message}` }); }
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init, { once: true });
  else init();
})();
