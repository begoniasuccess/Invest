(() => {
  const STORAGE_KEY = "investDashboard.navigation.v2";
  const LEGACY_STORAGE_KEY = "investDashboard.navigation.v1";
  const GROUPS = [
    { id: "tw", label: "台股" },
    { id: "us", label: "美股" },
  ];
  const ITEMS = [
    { id: "distribution", href: "/distribution", icon: "bi-bar-chart-line", label: "級距統計", group: "tw" },
    { id: "cycles", href: "/cycles", icon: "bi-calendar3", label: "週期統計", group: "tw" },
    { id: "night-bucket", href: "/night-bucket", icon: "bi-moon-stars", label: "夜盤分層分析", group: "tw" },
    { id: "next-day-session", href: "/next-day-session", icon: "bi-grid-3x3-gap", label: "隔日日盤研究", group: "tw" },
    { id: "raw-data", href: "/raw-data", icon: "bi-table", label: "原始資料", group: "tw" },
    { id: "kline", href: "/kline", icon: "bi-graph-up", label: "K 線分析", group: "tw" },
    { id: "yu-key-levels", href: "/yu-key-levels", icon: "bi-bullseye", label: "Yu 點位審核", group: "tw" },
    { id: "us-quick-view", href: "/us/quick-view", icon: "bi-grid-1x2", label: "快速閱覽", group: "us" },
    { id: "us-raw-data", href: "/us/raw-data", icon: "bi-table", label: "美股原始資料", group: "us" },
    { id: "us-kline", href: "/us/kline", icon: "bi-graph-up", label: "美股 K 線", group: "us" },
  ];

  function parseStored() {
    for (const key of [STORAGE_KEY, LEGACY_STORAGE_KEY]) {
      try {
        const value = JSON.parse(localStorage.getItem(key) || "null");
        if (value && typeof value === "object") return value;
      } catch { /* ignore broken localStorage */ }
    }
    return {};
  }

  function readSettings() {
    const raw = parseStored();
    const valid = new Set(ITEMS.map(item => item.id));
    const order = [...new Set(Array.isArray(raw.order) ? raw.order.filter(id => valid.has(id)) : [])];
    const quickViewWasMissing = !order.includes("us-quick-view");
    ITEMS.forEach(item => { if (!order.includes(item.id)) order.push(item.id); });
    if (quickViewWasMissing) {
      const quickIndex = order.indexOf("us-quick-view");
      if (quickIndex >= 0) order.splice(quickIndex, 1);
      const firstUsIndex = order.findIndex(id => ITEMS.find(item => item.id === id)?.group === "us");
      order.splice(firstUsIndex >= 0 ? firstUsIndex : order.length, 0, "us-quick-view");
    }
    const groupCollapsed = {
      tw: Boolean(raw.groupCollapsed?.tw),
      us: Boolean(raw.groupCollapsed?.us),
    };
    return { order, collapsed: Boolean(raw.collapsed), groupCollapsed };
  }

  function saveSettings(settings) { localStorage.setItem(STORAGE_KEY, JSON.stringify(settings)); }
  const settings = readSettings();

  // 首頁永遠進入使用者排序後的第一個頁籤。
  if (location.pathname === "/") {
    const first = ITEMS.find(item => item.id === settings.order[0]) || ITEMS[0];
    location.replace(first.href);
    return;
  }

  const globalUpdaterScript = document.createElement("script");
  globalUpdaterScript.src = "/static/global_update.js?v=20261027-nav-groups-v2";
  globalUpdaterScript.defer = true;
  document.head.append(globalUpdaterScript);

  document.addEventListener("DOMContentLoaded", () => {
    const nav = document.getElementById("sidebarNav");
    const collapseButton = document.getElementById("sidebarCollapseBtn");
    const mobileButton = document.getElementById("mobileMenuBtn");
    const backdrop = document.getElementById("sidebarBackdrop");
    const currentPage = document.body.dataset.page;
    // 舊模板仍保留這個文字；真正的兩大區塊標題改由 navigation.js 統一產生。
    const legacyLabel = document.querySelector("#appSidebar > .sidebar-label");
    if (legacyLabel) legacyLabel.hidden = true;

    function renderGroup(group) {
      const config = GROUPS.find(item => item.id === group);
      const ids = settings.order.filter(id => ITEMS.find(item => item.id === id)?.group === group);
      const collapsed = Boolean(settings.groupCollapsed[group]);
      const rows = ids.map((id, index) => {
        const item = ITEMS.find(candidate => candidate.id === id);
        return `<div class="nav-row">
          <a class="nav-item${id === currentPage ? " active" : ""}" href="${item.href}" title="${item.label}">
            <i class="bi ${item.icon}"></i><span>${item.label}</span>
          </a>
          <span class="nav-order-controls">
            <button type="button" data-move="up" data-id="${id}" ${index === 0 ? "disabled" : ""} title="向上移動"><i class="bi bi-chevron-up"></i></button>
            <button type="button" data-move="down" data-id="${id}" ${index === ids.length - 1 ? "disabled" : ""} title="向下移動"><i class="bi bi-chevron-down"></i></button>
          </span>
        </div>`;
      }).join("");
      return `<section class="nav-section${collapsed ? " is-collapsed" : ""}" data-nav-group="${group}">
        <button class="nav-section-header" type="button" data-toggle-group="${group}" aria-expanded="${String(!collapsed)}" title="${collapsed ? "展開" : "收合"}${config.label}">
          <span>${config.label}</span><i class="bi bi-chevron-${collapsed ? "right" : "down"}"></i>
        </button>
        <div class="nav-section-body">${rows}</div>
      </section>`;
    }

    function renderNavigation() {
      if (!nav) return;
      nav.innerHTML = GROUPS.map(group => renderGroup(group.id)).join("");
    }

    function setCollapsed(collapsed) {
      settings.collapsed = collapsed;
      document.body.classList.toggle("sidebar-collapsed", collapsed);
      collapseButton?.setAttribute("aria-label", collapsed ? "展開側欄" : "收合側欄");
      const icon = collapseButton?.querySelector("i");
      if (icon) icon.className = `bi ${collapsed ? "bi-layout-sidebar" : "bi-layout-sidebar-inset"}`;
    }

    function closeMobileMenu() { document.body.classList.remove("mobile-nav-open"); }
    renderNavigation();
    setCollapsed(settings.collapsed);

    nav?.addEventListener("click", event => {
      const groupButton = event.target.closest("button[data-toggle-group]");
      if (groupButton) {
        const group = groupButton.dataset.toggleGroup;
        settings.groupCollapsed[group] = !settings.groupCollapsed[group];
        saveSettings(settings);
        renderNavigation();
        return;
      }

      const button = event.target.closest("button[data-move]");
      if (!button) return;
      const item = ITEMS.find(candidate => candidate.id === button.dataset.id);
      if (!item) return;
      const groupIds = settings.order.filter(id => ITEMS.find(candidate => candidate.id === id)?.group === item.group);
      const groupIndex = groupIds.indexOf(button.dataset.id);
      const targetGroupIndex = button.dataset.move === "up" ? groupIndex - 1 : groupIndex + 1;
      if (groupIndex < 0 || targetGroupIndex < 0 || targetGroupIndex >= groupIds.length) return;
      const otherId = groupIds[targetGroupIndex];
      const index = settings.order.indexOf(button.dataset.id);
      const target = settings.order.indexOf(otherId);
      [settings.order[index], settings.order[target]] = [settings.order[target], settings.order[index]];
      saveSettings(settings); renderNavigation();
    });

    collapseButton?.addEventListener("click", () => {
      if (matchMedia("(max-width: 991.98px)").matches) return closeMobileMenu();
      setCollapsed(!settings.collapsed); saveSettings(settings);
    });
    mobileButton?.addEventListener("click", () => document.body.classList.add("mobile-nav-open"));
    backdrop?.addEventListener("click", closeMobileMenu);
    document.addEventListener("keydown", event => { if (event.key === "Escape") closeMobileMenu(); });

    // 所有頁面的狀態訊息都可手動關閉，並在停留數秒後自動收起。
    document.querySelectorAll(".app-alert").forEach(box => {
      let timer = null;
      let lastMessage = "";
      const enhance = () => {
        if (box.classList.contains("d-none")) { clearTimeout(timer); timer = null; lastMessage = ""; return; }
        const message = [...box.childNodes].filter(node => !(node.nodeType === 1 && node.classList?.contains("app-alert-close"))).map(node => node.textContent).join("").trim();
        if (!box.querySelector(".app-alert-close")) {
          const close = document.createElement("button");
          close.type = "button"; close.className = "app-alert-close"; close.title = "關閉通知"; close.setAttribute("aria-label", "關閉通知"); close.innerHTML = '<i class="bi bi-x-lg"></i>';
          close.addEventListener("click", () => { clearTimeout(timer); box.classList.add("d-none"); });
          box.append(close);
        }
        if (message && message !== lastMessage) {
          lastMessage = message; clearTimeout(timer);
          timer = setTimeout(() => box.classList.add("d-none"), box.classList.contains("alert-danger") ? 10000 : 6000);
        }
      };
      new MutationObserver(enhance).observe(box, { attributes: true, childList: true, subtree: true });
      enhance();
    });
  });
})();
