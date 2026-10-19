(() => {
  const STORAGE_KEY = "investDashboard.navigation.v1";
  const ITEMS = [
    { id: "distribution", href: "/distribution", icon: "bi-bar-chart-line", label: "級距統計" },
    { id: "cycles", href: "/cycles", icon: "bi-calendar3", label: "週期統計" },
    { id: "night-bucket", href: "/night-bucket", icon: "bi-moon-stars", label: "夜盤分層分析" },
    { id: "next-day-session", href: "/next-day-session", icon: "bi-grid-3x3-gap", label: "隔日日盤研究" },
    { id: "raw-data", href: "/raw-data", icon: "bi-table", label: "原始資料" },
    { id: "kline", href: "/kline", icon: "bi-graph-up", label: "K 線分析" },
    { id: "yu-key-levels", href: "/yu-key-levels", icon: "bi-bullseye", label: "Yu 點位審核" },
  ];

  function readSettings() {
    let raw = {};
    try { raw = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{}"); } catch { raw = {}; }
    const valid = new Set(ITEMS.map(item => item.id));
    const order = [...new Set(Array.isArray(raw.order) ? raw.order.filter(id => valid.has(id)) : [])];
    ITEMS.forEach(item => { if (!order.includes(item.id)) order.push(item.id); });
    return { order, collapsed: Boolean(raw.collapsed) };
  }

  function saveSettings(settings) { localStorage.setItem(STORAGE_KEY, JSON.stringify(settings)); }
  const settings = readSettings();

  // 首頁永遠進入使用者排序後的第一個頁籤。
  if (location.pathname === "/") {
    const first = ITEMS.find(item => item.id === settings.order[0]) || ITEMS[0];
    location.replace(first.href);
    return;
  }

  document.addEventListener("DOMContentLoaded", () => {
    const nav = document.getElementById("sidebarNav");
    const collapseButton = document.getElementById("sidebarCollapseBtn");
    const mobileButton = document.getElementById("mobileMenuBtn");
    const backdrop = document.getElementById("sidebarBackdrop");
    const currentPage = document.body.dataset.page;

    function renderNavigation() {
      nav.innerHTML = settings.order.map((id, index) => {
        const item = ITEMS.find(candidate => candidate.id === id);
        return `<div class="nav-row">
          <a class="nav-item${id === currentPage ? " active" : ""}" href="${item.href}" title="${item.label}">
            <i class="bi ${item.icon}"></i><span>${item.label}</span>
          </a>
          <span class="nav-order-controls">
            <button type="button" data-move="up" data-id="${id}" ${index === 0 ? "disabled" : ""} title="向上移動"><i class="bi bi-chevron-up"></i></button>
            <button type="button" data-move="down" data-id="${id}" ${index === settings.order.length - 1 ? "disabled" : ""} title="向下移動"><i class="bi bi-chevron-down"></i></button>
          </span>
        </div>`;
      }).join("");
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
      const button = event.target.closest("button[data-move]");
      if (!button) return;
      const index = settings.order.indexOf(button.dataset.id);
      const target = button.dataset.move === "up" ? index - 1 : index + 1;
      if (index < 0 || target < 0 || target >= settings.order.length) return;
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
