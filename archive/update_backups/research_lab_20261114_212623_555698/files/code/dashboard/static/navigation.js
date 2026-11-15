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
    { id: "margin-maintenance", href: "/margin-maintenance", icon: "bi-percent", label: "融資維持率", group: "tw" },
    { id: "raw-data", href: "/raw-data", icon: "bi-table", label: "原始資料", group: "tw" },
    { id: "kline", href: "/kline", icon: "bi-graph-up", label: "K 線圖", group: "tw" },
    { id: "replay", href: "/replay", icon: "bi-play-circle", label: "台股行情回放", group: "tw" },
    { id: "paper-trade", href: "/paper-trade", icon: "bi-journal-check", label: "模擬交易績效", group: "tw" },
    { id: "yu-key-levels", href: "/yu-key-levels", icon: "bi-bullseye", label: "Yu 點位分析", group: "tw" },
    { id: "paper-journal", href: "/paper-journal", icon: "bi-journal-text", label: "交易日記", group: "tw" },
    { id: "us-quick-view", href: "/us/quick-view", icon: "bi-grid-1x2", label: "快速閱覽", group: "us" },
    { id: "us-raw-data", href: "/us/raw-data", icon: "bi-table", label: "美股原始資料", group: "us" },
    { id: "us-kline", href: "/us/kline", icon: "bi-graph-up", label: "美股 K 線", group: "us" },
  ];

  const SUBGROUPS = [
    {id:"basic-analysis",label:"基礎分析",icon:"bi-bar-chart-line",children:["distribution","cycles","night-bucket"]},
    {id:"strategy-sim",label:"策略模擬",icon:"bi-diagram-3",children:["replay","paper-trade","paper-journal","yu-key-levels"]},
  ];
  const parentOf = id => SUBGROUPS.find(g=>g.children.includes(id));
  const navId = id => parentOf(id)?.id || id;
  const rowIds = group => [...new Set(settings.order.filter(id=>ITEMS.find(i=>i.id===id)?.group===group).map(navId))];
  const submenuCSS = document.createElement("style");
  submenuCSS.textContent = `
    .nav-subgroup-toggle { width:100%; border:0; background:none; text-align:left; }
    .nav-subgroup-toggle.active, .nav-subgroup-toggle:hover { background:rgba(59,130,246,.17); color:white; }
    body.sidebar-collapsed .nav-subgroup-toggle .sub-chevron { display:none; }
    .nav-subgroup-toggle .sub-chevron { margin-left:auto; flex:0 0 auto; font-size:.7rem; }
    .nav-subgroup-menu { display:none; position:fixed; z-index:80; width:220px; background:#172336; padding:7px; border:1px solid #35445b; border-radius:10px; box-shadow:0 8px 22px #0005; }
    .nav-subgroup.is-open .nav-subgroup-menu { display:block; }
    .nav-subgroup-menu .nav-item { padding:9px 12px; font-size:.86rem; }
    body.sidebar-collapsed .nav-subgroup-menu .nav-item span { display:inline; }
    body.sidebar-collapsed .nav-subgroup-menu .nav-item { justify-content:flex-start; padding:9px 12px; }
    @media(max-width:991.98px){body.sidebar-collapsed .nav-subgroup-toggle .sub-chevron{display:block;}.nav-subgroup-menu{position:static;width:auto;grid-column:1/-1;margin:2px 0 5px 12px;box-shadow:none;}.nav-subgroup.is-open .nav-subgroup-menu{display:block;}}
  `;
  document.head.append(submenuCSS);

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
    const replayWasMissing = !order.includes("replay");
    ITEMS.forEach(item => { if (!order.includes(item.id)) order.push(item.id); });
    if (quickViewWasMissing) {
      const quickIndex = order.indexOf("us-quick-view");
      if (quickIndex >= 0) order.splice(quickIndex, 1);
      const firstUsIndex = order.findIndex(id => ITEMS.find(item => item.id === id)?.group === "us");
      order.splice(firstUsIndex >= 0 ? firstUsIndex : order.length, 0, "us-quick-view");
    }
    if (replayWasMissing) {
      const replayIndex = order.indexOf("replay");
      if (replayIndex >= 0) order.splice(replayIndex, 1);
      order.splice(order.indexOf("kline") + 1, 0, "replay");
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
      const ids = rowIds(group);
      const collapsed = Boolean(settings.groupCollapsed[group]);
      const rows = ids.map((id, index) => {
        const sub=SUBGROUPS.find(g=>g.id===id);
        const item = sub || ITEMS.find(candidate => candidate.id === id);
        const links = sub ? `<button type="button" class="nav-item nav-subgroup-toggle${sub.children.includes(currentPage)?" active":""}" aria-expanded="false" title="${sub.label}"><i class="bi ${sub.icon}"></i><span>${sub.label}</span><i class="bi bi-chevron-right sub-chevron"></i></button><div class="nav-subgroup-menu" aria-label="${sub.label}">${sub.children.map(x=>{const c=ITEMS.find(i=>i.id===x);return `<a class="nav-item${x===currentPage?" active":""}" href="${c.href}" title="${c.label}"><i class="bi ${c.icon}"></i><span>${c.label}</span></a>`}).join("")}</div>` : `<a class="nav-item${id===currentPage?" active":""}" href="${item.href}" title="${item.label}"><i class="bi ${item.icon}"></i><span>${item.label}</span></a>`;
        return `<div class="nav-row${sub?" nav-subgroup":""}">
          ${links}

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
      const groups=Array.from(nav.querySelectorAll('.nav-subgroup'));
      function close(row){row.classList.remove('is-open');row.querySelector('button').setAttribute('aria-expanded','false');}
      function open(row){groups.forEach(r=>{if(r!==row)close(r)});row.classList.add('is-open');const btn=row.querySelector('button'), menu=row.querySelector('.nav-subgroup-menu');btn.setAttribute('aria-expanded','true');if(!matchMedia('(max-width:991.98px)').matches){const r=row.getBoundingClientRect();menu.style.left=(r.right-2)+'px';menu.style.top=Math.max(8,Math.min(r.top,innerHeight-menu.offsetHeight-8))+'px';}}
      groups.forEach(row=>{let timer;const btn=row.querySelector('button');row.addEventListener('mouseenter',()=>{if(matchMedia('(hover:hover) and (min-width:992px)').matches){clearTimeout(timer);open(row);}});row.addEventListener('mouseleave',()=>{if(matchMedia('(hover:hover) and (min-width:992px)').matches)timer=setTimeout(()=>{if(!row.contains(document.activeElement))close(row)},180);});btn.addEventListener('click',()=>{if(row.classList.contains('is-open'))close(row);else open(row);});row.addEventListener('focusout',e=>{if(!row.contains(e.relatedTarget))close(row);});row.addEventListener('keydown',e=>{if(e.key==='Escape'){close(row);btn.focus();}});});
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
      const id=button.dataset.id,sub=SUBGROUPS.find(g=>g.id===id),item=ITEMS.find(i=>i.id===id);
      const group=sub?'tw':item?.group;if(!group)return;
      const groupIds=rowIds(group), index=groupIds.indexOf(id), target=index+(button.dataset.move==='up'?-1:1);
      if(index<0||target<0||target>=groupIds.length)return;
      [groupIds[index],groupIds[target]]=[groupIds[target],groupIds[index]];
      const ordered=groupIds.flatMap(g=>settings.order.filter(x=>navId(x)===g));
      let n=0;settings.order=settings.order.map(x=>ITEMS.find(i=>i.id===x)?.group===group?ordered[n++]:x);
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
