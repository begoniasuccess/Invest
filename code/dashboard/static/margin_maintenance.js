(() => {
  const $ = id => document.getElementById(id);
  const labels = {
    date: "日期", open: "加權開盤", high: "加權最高", low: "加權最低", close: "加權收盤",
    margin_maintenance: "大盤融資維持率 (%)", margin_balance: "融資餘額 (張)",
    short_balance: "融券餘額 (張)", margin_balance_change: "融資餘額增減 (張)",
    short_balance_change: "融券餘額增減 (張)", margin_amount: "融資金額 (億元)",
    margin_amount_change: "融資金額增減 (億元)",
  };
  const numberFormat = new Intl.NumberFormat("zh-TW", { maximumFractionDigits: 2 });

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>'"]/g, char => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;",
    })[char]);
  }

  function showAlert(message, type = "danger") {
    const box = $("alertBox");
    box.className = `alert app-alert alert-${type}`;
    box.textContent = message;
  }

  function clearAlert() { $("alertBox").className = "alert app-alert d-none"; }
  function setLoading(active) { $("loadingOverlay").classList.toggle("d-none", !active); }

  function renderSummary(payload) {
    const latest = payload.summary.latest_maintenance;
    const items = [
      ["bi-calendar-check", "最新資料日", payload.summary.latest_date || "—", "blue"],
      ["bi-percent", "最新融資維持率", latest == null ? "—" : `${numberFormat.format(latest)}%`, "violet"],
      ["bi-list-ol", "區間資料筆數", numberFormat.format(payload.summary.row_count), "slate"],
      ["bi-layers", "可用指標", numberFormat.format(payload.series.length + (payload.has_price ? 1 : 0)), "green"],
    ];
    $("marginSummary").innerHTML = items.map(([icon, label, value, color]) => `
      <article class="metric-card metric-${color}"><span class="metric-icon"><i class="bi ${icon}"></i></span>
        <div><div class="metric-label">${label}</div><div class="metric-value">${escapeHtml(value)}</div></div></article>`).join("");
  }

  function axisName(prefix, index) { return index === 1 ? prefix : `${prefix}${index}`; }

  function renderChart(payload) {
    const dates = payload.rows.map(row => row.date);
    const panels = [];
    if (payload.has_price) panels.push({ key: "price", label: "加權指數", unit: "點", type: "candlestick" });
    panels.push(...payload.series);
    if (!panels.length) {
      $("marginChart").innerHTML = '<div class="margin-empty">所選期間沒有可繪製的資料</div>';
      return;
    }

    const rowHeights = panels.map(panel => panel.type === "candlestick" ? 2.3 : 1);
    const gap = 0.025;
    const usable = 1 - gap * (panels.length - 1);
    const unitHeight = usable / rowHeights.reduce((sum, value) => sum + value, 0);
    let top = 1;
    const traces = [];
    const layout = {
      margin: { l: 68, r: 70, t: 34, b: 42 }, paper_bgcolor: "#fff", plot_bgcolor: "#fff",
      hovermode: "x unified", showlegend: false, annotations: [],
    };

    panels.forEach((panel, offset) => {
      const index = offset + 1;
      const height = rowHeights[offset] * unitHeight;
      const bottom = top - height;
      const xRef = axisName("x", index);
      const yRef = axisName("y", index);
      const xLayout = axisName("xaxis", index);
      const yLayout = axisName("yaxis", index);
      layout[xLayout] = {
        domain: [0, 1], anchor: yRef, matches: index === 1 ? undefined : "x",
        type: "category", categoryorder: "array", categoryarray: dates,
        showticklabels: offset === panels.length - 1, showgrid: true, gridcolor: "#edf1f6",
        rangeslider: { visible: false }, zeroline: false,
      };
      layout[yLayout] = {
        domain: [Math.max(0, bottom), top], anchor: xRef, side: "right", fixedrange: false,
        showgrid: true, gridcolor: "#edf1f6", zerolinecolor: "#cbd5e1", tickfont: { size: 10 },
      };
      layout.annotations.push({
        x: 0, y: top - 0.012, xref: "paper", yref: "paper", text: `${panel.label}${panel.unit ? ` (${panel.unit})` : ""}`,
        showarrow: false, xanchor: "left", yanchor: "top", font: { size: 11, color: "#475569" },
      });

      if (panel.type === "candlestick") {
        traces.push({
          type: "candlestick", x: dates, open: payload.rows.map(row => row.open), high: payload.rows.map(row => row.high),
          low: payload.rows.map(row => row.low), close: payload.rows.map(row => row.close), xaxis: xRef, yaxis: yRef,
          increasing: { line: { color: "#ef4444" }, fillcolor: "#ef4444" },
          decreasing: { line: { color: "#22c55e" }, fillcolor: "#22c55e" }, name: panel.label,
        });
      } else if (panel.type === "bar") {
        const values = payload.rows.map(row => row[panel.key]);
        traces.push({
          type: "bar", x: dates, y: values, xaxis: xRef, yaxis: yRef, name: panel.label,
          marker: { color: values.map(value => value == null ? "#cbd5e1" : value >= 0 ? "#ef4444" : "#22c55e") },
          hovertemplate: `%{y:,.2f} ${panel.unit}<extra></extra>`,
        });
      } else {
        traces.push({
          type: "scatter", mode: "lines", x: dates, y: payload.rows.map(row => row[panel.key]),
          xaxis: xRef, yaxis: yRef, name: panel.label, line: { color: panel.color, width: 1.6 },
          connectgaps: false, hovertemplate: `%{y:,.2f} ${panel.unit}<extra></extra>`,
        });
      }
      top = bottom - gap;
    });

    const height = Math.max(560, 260 + Math.max(0, panels.length - 1) * 145);
    Plotly.react("marginChart", traces, layout, { responsive: true, displaylogo: false, locale: "zh-TW" });
    $("marginChart").style.height = `${height}px`;
    Plotly.Plots.resize($("marginChart"));
    $("chartHint").textContent = `顯示 ${panels.length} 個資料面板；缺少的資料序列已自動略過`;
  }

  function renderTable(payload) {
    const table = $("marginTable");
    table.querySelector("thead").innerHTML = `<tr>${payload.columns.map(column => `<th>${escapeHtml(labels[column] || column)}</th>`).join("")}</tr>`;
    table.querySelector("tbody").innerHTML = [...payload.rows].reverse().map(row => `<tr>${payload.columns.map(column => {
      const value = row[column];
      const display = column === "date" || value == null ? (value ?? "—") : numberFormat.format(value);
      return `<td>${escapeHtml(display)}</td>`;
    }).join("")}</tr>`).join("");
    $("tableCount").textContent = `共 ${numberFormat.format(payload.rows.length)} 筆`;
  }

  async function loadData() {
    const start = $("startDate").value;
    const end = $("endDate").value;
    if (!start || !end) return showAlert("請選擇開始與結束日期");
    clearAlert(); setLoading(true); $("refreshBtn").disabled = true;
    try {
      const params = new URLSearchParams({ start_date: start, end_date: end });
      const response = await fetch(`/api/margin-maintenance?${params}`);
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || "資料讀取失敗");
      renderSummary(data); renderChart(data); renderTable(data);
      $("sourceBadge").innerHTML = `<i class="bi bi-database-check"></i> ${escapeHtml(data.period.start)}～${escapeHtml(data.period.end)} · 更新 ${escapeHtml(data.generated_at)}`;
      if (!data.rows.length) showAlert("所選期間沒有資料，請調整日期區間。", "warning");
    } catch (error) {
      showAlert(`讀取失敗：${error.message}`);
      $("sourceBadge").innerHTML = '<i class="bi bi-database-x"></i> 讀取失敗';
    } finally {
      setLoading(false); $("refreshBtn").disabled = false;
    }
  }

  $("refreshBtn").addEventListener("click", loadData);
  [$("startDate"), $("endDate")].forEach(input => input.addEventListener("keydown", event => {
    if (event.key === "Enter") loadData();
  }));
  loadData();
})();
