(() => {
  const NY_ZONE = "America/New_York";
  const TW_ZONE = "Asia/Taipei";

  function partsInZone(instant, timeZone) {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
    }).formatToParts(instant);
    return Object.fromEntries(parts.filter(part => part.type !== "literal").map(part => [part.type, Number(part.value)]));
  }

  function zonedDateTimeToUtc(dateText, hour, minute, timeZone) {
    const match = String(dateText || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!match) return null;
    const desired = {
      year: Number(match[1]), month: Number(match[2]), day: Number(match[3]),
      hour: Number(hour), minute: Number(minute), second: 0,
    };
    const desiredUtc = Date.UTC(desired.year, desired.month - 1, desired.day, desired.hour, desired.minute, 0);
    let guess = desiredUtc;
    for (let iteration = 0; iteration < 3; iteration += 1) {
      const actual = partsInZone(new Date(guess), timeZone);
      const actualUtc = Date.UTC(actual.year, actual.month - 1, actual.day, actual.hour, actual.minute, actual.second);
      guess += desiredUtc - actualUtc;
    }
    return new Date(guess);
  }

  function formatInZone(instant, timeZone) {
    if (!(instant instanceof Date) || Number.isNaN(instant.getTime())) return "—";
    return new Intl.DateTimeFormat("zh-TW", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    }).format(instant);
  }

  function marketDateTooltip(dateText) {
    const close = zonedDateTimeToUtc(dateText, 16, 0, NY_ZONE);
    if (!close) return "美股日期採 America/New_York（美東）交易日。";
    const ny = formatInZone(close, NY_ZONE);
    const tw = formatInZone(close, TW_ZONE);
    return `美東交易日 ${dateText}；美股收盤 ${ny}（America/New_York）＝台灣 ${tw}（Asia/Taipei）。夏令／冬令時間已自動換算。`;
  }

  function taipeiTimestampTooltip(value) {
    const match = String(value || "").match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}):(\d{2})/);
    if (!match) return "此更新時間採台灣時間（Asia/Taipei）。";
    const instant = zonedDateTimeToUtc(match[1], Number(match[2]), Number(match[3]), TW_ZONE);
    if (!instant) return "此更新時間採台灣時間（Asia/Taipei）。";
    return `台灣 ${formatInZone(instant, TW_ZONE)}（Asia/Taipei）＝美東 ${formatInZone(instant, NY_ZONE)}（America/New_York）。夏令／冬令時間已自動換算。`;
  }

  function icon(title) {
    return `<i class="bi bi-clock-history timezone-hint" tabindex="0" aria-label="${title}" title="${title}"></i>`;
  }

  function annotateDateLabels() {
    for (const id of ["startDate", "endDate", "verticalDate"]) {
      const label = document.querySelector(`label[for="${id}"]`);
      if (!label || label.querySelector(".timezone-hint")) continue;
      const title = "日期採美東交易日（America/New_York）；將游標移到行情日期可查看對應台灣收盤時間。";
      label.insertAdjacentHTML("beforeend", ` ${icon(title)}`);
    }
    const periodTitle = document.querySelector(".filter-section-period .filter-section-title");
    if (periodTitle && !periodTitle.querySelector(".timezone-hint")) {
      const title = "此頁日期範圍採美東交易日（America/New_York）。";
      periodTitle.insertAdjacentHTML("beforeend", ` ${icon(title)}`);
    }
  }

  window.USMarketTime = {
    annotateDateLabels,
    icon,
    marketDateTooltip,
    taipeiTimestampTooltip,
  };

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", annotateDateLabels);
  else annotateDateLabels();
})();
