from __future__ import annotations

import argparse
import html
import json
from dataclasses import asdict
from datetime import date
from pathlib import Path

import pandas as pd

from backtest.futures_combination_exit_backtest import Settings as ExitSettings, _number, load_contract_map, load_day_ticks
from backtest.futures_retail_bounce_reentry_comparison_backtest import Settings, run_backtest


VARIANT_LABELS = {
    "CANCEL_DAY": "原規則",
    "DIRECT_ENTRY": "直接進場",
    "BOUNCE_1_6R": "反彈 1/6R",
}
VARIANT_COLORS = {
    "CANCEL_DAY": "#2563eb",
    "DIRECT_ENTRY": "#9333ea",
    "BOUNCE_1_6R": "#ea580c",
}
STATUS_LABELS = {
    "ENTERED": "有進場",
    "CANCELLED_PRE_ENTRY_STOP": "破失效價，取消",
    "TARGET_NOT_REACHED": "未觸及預設進場價",
    "NO_BOUNCE_TRIGGER": "跌破後未反彈達標",
    "NO_TIME_AFTER_TRIGGER": "反彈太晚，未進場",
    "MISSING_TICKS": "缺逐筆資料",
    "MISSING_REFERENCE": "缺參考資料",
}


def esc(value: object) -> str:
    return html.escape("" if pd.isna(value) else str(value))


def fmt(value: object, digits: int = 0) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    if pd.isna(number):
        return "—"
    return f"{number:,.{digits}f}"


def five_minute_bars(ticks) -> pd.DataFrame:
    if not ticks:
        return pd.DataFrame(columns=["at", "open", "high", "low", "close"])
    frame = pd.DataFrame({"at": [t.at for t in ticks], "price": [t.price for t in ticks]}).set_index("at")
    bars = frame["price"].resample("5min", origin="start_day", offset="0min").ohlc().dropna().reset_index()
    return bars


def svg_chart(bars: pd.DataFrame, rows: pd.DataFrame, width: int = 960, height: int = 500) -> str:
    if bars.empty:
        return '<div class="missing">缺少日盤逐筆資料，無法繪圖</div>'
    left, right, top, bottom = 72, 22, 24, 46
    plot_w, plot_h = width - left - right, height - top - bottom
    levels = []
    first = rows.iloc[0]
    for key in ("target_entry_price", "pre_entry_stop_price", "night_close", "previous_day_close"):
        value = _number(first.get(key))
        if value is not None:
            levels.append(value)
    for row in rows.itertuples(index=False):
        for key in ("entry_price", "exit_price", "bounce_low_price", "bounce_trigger_price"):
            value = _number(getattr(row, key, None))
            if value is not None:
                levels.append(value)
    ymin = min(float(bars.low.min()), *levels)
    ymax = max(float(bars.high.max()), *levels)
    pad = max((ymax - ymin) * .06, 5)
    ymin, ymax = ymin - pad, ymax + pad

    def y(price: float) -> float:
        return top + (ymax - price) / (ymax - ymin) * plot_h

    def x(index: int) -> float:
        return left + (index + .5) * plot_w / len(bars)

    def tx(value) -> float | None:
        if pd.isna(value):
            return None
        stamp = pd.Timestamp(value)
        start = pd.Timestamp(bars["at"].iloc[0])
        end = pd.Timestamp(bars["at"].iloc[-1]) + pd.Timedelta(minutes=5)
        ratio = (stamp - start).total_seconds() / max((end - start).total_seconds(), 1)
        return left + min(max(ratio, 0), 1) * plot_w

    out = [f'<svg class="chart" viewBox="0 0 {width} {height}" role="img">']
    out.append(f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="#fff"/>')
    for step in range(6):
        price = ymin + (ymax - ymin) * step / 5
        yy = y(price)
        out.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{width-right}" y2="{yy:.1f}" class="grid"/>')
        out.append(f'<text x="{left-8}" y="{yy+4:.1f}" text-anchor="end" class="axis">{price:,.0f}</text>')
    candle_w = max(2.2, plot_w / len(bars) * .55)
    for i, bar in bars.iterrows():
        xx, yo, yc, yh, yl = x(i), y(bar.open), y(bar.close), y(bar.high), y(bar.low)
        color = "#dc2626" if bar.close >= bar.open else "#16a34a"
        bar_at = pd.Timestamp(bar["at"])
        out.append(f'<g><title>{bar_at:%H:%M} O {bar.open:.0f} H {bar.high:.0f} L {bar.low:.0f} C {bar.close:.0f}</title>')
        out.append(f'<line x1="{xx:.1f}" y1="{yh:.1f}" x2="{xx:.1f}" y2="{yl:.1f}" stroke="{color}"/>')
        out.append(f'<rect x="{xx-candle_w/2:.1f}" y="{min(yo,yc):.1f}" width="{candle_w:.1f}" height="{max(abs(yc-yo),1):.1f}" fill="{color}"/></g>')
    for label, key, color, dash in (
        ("昨日日盤收", "previous_day_close", "#0891b2", "10 4 2 4"),
        ("夜盤收", "night_close", "#64748b", "3 4"),
        ("預設進場", "target_entry_price", "#0f172a", "8 4"),
        ("失效價", "pre_entry_stop_price", "#dc2626", "4 3"),
    ):
        value = _number(first.get(key))
        if value is None:
            continue
        yy = y(value)
        out.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{width-right}" y2="{yy:.1f}" stroke="{color}" stroke-width="1.4" stroke-dasharray="{dash}"/>')
        out.append(f'<text x="{left+4}" y="{yy-4:.1f}" fill="{color}" class="level">{label} {value:,.0f}</text>')
    for row in rows.itertuples(index=False):
        if getattr(row, "entry_status", "") != "ENTERED":
            continue
        variant = row.variant
        color, label = VARIANT_COLORS[variant], VARIANT_LABELS[variant]
        entry, exit_ = _number(getattr(row, "entry_price", None)), _number(getattr(row, "exit_price", None))
        xe, xx = tx(getattr(row, "entry_at", None)), tx(getattr(row, "exit_at", None))
        if entry is not None:
            yy = y(entry)
            out.append(f'<line class="variant variant-{variant}" x1="{left}" y1="{yy:.1f}" x2="{width-right}" y2="{yy:.1f}" stroke="{color}" stroke-width="1.7" opacity=".78"/>')
            if xe is not None:
                out.append(f'<circle class="variant variant-{variant}" cx="{xe:.1f}" cy="{yy:.1f}" r="5" fill="{color}"><title>{label}進場 {getattr(row,"entry_at","")} @ {entry:.0f}</title></circle>')
        if exit_ is not None:
            yy = y(exit_)
            out.append(f'<line class="variant variant-{variant}" x1="{left}" y1="{yy:.1f}" x2="{width-right}" y2="{yy:.1f}" stroke="{color}" stroke-width="1.4" stroke-dasharray="2 5" opacity=".7"/>')
            if xx is not None:
                out.append(f'<rect class="variant variant-{variant}" x="{xx-4:.1f}" y="{yy-4:.1f}" width="8" height="8" fill="{color}"><title>{label}出場 {getattr(row,"exit_at","")} @ {exit_:.0f}</title></rect>')
    for i, label in enumerate(("08:45", "09:15", "10:00", "11:00", "12:00", "13:00", "13:45")):
        stamp = pd.Timestamp(f"{pd.Timestamp(bars['at'].iloc[0]).date()} {label}")
        xx = tx(stamp)
        if xx is not None:
            out.append(f'<text x="{xx:.1f}" y="{height-15}" text-anchor="middle" class="axis">{label}</text>')
            if label == "09:15":
                out.append(f'<line x1="{xx:.1f}" y1="{top}" x2="{xx:.1f}" y2="{top+plot_h}" stroke="#f59e0b" stroke-dasharray="5 5"/>')
    out.append("</svg>")
    return "".join(out)


def result_table(rows: pd.DataFrame) -> str:
    cells = []
    for variant in VARIANT_LABELS:
        row = rows[rows.variant == variant].iloc[0]
        status = STATUS_LABELS.get(str(row.entry_status), str(row.entry_status))
        entered = row.entry_status == "ENTERED"
        detail = f"{pd.Timestamp(row.entry_at):%H:%M} @ {fmt(row.entry_price)} → {pd.Timestamp(row.exit_at):%H:%M} @ {fmt(row.exit_price)}" if entered else status
        net = f'{fmt(row.net_points, 0)} 點' if entered else "—"
        klass = "gain" if entered and float(row.net_points) > 0 else "loss" if entered and float(row.net_points) < 0 else "flat"
        cells.append(f'<div class="result"><b style="color:{VARIANT_COLORS[variant]}">{VARIANT_LABELS[variant]}</b><span>{esc(detail)}</span><strong class="{klass}">{net}</strong></div>')
    return '<div class="results">' + "".join(cells) + "</div>"


def build_gallery(detail: pd.DataFrame, settings: Settings, output: Path) -> None:
    exit_settings = ExitSettings(start=settings.start, end=settings.end, futures_id=settings.futures_id, range_days=settings.range_days)
    contracts = load_contract_map(exit_settings)
    cards = []
    for trade_date, rows in detail.groupby("trade_date", sort=True):
        trade_date = pd.Timestamp(trade_date).date()
        contract = contracts.get(trade_date)
        ticks = load_day_ticks(exit_settings, trade_date, contract) if contract else []
        bars = five_minute_bars(ticks)
        bounce = rows[rows.variant == "BOUNCE_1_6R"].iloc[0]
        breached = bool(bounce.get("pre_entry_stop_crossed", False))
        rescued = bool(bounce.get("reentry_mode", False)) and bounce.entry_status == "ENTERED"
        outcomes = {}
        for variant, data_key in (("CANCEL_DAY", "cancel"), ("DIRECT_ENTRY", "direct"), ("BOUNCE_1_6R", "bounce")):
            variant_row = rows[rows.variant == variant].iloc[0]
            if variant_row.entry_status != "ENTERED":
                outcome = "no_entry"
            else:
                net = _number(variant_row.get("net_points"))
                outcome = "profit" if net is not None and net > 0 else "loss" if net is not None and net < 0 else "flat"
            outcomes[data_key] = outcome
        attrs = (
            f'data-year="{trade_date.year}" data-breached="{str(breached).lower()}" '
            f'data-rescued="{str(rescued).lower()}" data-outcome-cancel="{outcomes["cancel"]}" '
            f'data-outcome-direct="{outcomes["direct"]}" data-outcome-bounce="{outcomes["bounce"]}"'
        )
        title = f"{trade_date:%Y-%m-%d}｜訊號日 {pd.Timestamp(rows.iloc[0].signal_date):%Y-%m-%d}"
        meta = f"昨日日盤收 {fmt(rows.iloc[0].get('previous_day_close'))}　夜盤收 {fmt(rows.iloc[0].night_close)}　30日均幅 {fmt(rows.iloc[0].average_range_30,1)}　散戶分位 {fmt(rows.iloc[0].retail_percentile_pct,1)}%"
        cards.append(f'<article class="card" {attrs}><h2>{title}</h2><p class="meta">{meta}</p>{svg_chart(bars, rows)}{result_table(rows)}</article>')
    years = sorted({pd.Timestamp(v).year for v in detail.trade_date})
    settings_json = json.dumps({k: v.isoformat() if isinstance(v, date) else v for k, v in asdict(settings).items()}, ensure_ascii=False)
    page = f'''<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>散戶極度偏空策略｜5分K交易日圖庫</title><style>
:root{{--bg:#f1f5f9;--ink:#172033;--muted:#64748b}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font-family:system-ui,-apple-system,"Noto Sans TC",sans-serif}}header{{position:sticky;top:0;z-index:5;background:#ffffffee;backdrop-filter:blur(8px);padding:14px 20px;border-bottom:1px solid #cbd5e1}}h1{{font-size:20px;margin:0 0 9px}}.controls{{display:flex;gap:9px;flex-wrap:wrap;align-items:center;font-size:13px}}select,label{{background:#fff;border:1px solid #cbd5e1;border-radius:7px;padding:6px 8px}}.filter-group{{display:flex;gap:5px;align-items:center;background:#eaf2ff;border-radius:8px;padding:4px 6px}}.filter-group span{{font-weight:650;color:#334155}}.sample-count{{margin-left:auto;background:#0f172a;color:#fff;border-radius:999px;padding:6px 11px;white-space:nowrap}}main{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;padding:14px}}.card{{background:#fff;border-radius:12px;padding:12px;box-shadow:0 1px 4px #0f172a18;min-width:0}}h2{{font-size:15px;margin:0 0 3px}}.meta{{font-size:11px;color:var(--muted);margin:0 0 6px}}.chart{{display:block;width:100%;height:auto;background:#fff}}.grid{{stroke:#e2e8f0;stroke-width:1}}.axis{{font-size:11px;fill:#64748b}}.level{{font-size:10px;font-weight:650}}.results{{display:grid;gap:4px;margin-top:6px}}.result{{display:grid;grid-template-columns:70px 1fr 62px;gap:5px;font-size:11px;border-top:1px solid #eef2f7;padding-top:4px}}.result strong{{text-align:right}}.gain{{color:#dc2626}}.loss{{color:#16803d}}.flat{{color:#64748b}}.missing{{height:220px;display:grid;place-items:center;color:#94a3b8}}.hidden{{display:none!important}}.count{{font-weight:700}}
@media(max-width:1250px){{main{{grid-template-columns:repeat(2,minmax(0,1fr))}}}}@media(max-width:760px){{main{{grid-template-columns:1fr;padding:8px}}header{{padding:10px}}.result{{grid-template-columns:64px 1fr 54px}}}}
</style></head><body><header><h1>散戶極度偏空策略｜5分K交易日圖庫</h1><div class="controls"><select id="year"><option value="all">全部年份</option>{''.join(f'<option>{y}</option>' for y in years)}</select><div class="filter-group"><span>績效篩選</span><select id="strategy"><option value="all">三個策略</option><option value="cancel">原規則</option><option value="direct">直接進場</option><option value="bounce">反彈1/6R</option></select><select id="outcome"><option value="all">全部結果</option><option value="no_entry">沒有進場</option><option value="profit">賺錢</option><option value="loss">賠錢</option><option value="flat">損益為0</option></select></div><label><input id="breach" type="checkbox"> 僅看前30分鐘破失效價</label><label><input id="rescue" type="checkbox"> 僅看反彈救援有進場</label><label><input class="toggle" data-v="CANCEL_DAY" type="checkbox" checked> 原規則線</label><label><input class="toggle" data-v="DIRECT_ENTRY" type="checkbox" checked> 直接進場線</label><label><input class="toggle" data-v="BOUNCE_1_6R" type="checkbox" checked> 反彈線</label><span class="sample-count">顯示 <span class="count">0</span> / {len(cards)} 個樣本</span></div></header><main>{''.join(cards)}</main>
<script>const cards=[...document.querySelectorAll('.card')],year=document.querySelector('#year'),strategy=document.querySelector('#strategy'),outcome=document.querySelector('#outcome'),breach=document.querySelector('#breach'),rescue=document.querySelector('#rescue');function outcomeMatch(c){{if(outcome.value==='all')return true;const values={{cancel:c.dataset.outcomeCancel,direct:c.dataset.outcomeDirect,bounce:c.dataset.outcomeBounce}};if(strategy.value==='all'){{if(outcome.value==='no_entry')return Object.values(values).every(v=>v==='no_entry');return Object.values(values).some(v=>v===outcome.value)}}return values[strategy.value]===outcome.value}}function filter(){{let n=0;cards.forEach(c=>{{const show=(year.value==='all'||c.dataset.year===year.value)&&(!breach.checked||c.dataset.breached==='true')&&(!rescue.checked||c.dataset.rescued==='true')&&outcomeMatch(c);c.classList.toggle('hidden',!show);if(show)n++}});document.querySelector('.count').textContent=n}}[year,strategy,outcome,breach,rescue].forEach(x=>x.addEventListener('change',filter));document.querySelectorAll('.toggle').forEach(x=>x.addEventListener('change',()=>document.querySelectorAll('.variant-'+x.dataset.v).forEach(e=>e.style.display=x.checked?'':'none')));filter();</script>
<!-- 回測參數：{html.escape(settings_json)} --></body></html>'''
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(page, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="輸出符合策略前置條件之交易日5分K HTML圖庫")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--cost-points", type=float, default=3.0)
    parser.add_argument("--output", type=Path, default=Path("../output/backtest/retail_bounce_reentry/retail_bounce_reentry_5m_gallery.html"))
    args = parser.parse_args()
    settings = Settings(start=args.start, end=args.end, round_trip_cost_points=args.cost_points)
    detail = run_backtest(settings)
    build_gallery(detail, settings, args.output)
    print(f"[retail_bounce_reentry_gallery] days: {detail['trade_date'].nunique()}")
    print(f"[retail_bounce_reentry_gallery] html: {args.output.resolve()}")


if __name__ == "__main__":
    main()
