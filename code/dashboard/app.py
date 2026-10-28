from __future__ import annotations

import io
import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from dashboard.config import DEFAULT_START_DATE, INDICATORS, MARKETS
from dashboard.cycle_service import CYCLE_TYPES, build_cycle_statistics
from dashboard.data_service import TAIL_MERGE_THRESHOLD_PCT, build_distribution, filter_period, load_market_snapshot
from dashboard.kline_service import KLINE_INTERVALS, US_KLINE_INTERVALS, US_MARKETS, load_kline_data, load_preset_levels, load_us_kline_data, load_us_quick_view_data
from dashboard.yu_level_chart_service import load_yu_chart_date_bounds, load_yu_chart_levels
from dashboard.night_bucket_service import (
    DEFAULT_END_DATE as NIGHT_BUCKET_END_DATE,
    DEFAULT_METRIC as NIGHT_BUCKET_DEFAULT_METRIC,
    DEFAULT_START_DATE as NIGHT_BUCKET_START_DATE,
    METRICS as NIGHT_BUCKET_METRICS,
    build_payload as build_night_bucket_payload,
    export_frame as export_night_bucket_frame,
)
from dashboard.next_day_session_service import (
    CORE_ORDER as NEXT_DAY_CORE_ORDER,
    DEFAULT_END_DATE as NEXT_DAY_END_DATE,
    DEFAULT_START_DATE as NEXT_DAY_START_DATE,
    NIGHT_BUCKET_ORDER as NEXT_DAY_BUCKET_ORDER,
    NIGHT_TREND_ORDER as NEXT_DAY_TREND_ORDER,
    build_payload as build_next_day_session_payload,
)
from dashboard.raw_service import PAGE_SIZES, RAW_REPORTS, TAIWAN_RAW_REPORTS, US_RAW_REPORTS, export_raw_report, load_raw_report
from dashboard.report_updater import get_report_update_status, get_update_catalog, start_report_update
from dashboard.updater import update_price_data


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(
    title="級距統計",
    description="加權指數與台指期日盤、夜盤、全日歷史統計",
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url=None,
)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


class ReportUpdateRequest(BaseModel):
    modules: list[str] = Field(default_factory=lambda: ["taiex_report", "futures_report"])
    force: bool = False


def _validate_options(market: str, indicator: str) -> None:
    if market not in MARKETS:
        raise HTTPException(status_code=400, detail="不支援的市場")
    if indicator not in INDICATORS:
        raise HTTPException(status_code=400, detail="不支援的指標")


def _build_payload(market: str, indicator: str, start_date: str, end_date: str, bin_width: float | None = None) -> dict:
    _validate_options(market, indicator)
    if pd.Timestamp(start_date) > pd.Timestamp(end_date):
        raise HTTPException(status_code=400, detail="開始日期不可晚於結束日期")

    snapshot = load_market_snapshot(market)
    selected = filter_period(snapshot.data, start_date, end_date)
    spec = INDICATORS[indicator]
    effective_bin_width = spec.default_bin_width if bin_width is None else bin_width
    table, summary = build_distribution(selected, spec, effective_bin_width)
    # pandas／NumPy 型別先經 JSON 轉換，確保 FastAPI 可以穩定序列化。
    records = json.loads(table.to_json(orient="records", force_ascii=False))

    return {
        "market": {"key": market, "name": snapshot.market_name},
        "indicator": {"key": indicator, "title": spec.title, "unit": spec.unit},
        "binning": {
            "width": effective_bin_width,
            "bucket_count": len(table),
            "tail_merge_threshold_pct": TAIL_MERGE_THRESHOLD_PCT,
        },
        "period": {"start": start_date, "end": end_date},
        "source": {
            "min_date": snapshot.source_min_date.strftime("%Y-%m-%d"),
            "max_date": snapshot.source_max_date.strftime("%Y-%m-%d"),
            "invalid_ohlc_count": snapshot.invalid_ohlc_count,
        },
        "summary": summary,
        "columns": list(table.columns),
        "rows": records,
        "chart": {
            "labels": table["級距"].tolist(),
            "counts": table["總計"].astype(int).tolist(),
            "percentages": table["分組占比"].str.rstrip("%").astype(float).tolist(),
        },
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _build_cycle_payload(market: str, cycle_type: str, start_date: str, end_date: str) -> dict:
    if market not in MARKETS:
        raise HTTPException(status_code=400, detail="不支援的市場")
    if cycle_type not in CYCLE_TYPES:
        raise HTTPException(status_code=400, detail="不支援的週期類型")
    if pd.Timestamp(start_date) > pd.Timestamp(end_date):
        raise HTTPException(status_code=400, detail="開始日期不可晚於結束日期")

    snapshot = load_market_snapshot(market)
    selected = filter_period(snapshot.data, start_date, end_date)
    table, summary = build_cycle_statistics(selected, cycle_type)
    numeric_records = json.loads(table.to_json(orient="records", force_ascii=False))
    display = table.copy()
    for column in ["上漲機率", "下跌機率", "平均漲跌幅"]:
        display[column] = display[column].map(lambda value: f"{value:.2f}%")
    for column in ["平均上漲點", "平均下跌點", "平均漲跌點數"]:
        display[column] = display[column].map(lambda value: f"{value:.2f}")

    return {
        "market": {"key": market, "name": snapshot.market_name},
        "cycle": {"key": cycle_type, "name": CYCLE_TYPES[cycle_type]},
        "period": {"start": start_date, "end": end_date},
        "source": {
            "min_date": snapshot.source_min_date.strftime("%Y-%m-%d"),
            "max_date": snapshot.source_max_date.strftime("%Y-%m-%d"),
        },
        "summary": summary,
        "columns": list(display.columns),
        "rows": json.loads(display.to_json(orient="records", force_ascii=False)),
        "chart_rows": numeric_records,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "markets": MARKETS,
            "indicators": INDICATORS,
            "default_start_date": DEFAULT_START_DATE,
            "default_end_date": datetime.now().strftime("%Y-%m-%d"),
        },
    )


@app.get("/distribution", response_class=HTMLResponse)
async def distribution_page(request: Request):
    return await index(request)


@app.get("/cycles", response_class=HTMLResponse)
async def cycles_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="cycles.html",
        context={
            "markets": MARKETS,
            "cycle_types": CYCLE_TYPES,
            "default_start_date": DEFAULT_START_DATE,
            "default_end_date": datetime.now().strftime("%Y-%m-%d"),
        },
    )


@app.get("/raw-data", response_class=HTMLResponse)
async def raw_data_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="raw_data.html",
        context={
            "reports": TAIWAN_RAW_REPORTS,
            "page_sizes": PAGE_SIZES,
            "default_end_date": datetime.now().strftime("%Y-%m-%d"),
        },
    )




@app.get("/us/quick-view", response_class=HTMLResponse)
async def us_quick_view_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="us_quick_view.html",
        context={
            "markets": [
                {"key": key, "name": name, "symbol": symbol}
                for key, (name, symbol) in US_MARKETS.items()
            ],
            "intervals": US_KLINE_INTERVALS,
            "default_start_date": (pd.Timestamp.now().normalize() - pd.DateOffset(years=1)).strftime("%Y-%m-%d"),
            "default_end_date": datetime.now().strftime("%Y-%m-%d"),
        },
    )

@app.get("/us/raw-data", response_class=HTMLResponse)
async def us_raw_data_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="us_raw_data.html",
        context={
            "reports": US_RAW_REPORTS,
            "page_sizes": PAGE_SIZES,
            "default_end_date": datetime.now().strftime("%Y-%m-%d"),
        },
    )


@app.get("/kline", response_class=HTMLResponse)
async def kline_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="kline.html",
        context={
            "markets": MARKETS,
            "intervals": KLINE_INTERVALS,
            "default_start_date": DEFAULT_START_DATE,
            "default_end_date": datetime.now().strftime("%Y-%m-%d"),
        },
    )


@app.get("/us/kline", response_class=HTMLResponse)
async def us_kline_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="us_kline.html",
        context={
            "markets": {key: value[0] for key, value in US_MARKETS.items()},
            "intervals": US_KLINE_INTERVALS,
            "default_start_date": DEFAULT_START_DATE,
            "default_end_date": datetime.now().strftime("%Y-%m-%d"),
        },
    )


@app.get("/yu-key-levels", response_class=HTMLResponse)
async def yu_key_levels_page(request: Request):
    default_start_date, default_end_date = await run_in_threadpool(load_yu_chart_date_bounds)
    return templates.TemplateResponse(
        request=request,
        name="yu_key_levels.html",
        context={
            "default_start_date": default_start_date,
            "default_end_date": default_end_date,
        },
    )


@app.get("/night-bucket", response_class=HTMLResponse)
async def night_bucket_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="night_bucket.html",
        context={
            "metrics": NIGHT_BUCKET_METRICS,
            "default_metric": NIGHT_BUCKET_DEFAULT_METRIC,
            "default_start_date": NIGHT_BUCKET_START_DATE,
            "default_end_date": NIGHT_BUCKET_END_DATE,
        },
    )


@app.get("/next-day-session", response_class=HTMLResponse)
async def next_day_session_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="next_day_session.html",
        context={
            "core_options": NEXT_DAY_CORE_ORDER,
            "night_bucket_options": NEXT_DAY_BUCKET_ORDER,
            "night_trend_options": NEXT_DAY_TREND_ORDER,
            "default_start_date": NEXT_DAY_START_DATE,
            "default_end_date": datetime.now().strftime("%Y-%m-%d"),
        },
    )


@app.get("/api/next-day-session")
async def next_day_session_data(
    start_date: str = Query(NEXT_DAY_START_DATE),
    end_date: str = Query(NEXT_DAY_END_DATE),
    core: str = Query("全部"),
    night_bucket: str = Query("全部"),
    night_trend: str = Query("全部"),
):
    try:
        payload = await run_in_threadpool(
            build_next_day_session_payload,
            start_date,
            end_date,
            core,
            night_bucket,
            night_trend,
        )
        return JSONResponse(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/night-bucket")
async def night_bucket_data(
    start_date: str = Query(NIGHT_BUCKET_START_DATE),
    end_date: str = Query(NIGHT_BUCKET_END_DATE),
    metric: str = Query(NIGHT_BUCKET_DEFAULT_METRIC),
):
    try:
        payload = await run_in_threadpool(
            build_night_bucket_payload, start_date, end_date, metric
        )
        return JSONResponse(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/night-bucket/export.csv")
async def export_night_bucket_csv(
    dataset: str = Query("daily"),
    start_date: str = Query(NIGHT_BUCKET_START_DATE),
    end_date: str = Query(NIGHT_BUCKET_END_DATE),
    metric: str = Query(NIGHT_BUCKET_DEFAULT_METRIC),
):
    try:
        frame = await run_in_threadpool(
            export_night_bucket_frame, start_date, end_date, dataset, metric
        )
        buffer = io.StringIO()
        frame.to_csv(buffer, index=False)
        content = "\ufeff" + buffer.getvalue()
        filename = (
            f"futures_night_bucket_{dataset}_"
            f"{start_date.replace('-', '')}_{end_date.replace('-', '')}.csv"
        )
        return StreamingResponse(
            iter([content.encode("utf-8")]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}"},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/kline")
async def kline_data(
    market: str = Query("taiex"),
    interval: str = Query("day"),
    start_date: str = Query(DEFAULT_START_DATE),
    end_date: str | None = Query(None),
):
    try:
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")
        frame, metadata = await run_in_threadpool(
            load_kline_data, market, interval, start_date, end_date
        )
        rows = []
        for record in frame.to_dict(orient="records"):
            bar_time = pd.Timestamp(record["日期"])
            if metadata.get("intraday"):
                # Lightweight Charts 的分時資料採 UTC timestamp；以臺北時區轉換，避免日期漂移。
                chart_time = int(bar_time.tz_localize("Asia/Taipei").timestamp())
            else:
                chart_time = bar_time.strftime("%Y-%m-%d")
            rows.append({
                "time": chart_time,
                "open": float(record["開盤"]),
                "high": float(record["最高"]),
                "low": float(record["最低"]),
                "close": float(record["收盤"]),
                "volume": float(record["成交量"]),
            })
        return JSONResponse({"meta": metadata, "bars": rows})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc




@app.get("/api/us/quick-view")
async def us_quick_view_data(
    interval: str = Query("day"),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    force: bool = Query(False),
):
    try:
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")
        start_date = start_date or (pd.Timestamp(end_date) - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
        datasets = await run_in_threadpool(load_us_quick_view_data, interval, start_date, end_date, force)
        charts = {}
        for market_key, (frame, metadata) in datasets.items():
            bars = [{
                "time": pd.Timestamp(record["日期"]).strftime("%Y-%m-%d"),
                "open": float(record["開盤"]),
                "high": float(record["最高"]),
                "low": float(record["最低"]),
                "close": float(record["收盤"]),
                "volume": float(record["成交量"]),
            } for record in frame.to_dict(orient="records")]
            charts[market_key] = {"meta": metadata, "bars": bars}
        return JSONResponse({
            "interval": interval,
            "interval_name": US_KLINE_INTERVALS[interval],
            "start_date": start_date,
            "end_date": end_date,
            "charts": charts,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.get("/api/us/kline")
async def us_kline_data(
    market: str = Query("us_ixic"),
    interval: str = Query("day"),
    start_date: str = Query(DEFAULT_START_DATE),
    end_date: str | None = Query(None),
):
    try:
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")
        frame, metadata = await run_in_threadpool(
            load_us_kline_data, market, interval, start_date, end_date
        )
        rows = [{
            "time": pd.Timestamp(record["日期"]).strftime("%Y-%m-%d"),
            "open": float(record["開盤"]),
            "high": float(record["最高"]),
            "low": float(record["最低"]),
            "close": float(record["收盤"]),
            "volume": float(record["成交量"]),
        } for record in frame.to_dict(orient="records")]
        return JSONResponse({"meta": metadata, "bars": rows})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/kline/preset-levels")
async def kline_preset_levels(date: str, start_date: str, end_date: str):
    try:
        result = await run_in_threadpool(load_preset_levels, date, start_date, end_date)
        return JSONResponse(result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/yu-key-levels")
async def yu_key_levels(
    start_date: str = Query(...), as_of_date: str = Query(...), mode: str = Query("strict")
):
    try:
        cutoff = pd.Timestamp(as_of_date).normalize()
        if pd.isna(cutoff):
            raise ValueError("截止日期格式錯誤")
        return JSONResponse(await run_in_threadpool(
            load_yu_chart_levels, start_date, cutoff.strftime("%Y-%m-%d"), mode
        ))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/raw-data")
async def raw_data(
    report: str = Query("taiex"),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    limit: int = Query(30),
    offset: int = Query(0, ge=0),
    sort_column: str | None = Query(None),
    sort_direction: str = Query("desc"),
    search_column: str | None = Query(None),
    search_text: str | None = Query(None),
):
    try:
        frame, metadata = await run_in_threadpool(
            load_raw_report, report, start_date, end_date, limit, offset,
            sort_column, sort_direction, search_column, search_text
        )
        return JSONResponse({
            "report": metadata,
            "columns": list(frame.columns),
            "rows": json.loads(frame.to_json(orient="records", force_ascii=False, date_format="iso")),
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/report-update")
async def begin_report_update(request: ReportUpdateRequest | None = None):
    try:
        return start_report_update(
            request.modules if request else None,
            force=request.force if request else False,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/report-update/status")
async def report_update_status():
    return get_report_update_status()


@app.get("/api/report-update/catalog")
async def report_update_catalog():
    try:
        return await run_in_threadpool(get_update_catalog)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/raw-data/export.csv")
async def export_raw_data_csv(
    report: str = Query("taiex"),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    columns: str | None = Query(None),
):
    try:
        selected_columns = [item for item in (columns or "").split("|") if item] or None
        frame, metadata = await run_in_threadpool(
            export_raw_report, report, start_date, end_date, selected_columns
        )
        buffer = io.StringIO()
        frame.to_csv(buffer, index=False)
        content = "\ufeff" + buffer.getvalue()
        raw_filename = (
            f"{metadata['name']}_{metadata['start_date'].replace('-', '')}-"
            f"{metadata['end_date'].replace('-', '')}.csv"
        )
        filename = re.sub(r'[<>:"/\\|?*]+', "_", raw_filename)
        return StreamingResponse(
            iter([content.encode("utf-8")]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}"},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/cycles")
async def cycles_data(
    market: str = Query("taiex"),
    cycle_type: str = Query("weekday"),
    start_date: str = Query(DEFAULT_START_DATE),
    end_date: str | None = Query(None),
):
    try:
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")
        payload = await run_in_threadpool(_build_cycle_payload, market, cycle_type, start_date, end_date)
        return JSONResponse(payload)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/dashboard")
async def dashboard_data(
    market: str = Query("taiex"),
    indicator: str = Query("range_points"),
    start_date: str = Query(DEFAULT_START_DATE),
    end_date: str | None = Query(None),
    bin_width: float | None = Query(None, gt=0),
):
    try:
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")
        payload = await run_in_threadpool(_build_payload, market, indicator, start_date, end_date, bin_width)
        return JSONResponse(payload)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/update")
async def update_data():
    try:
        result = await run_in_threadpool(update_price_data)
        return {
            "ok": True,
            "message": "最新行情更新完成",
            "taiex_max_date": result.taiex_max_date,
            "tx_max_date": result.tx_max_date,
            "tx_rows_synced": result.tx_rows_synced,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/export.csv")
async def export_csv(
    market: str = Query("taiex"),
    indicator: str = Query("range_points"),
    start_date: str = Query(DEFAULT_START_DATE),
    end_date: str | None = Query(None),
    bin_width: float | None = Query(None, gt=0),
):
    try:
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")
        payload = await run_in_threadpool(_build_payload, market, indicator, start_date, end_date, bin_width)
        frame = pd.DataFrame(payload["rows"], columns=payload["columns"])
        metadata = {
            "市場": payload["market"]["name"],
            "統計指標": payload["indicator"]["title"],
            "單位": payload["indicator"]["unit"],
            "級距寬度": payload["binning"]["width"],
            "頭尾合併門檻": f"<{payload['binning']['tail_merge_threshold_pct']}%",
            "開始日期": payload["period"]["start"],
            "結束日期": payload["period"]["end"],
        }
        frame = pd.concat([pd.DataFrame(metadata, index=frame.index), frame], axis=1)
        buffer = io.StringIO()
        frame.to_csv(buffer, index=False)
        content = "\ufeff" + buffer.getvalue()
        bin_width_text = f"{payload['binning']['width']:g}{payload['indicator']['unit']}"
        raw_filename = (
            f"{payload['market']['name']}_{payload['indicator']['title']}_"
            f"{start_date.replace('-', '')}-{end_date.replace('-', '')}_級距{bin_width_text}.csv"
        )
        filename = re.sub(r'[<>:"/\\|?*]+', "_", raw_filename)
        return StreamingResponse(
            iter([content.encode("utf-8")]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}"},
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/cycles/export.csv")
async def export_cycles_csv(
    market: str = Query("taiex"),
    cycle_type: str = Query("weekday"),
    start_date: str = Query(DEFAULT_START_DATE),
    end_date: str | None = Query(None),
):
    try:
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")
        payload = await run_in_threadpool(_build_cycle_payload, market, cycle_type, start_date, end_date)
        frame = pd.DataFrame(payload["rows"], columns=payload["columns"])
        metadata = {
            "市場": payload["market"]["name"],
            "統計類型": payload["cycle"]["name"],
            "漲跌定義": "今收－昨收",
            "開始日期": payload["period"]["start"],
            "結束日期": payload["period"]["end"],
        }
        frame = pd.concat([pd.DataFrame(metadata, index=frame.index), frame], axis=1)
        buffer = io.StringIO()
        frame.to_csv(buffer, index=False)
        content = "\ufeff" + buffer.getvalue()
        filename = (
            f"{payload['market']['name']}_{payload['cycle']['name']}_"
            f"{start_date.replace('-', '')}-{end_date.replace('-', '')}.csv"
        )
        return StreamingResponse(
            iter([content.encode("utf-8")]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}"},
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/health")
async def health():
    return {"status": "ok", "time": datetime.now().isoformat(timespec="seconds")}
