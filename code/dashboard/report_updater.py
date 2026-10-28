from __future__ import annotations

import threading
import time
import traceback
from datetime import datetime, timedelta
from typing import Iterable

from common import db


# 三大區塊與其中項目。前端直接讀這份 catalog，避免 UI 與後端名稱分叉。
UPDATE_SECTIONS = (
    {
        "id": "raw",
        "name": "原始資料",
        "icon": "bi-database-down",
        "description": "更新加權指數與台指期日資料／報表來源。",
        "items": (
            {"id": "taiex_report", "name": "加權指數日資料", "ttl_minutes": 10},
            {"id": "futures_report", "name": "台指期日資料", "ttl_minutes": 10},
        ),
    },
    {
        "id": "kline",
        "name": "K 線",
        "icon": "bi-bar-chart-steps",
        "description": "增量更新一分 K；近期區間會回看以補齊遲到資料。",
        "items": (
            {"id": "taiex_kbar", "name": "加權指數分 K", "ttl_minutes": 5},
            {"id": "futures_kbar", "name": "台指期分 K", "ttl_minutes": 5},
        ),
    },
    {
        "id": "foreign",
        "name": "外國股市資料",
        "icon": "bi-globe-americas",
        "description": "FinMind 美股日資料；只補資料庫最新日期附近，不重抓完整歷史。",
        "items": (
            {"id": "us_ixic", "name": "NASDAQ", "symbol": "^IXIC", "ttl_minutes": 360},
            {"id": "us_sox", "name": "費城半導體", "symbol": "^SOX", "ttl_minutes": 360},
            {"id": "us_dji", "name": "道瓊", "symbol": "^DJI", "ttl_minutes": 360},
            {"id": "us_gspc", "name": "S&P 500", "symbol": "^GSPC", "ttl_minutes": 360},
            {"id": "us_vix", "name": "VIX", "symbol": "^VIX", "ttl_minutes": 360},
            {"id": "us_tsm", "name": "TSM ADR", "symbol": "TSM", "ttl_minutes": 360},
        ),
    },
)

UPDATE_ITEMS = {
    item["id"]: {**item, "section_id": section["id"], "section_name": section["name"]}
    for section in UPDATE_SECTIONS
    for item in section["items"]
}
UPDATE_NAMES = {module: meta["name"] for module, meta in UPDATE_ITEMS.items()}
DEFAULT_UPDATE_ORDER = tuple(UPDATE_NAMES)
CACHE_PREFIX = "dashboard_update:"

_LOCK = threading.Lock()
_STATE_LOCK = threading.Lock()
_STATE = {
    "running": False,
    "step": "idle",
    "message": "尚未執行更新",
    "started_at": None,
    "finished_at": None,
    "error": None,
    "modules": [],
    "completed_modules": [],
    "skipped_modules": [],
    "results": {},
    "current_index": 0,
    "total_modules": 0,
    "force": False,
}


def _set_state(**values) -> None:
    with _STATE_LOCK:
        _STATE.update(values)


def get_report_update_status() -> dict:
    with _STATE_LOCK:
        return dict(_STATE)


def _cache_name(module: str) -> str:
    return f"{CACHE_PREFIX}{module}"


def _get_cache_rows() -> dict[str, dict]:
    try:
        frame = db.query_to_df(
            """
            SELECT cache_name, refreshed_at, row_count, elapsed_seconds
            FROM core.report_cache_status
            WHERE cache_name LIKE ?
            """,
            (f"{CACHE_PREFIX}%",),
        )
    except Exception:
        return {}
    rows: dict[str, dict] = {}
    for row in frame.to_dict("records"):
        module = str(row["cache_name"])[len(CACHE_PREFIX):]
        refreshed = row.get("refreshed_at")
        rows[module] = {
            "refreshed_at": None if refreshed is None else refreshed.isoformat(timespec="seconds"),
            "row_count": int(row.get("row_count") or 0),
            "elapsed_seconds": row.get("elapsed_seconds"),
        }
    return rows


def _last_refresh(module: str) -> datetime | None:
    value = db.query_single_value(
        "SELECT refreshed_at FROM core.report_cache_status WHERE cache_name = ?",
        (_cache_name(module),),
    )
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _is_fresh(module: str) -> bool:
    ttl = int(UPDATE_ITEMS[module].get("ttl_minutes", 0))
    if ttl <= 0:
        return False
    try:
        refreshed = _last_refresh(module)
    except Exception:
        return False
    return refreshed is not None and datetime.now() - refreshed < timedelta(minutes=ttl)


def _mark_refresh(module: str, row_count: int, elapsed_seconds: float) -> None:
    ok = db.execute_sql(
        """
        INSERT INTO core.report_cache_status (cache_name, refreshed_at, row_count, elapsed_seconds)
        VALUES (?, CURRENT_TIMESTAMP, ?, ?)
        ON CONFLICT (cache_name) DO UPDATE SET
            refreshed_at = EXCLUDED.refreshed_at,
            row_count = EXCLUDED.row_count,
            elapsed_seconds = EXCLUDED.elapsed_seconds
        """,
        (_cache_name(module), int(row_count), float(elapsed_seconds)),
    )
    if not ok:
        print(f"[Dashboard 更新快取] 無法記錄 {module} 的完成時間；不影響資料更新結果。")


def get_update_catalog() -> dict:
    cache = _get_cache_rows()
    sections = []
    for section in UPDATE_SECTIONS:
        items = []
        for item in section["items"]:
            module = item["id"]
            items.append({**item, "cache": cache.get(module)})
        sections.append({
            "id": section["id"],
            "name": section["name"],
            "icon": section["icon"],
            "description": section["description"],
            "items": items,
        })
    return {"sections": sections, "status": get_report_update_status()}


def _normalize_modules(modules: Iterable[str] | None) -> tuple[str, ...]:
    values = list(modules or ("taiex_report", "futures_report"))
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        module = str(value).strip()
        if not module or module in seen:
            continue
        if module not in UPDATE_ITEMS:
            raise ValueError(f"不支援的更新模組：{module}")
        seen.add(module)
        ordered.append(module)
    if not ordered:
        raise ValueError("請至少選擇一個更新模組")
    return tuple(ordered)


def _row_count_from_result(result) -> int:
    if result is None:
        return 0
    if hasattr(result, "shape"):
        return int(result.shape[0])
    if isinstance(result, dict):
        total = 0
        for value in result.values():
            if hasattr(value, "shape"):
                total += int(value.shape[0])
            elif isinstance(value, (int, float)):
                total += int(value)
        return total
    try:
        return int(len(result))
    except Exception:
        return 0


def _run_module(module: str, *, selected_modules: set[str]) -> tuple[int, dict]:
    if module == "taiex_report":
        from main.stock_report_taiex import run_report_pipeline
        result = run_report_pipeline(export_csv=False)
        return _row_count_from_result(result), {}

    if module == "taiex_kbar":
        from main.intraday_pipeline import update_taiex_kbar_1m
        result = update_taiex_kbar_1m(datetime.now())
        return _row_count_from_result(result), {}

    if module == "futures_report":
        from main.futures import run_report_pipeline
        # 同一輪若另有 futures_kbar，就不要在報表流程裡再打一輪 Tick API。
        result = run_report_pipeline(
            refresh_cache=False,
            export_csv=False,
            sync_intraday="futures_kbar" not in selected_modules,
        )
        return _row_count_from_result(result), {}

    if module == "futures_kbar":
        from main.intraday_pipeline import update_tx_kbar_1m
        result = update_tx_kbar_1m(
            datetime.now(),
            refresh_turning="futures_report" not in selected_modules,
        )
        return _row_count_from_result(result), {}

    raise ValueError(f"模組 {module} 必須由批次更新器處理")


def _run_us_batch(modules: list[str]) -> dict[str, dict]:
    from module.us_market import update_us_stock_daily_incremental

    symbols = [UPDATE_ITEMS[module]["symbol"] for module in modules]
    by_symbol = update_us_stock_daily_incremental(symbols, datetime.now(), lookback_days=7)
    return {
        module: by_symbol.get(UPDATE_ITEMS[module]["symbol"], {})
        for module in modules
    }


def _refresh_futures_derivatives_after_late_kbar() -> None:
    """報表先跑、K 線後跑時，只重算依賴 K 線的衍生層，不再抓 API。"""
    from module.futures_turning import ensure_turning_views, refresh_turning_levels
    from module.report_cache import refresh_futures_dashboard_cache, refresh_futures_report_tail_cache

    refresh_turning_levels()
    refresh_futures_report_tail_cache()
    refresh_futures_dashboard_cache(recent_days=14, force_full=False)
    ensure_turning_views()


def _run(modules: tuple[str, ...], force: bool) -> None:
    completed: list[str] = []
    skipped: list[str] = []
    results: dict[str, dict] = {}
    selected_set = set(modules)
    try:
        index = 0
        while index < len(modules):
            module = modules[index]

            if module.startswith("us_"):
                batch: list[str] = []
                while index < len(modules) and modules[index].startswith("us_"):
                    candidate = modules[index]
                    if not force and _is_fresh(candidate):
                        skipped.append(candidate)
                        results[candidate] = {"status": "cached"}
                    else:
                        batch.append(candidate)
                    index += 1
                if batch:
                    _set_state(
                        step=batch[0],
                        current_index=len(completed) + len(skipped) + 1,
                        message=f"正在更新外國股市資料（{len(batch)} 項）…",
                    )
                    started = time.perf_counter()
                    batch_result = _run_us_batch(batch)
                    elapsed = time.perf_counter() - started
                    each_elapsed = elapsed / max(1, len(batch))
                    for candidate in batch:
                        info = batch_result.get(candidate, {})
                        rows = int(info.get("written_rows") or info.get("rows") or 0)
                        _mark_refresh(candidate, rows, each_elapsed)
                        completed.append(candidate)
                        results[candidate] = {"status": "updated", **info}
                    _set_state(completed_modules=list(completed), skipped_modules=list(skipped), results=dict(results))
                continue

            _set_state(
                step=module,
                current_index=len(completed) + len(skipped) + 1,
                message=f"正在更新{UPDATE_NAMES[module]}…",
            )
            if not force and _is_fresh(module):
                skipped.append(module)
                results[module] = {"status": "cached"}
                _set_state(skipped_modules=list(skipped), results=dict(results))
                index += 1
                continue

            started = time.perf_counter()
            rows, details = _run_module(module, selected_modules=selected_set)
            elapsed = time.perf_counter() - started
            _mark_refresh(module, rows, elapsed)
            completed.append(module)
            results[module] = {"status": "updated", "rows": rows, "elapsed_seconds": round(elapsed, 3), **details}
            _set_state(completed_modules=list(completed), results=dict(results))
            index += 1

        # 若使用者把「原始資料」排在「K 線」前面，期貨報表當下刻意不重抓 K 線；
        # K 線完成後再刷新依賴 K 線的轉折與尾端快取，避免重複 API。
        if "futures_report" in modules and "futures_kbar" in modules:
            if modules.index("futures_report") < modules.index("futures_kbar") and "futures_kbar" in completed:
                _set_state(step="finalize", message="正在刷新期貨 K 線衍生資料…")
                _refresh_futures_derivatives_after_late_kbar()

        updated_names = "、".join(UPDATE_NAMES[module] for module in completed)
        if skipped:
            cached_names = "、".join(UPDATE_NAMES[module] for module in skipped)
            message = f"更新完成；已更新：{updated_names or '無'}；快取略過：{cached_names}"
        else:
            message = f"{updated_names}更新完成"
        _set_state(
            running=False,
            step="completed",
            message=message,
            current_index=len(modules),
            completed_modules=list(completed),
            skipped_modules=list(skipped),
            results=dict(results),
            finished_at=datetime.now().isoformat(timespec="seconds"),
        )
    except Exception as exc:
        if getattr(exc, "is_data_pending", False):
            _set_state(
                running=False,
                step="pending",
                message=str(exc),
                error=None,
                completed_modules=list(completed),
                skipped_modules=list(skipped),
                results=dict(results),
                finished_at=datetime.now().isoformat(timespec="seconds"),
            )
        else:
            traceback.print_exc()
            _set_state(
                running=False,
                step="failed",
                message=f"更新失敗：{exc}",
                error=str(exc),
                completed_modules=list(completed),
                skipped_modules=list(skipped),
                results=dict(results),
                finished_at=datetime.now().isoformat(timespec="seconds"),
            )
    finally:
        _LOCK.release()


def start_report_update(
    modules: list[str] | tuple[str, ...] | None = None,
    *,
    force: bool = False,
) -> dict:
    ordered = _normalize_modules(modules)
    if not _LOCK.acquire(blocking=False):
        return get_report_update_status()
    _set_state(
        running=True,
        step="starting",
        message="正在準備更新…",
        started_at=datetime.now().isoformat(timespec="seconds"),
        finished_at=None,
        error=None,
        modules=list(ordered),
        completed_modules=[],
        skipped_modules=[],
        results={},
        current_index=0,
        total_modules=len(ordered),
        force=bool(force),
    )
    threading.Thread(
        target=_run,
        args=(ordered, bool(force)),
        name="dashboard-global-update",
        daemon=True,
    ).start()
    return get_report_update_status()
