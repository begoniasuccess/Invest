from __future__ import annotations

import threading
import traceback
from datetime import datetime


UPDATE_ORDER = (
    "taiex_report",
    "taiex_kbar",
    "futures_report",
    "futures_kbar",
)
UPDATE_NAMES = {
    "taiex_report": "加權指數報表",
    "taiex_kbar": "加權指數分 K 資料",
    "futures_report": "期貨指數報表",
    "futures_kbar": "期貨指數分 K 資料",
}

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
}


def _set_state(**values) -> None:
    with _STATE_LOCK:
        _STATE.update(values)


def get_report_update_status() -> dict:
    with _STATE_LOCK:
        return dict(_STATE)


def _run_module(module: str) -> None:
    if module == "taiex_report":
        from main.stock_report_taiex import run_report_pipeline

        run_report_pipeline()
    elif module == "taiex_kbar":
        from main.intraday_pipeline import update_taiex_kbar_1m

        update_taiex_kbar_1m(datetime.now())
    elif module == "futures_report":
        from main.futures import run_report_pipeline

        # 日常更新只補近期來源與 raw，不刷新整張歷史物化快取。
        # Dashboard 的期貨表改讀 live View，因此新交易日仍會立即出現。
        run_report_pipeline(refresh_cache=False, export_csv=False)
    elif module == "futures_kbar":
        from main.intraday_pipeline import update_tx_kbar_1m

        update_tx_kbar_1m(datetime.now())


def _run(modules: tuple[str, ...]) -> None:
    completed: list[str] = []
    try:
        for module in modules:
            _set_state(step=module, message=f"正在更新{UPDATE_NAMES[module]}…")
            _run_module(module)
            completed.append(module)
            _set_state(completed_modules=list(completed))
        names = "、".join(UPDATE_NAMES[module] for module in modules)
        _set_state(
            running=False,
            step="completed",
            message=f"{names}更新完成",
            finished_at=datetime.now().isoformat(timespec="seconds"),
        )
    except Exception as exc:
        traceback.print_exc()
        _set_state(
            running=False,
            step="failed",
            message=f"更新失敗：{exc}",
            error=str(exc),
            finished_at=datetime.now().isoformat(timespec="seconds"),
        )
    finally:
        _LOCK.release()


def start_report_update(modules: list[str] | tuple[str, ...] | None = None) -> dict:
    requested = set(modules or ("taiex_report", "futures_report"))
    unknown = requested.difference(UPDATE_NAMES)
    if unknown:
        raise ValueError(f"不支援的更新模組：{', '.join(sorted(unknown))}")
    ordered = tuple(module for module in UPDATE_ORDER if module in requested)
    if not ordered:
        raise ValueError("請至少選擇一個更新模組")
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
    )
    threading.Thread(
        target=_run,
        args=(ordered,),
        name="dashboard-report-update",
        daemon=True,
    ).start()
    return get_report_update_status()
