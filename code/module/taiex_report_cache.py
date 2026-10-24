"""Dashboard 專用的加權報表實體快取。

複雜報表 View 含多層視窗函式與策略判斷，不應在每次翻頁時重算。
初始化只能由加權報表更新流程或獨立初始化指令觸發；Dashboard 啟動與
一般查詢絕不建立、刷新或重建快取。
"""
from __future__ import annotations

import time

from common import db


SOURCE_VIEW = "report.v_taiex_report_turnover"
CACHE_TABLE = "report.taiex_report_dashboard_cache_v2"
REFRESH_DAYS = 400


def _columns(relation: str) -> list[str]:
    frame = db.query_to_df(
        """
        SELECT attname
        FROM pg_attribute
        WHERE attrelid = ?::regclass
          AND attnum > 0
          AND NOT attisdropped
        ORDER BY attnum
        """,
        (relation,),
    )
    return frame["attname"].tolist() if frame is not None and not frame.empty else []


def taiex_dashboard_cache_available() -> bool:
    """只檢查實體表是否存在，不執行 DDL 或資料刷新。"""
    return db.query_single_value("SELECT to_regclass(?)", (CACHE_TABLE,)) is not None


def initialize_taiex_dashboard_cache() -> int:
    """一次性建立並填滿永久快取；已存在時完全不重建。"""
    if taiex_dashboard_cache_available():
        return int(db.query_single_value(f"SELECT COUNT(*) FROM {CACHE_TABLE}") or 0)

    source_columns = _columns(SOURCE_VIEW)
    if not source_columns:
        raise RuntimeError("加權報表來源 View 沒有可用欄位")

    started = time.perf_counter()
    print("[加權快取] 正在一次性建立永久快取表…")
    if not db.execute_sql(
        f"""
        CREATE TABLE {CACHE_TABLE} AS
        SELECT * FROM {SOURCE_VIEW};
        CREATE UNIQUE INDEX uq_taiex_report_dashboard_cache_v2_date
            ON {CACHE_TABLE} ("日期");
        ANALYZE {CACHE_TABLE};
        """
    ):
        raise RuntimeError("無法建立加權 Dashboard 永久快取表")
    row_count = int(db.query_single_value(f"SELECT COUNT(*) FROM {CACHE_TABLE}") or 0)
    print(f"[加權快取] 永久快取建立完成，共 {row_count:,} 筆，耗時 {time.perf_counter() - started:.2f} 秒")
    return row_count


def refresh_taiex_dashboard_cache(
    recent_days: int = REFRESH_DAYS,
    *,
    full_rebuild: bool = False,
) -> int:
    """刷新 Dashboard 快取。

    日常更新只刷新近期尾端；計算規則升級時才完整同步一次，避免歷史列
    留著舊公式，同時避免之後每次進 Dashboard 都重算整份報表。
    """
    if not taiex_dashboard_cache_available():
        return initialize_taiex_dashboard_cache()

    started = time.perf_counter()
    if full_rebuild:
        if not db.execute_sql(
            f"""
            DO $refresh$
            BEGIN
              PERFORM pg_advisory_xact_lock(hashtext('{CACHE_TABLE}'));
              TRUNCATE TABLE {CACHE_TABLE};
              INSERT INTO {CACHE_TABLE} SELECT * FROM {SOURCE_VIEW};
            END $refresh$;
            ANALYZE {CACHE_TABLE};
            """
        ):
            raise RuntimeError("無法完整刷新加權 Dashboard 快取")
        row_count = int(db.query_single_value(f"SELECT COUNT(*) FROM {CACHE_TABLE}") or 0)
        print(f"[加權快取] 規則升級完整刷新，共 {row_count:,} 筆，耗時 {time.perf_counter() - started:.2f} 秒")
        return row_count

    days = max(1, int(recent_days))
    if not db.execute_sql(
        f"""
        DO $refresh$
        DECLARE refresh_start date;
        BEGIN
          PERFORM pg_advisory_xact_lock(hashtext('{CACHE_TABLE}'));
          SELECT COALESCE(MAX("日期"), CURRENT_DATE) - {days}
            INTO refresh_start
            FROM {CACHE_TABLE};
          DELETE FROM {CACHE_TABLE} WHERE "日期" >= refresh_start;
          INSERT INTO {CACHE_TABLE}
          SELECT * FROM {SOURCE_VIEW} WHERE "日期" >= refresh_start;
        END $refresh$;
        ANALYZE {CACHE_TABLE};
        """
    ):
        raise RuntimeError("無法刷新加權 Dashboard 快取")
    row_count = int(db.query_single_value(f"SELECT COUNT(*) FROM {CACHE_TABLE}") or 0)
    print(f"[加權快取] 近期 {days} 日刷新完成，共 {row_count:,} 筆，耗時 {time.perf_counter() - started:.2f} 秒")
    return row_count
