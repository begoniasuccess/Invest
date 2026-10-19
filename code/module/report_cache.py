from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

import pandas as pd

from common import db
from module.core_strategy import CORE_STRATEGY_RULE_VERSION


FUTURES_CACHE = "report.mv_futures_report_daily_base"
FUTURES_TAIL_CACHE = "report.futures_report_daily_tail_cache"
FUTURES_DASHBOARD_CACHE = "report.futures_report_daily_dashboard_cache"


@dataclass(frozen=True)
class CacheRefreshResult:
    refreshed: bool
    row_count: int
    elapsed_seconds: float
    message: str


def futures_cache_available() -> bool:
    value = db.query_single_value("SELECT to_regclass(?)", (FUTURES_CACHE,))
    return value is not None


def ensure_futures_report_live_view() -> bool:
    """讓正式期貨 View 採「物化歷史＋小型尾端快取」。

    最新資料只在報表更新時算一次並寫入尾端表；Dashboard 每次載入不再
    重算 live base。既有資料庫若已使用 live_tail，會自動切換為尾端表。
    """
    # The turning overlay keeps the original dependency graph under this name.
    target = "report.v_futures_report_daily_without_turning" if db.query_single_value(
        "SELECT to_regclass('report.v_futures_report_daily_without_turning')"
    ) else "report.v_futures_report_daily"
    if not db.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {FUTURES_TAIL_CACHE}
        (LIKE {FUTURES_CACHE} INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_futures_report_daily_tail_cache_id
            ON {FUTURES_TAIL_CACHE} (id);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_futures_report_daily_tail_cache_market_date
            ON {FUTURES_TAIL_CACHE} (futures_id, "日期");
        """
    ):
        raise RuntimeError("建立期貨報表尾端快取失敗")

    view_definition = str(db.query_single_value(
        f"""
        SELECT pg_get_viewdef('{target}'::regclass, true)
        """
    ) or "")
    if "futures_report_daily_tail_cache" in view_definition:
        return False

    ok = db.execute_sql(
        f"""
        DO $migration$
        DECLARE
            view_sql text;
        BEGIN
            view_sql := pg_get_viewdef(
                '{target}'::regclass,
                true
            );

            -- 已套用舊版 live_tail：只把昂貴的 live View 換成尾端快取表。
            IF position('report.v_futures_report_daily_base live_tail' IN view_sql) > 0 THEN
                view_sql := replace(
                    view_sql,
                    'report.v_futures_report_daily_base live_tail',
                    'report.futures_report_daily_tail_cache live_tail'
                );
            ELSIF position('v_futures_report_daily_base live_tail' IN view_sql) > 0 THEN
                view_sql := replace(
                    view_sql,
                    'v_futures_report_daily_base live_tail',
                    'report.futures_report_daily_tail_cache live_tail'
                );
            -- 尚未套用混合 View：在既有物化歷史後接上尾端快取。
            ELSIF position('report.mv_futures_report_daily_base cache' IN view_sql) > 0 THEN
                view_sql := replace(
                    view_sql,
                    'report.mv_futures_report_daily_base cache',
                    '(SELECT history.* FROM report.mv_futures_report_daily_base history '
                    'UNION ALL '
                    'SELECT live_tail.* FROM report.futures_report_daily_tail_cache live_tail '
                    'WHERE live_tail."日期" > COALESCE('
                    '(SELECT MAX(history_max."日期") FROM report.mv_futures_report_daily_base history_max), '
                    'DATE ''1900-01-01'')) cache'
                );
            ELSE
                view_sql := replace(
                    view_sql,
                    'mv_futures_report_daily_base cache',
                    '(SELECT history.* FROM report.mv_futures_report_daily_base history '
                    'UNION ALL '
                    'SELECT live_tail.* FROM report.futures_report_daily_tail_cache live_tail '
                    'WHERE live_tail."日期" > COALESCE('
                    '(SELECT MAX(history_max."日期") FROM report.mv_futures_report_daily_base history_max), '
                    'DATE ''1900-01-01'')) cache'
                );
            END IF;

            IF view_sql NOT LIKE '%futures_report_daily_tail_cache%' THEN
                RAISE EXCEPTION '找不到期貨報表來源，未切換尾端快取';
            END IF;
            EXECUTE 'CREATE OR REPLACE VIEW {target} AS ' || view_sql;
            COMMENT ON VIEW report.v_futures_report_daily IS
                '期貨指數混合快取報表；物化歷史加小型尾端快取，頁面載入不重算 live base。';
        END
        $migration$;
        """
    )
    if not ok:
        raise RuntimeError("期貨正式 View 切換即時來源失敗")
    return True


def refresh_futures_report_tail_cache() -> int:
    """重建物化歷史之後的少量新交易日，供 Dashboard 快速讀取。"""
    if not db.execute_sql(
        f"""
        DELETE FROM {FUTURES_TAIL_CACHE};
        INSERT INTO {FUTURES_TAIL_CACHE}
        SELECT live_row.*
        FROM report.v_futures_report_daily_base live_row
        WHERE live_row."日期" > COALESCE(
            (SELECT MAX(history_row."日期") FROM {FUTURES_CACHE} history_row),
            DATE '1900-01-01'
        );
        ANALYZE {FUTURES_TAIL_CACHE};
        """
    ):
        raise RuntimeError("刷新期貨報表尾端快取失敗")
    return int(db.query_single_value(f"SELECT COUNT(*) FROM {FUTURES_TAIL_CACHE}") or 0)


def refresh_futures_dashboard_cache(recent_days: int = 14, *, force_full: bool = False) -> int:
    """更新 Dashboard 專用實體快取，避免頁面反覆計算策略 View。

    首次建立時寫入完整正式報表；日常只改寫最近 recent_days 日曆日。
    核心規則改版時只原地重算全歷史的三個核心欄位，不重跑昂貴 View。
    正式策略 View 會自行保留完整歷史視窗，因此近期列的 LAG 計算仍正確。
    """
    if not db.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {FUTURES_DASHBOARD_CACHE} AS
        SELECT *
        FROM report.v_futures_report_daily_corrected
        WITH NO DATA;

        CREATE UNIQUE INDEX IF NOT EXISTS uq_futures_dashboard_cache_id
            ON {FUTURES_DASHBOARD_CACHE} (id);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_futures_dashboard_cache_market_date
            ON {FUTURES_DASHBOARD_CACHE} (futures_id, "日期");
        CREATE INDEX IF NOT EXISTS idx_futures_dashboard_cache_date
            ON {FUTURES_DASHBOARD_CACHE} ("日期" DESC);
        """
    ):
        raise RuntimeError("建立 Dashboard 期貨報表快取失敗")

    cache_comment = str(
        db.query_single_value(
            "SELECT obj_description(to_regclass(?), 'pg_class')",
            (FUTURES_DASHBOARD_CACHE,),
        )
        or ""
    )
    rebuild_core_columns = force_full or CORE_STRATEGY_RULE_VERSION not in cache_comment
    refresh_days = max(1, int(recent_days))
    if not db.execute_sql(
        f"""
        DO $refresh$
        DECLARE
            cache_is_empty boolean;
            refresh_start date;
        BEGIN
            SELECT NOT EXISTS (SELECT 1 FROM {FUTURES_DASHBOARD_CACHE})
              INTO cache_is_empty;

            IF cache_is_empty THEN
                INSERT INTO {FUTURES_DASHBOARD_CACHE}
                SELECT * FROM report.v_futures_report_daily_corrected;
            ELSIF {str(bool(rebuild_core_columns)).lower()} THEN
                UPDATE {FUTURES_DASHBOARD_CACHE}
                   SET "策略_核心_昨價量" = public.fn_report_core_strategy(
                           "策略_全價量昨日", "策略_線型", "全位置",
                           "策略_籌碼", "法人籌碼"
                       ),
                       "策略_核心_五日價量" = public.fn_report_core_strategy(
                           "策略_全價量五日", "策略_線型", "全位置",
                           "策略_籌碼", "法人籌碼"
                       );
            ELSE
                SELECT COALESCE(MAX("日期"), CURRENT_DATE) - {refresh_days}
                  INTO refresh_start
                  FROM {FUTURES_DASHBOARD_CACHE};

                DELETE FROM {FUTURES_DASHBOARD_CACHE}
                 WHERE "日期" >= refresh_start;

                INSERT INTO {FUTURES_DASHBOARD_CACHE}
                SELECT *
                  FROM report.v_futures_report_daily_corrected
                 WHERE "日期" >= refresh_start;
            END IF;

            COMMENT ON TABLE {FUTURES_DASHBOARD_CACHE} IS
                'Dashboard 期貨日報表實體快取；核心策略版本 {CORE_STRATEGY_RULE_VERSION}。';
        END
        $refresh$;

        ANALYZE {FUTURES_DASHBOARD_CACHE};
        """
    ):
        raise RuntimeError("刷新 Dashboard 期貨報表快取失敗")

    return int(db.query_single_value(f"SELECT COUNT(*) FROM {FUTURES_DASHBOARD_CACHE}") or 0)


def refresh_futures_dashboard_cache_dates(dates) -> int:
    """只刷新指定交易日，供歷史缺漏修補後同步 Dashboard。"""
    date_values = sorted({pd.Timestamp(value).strftime("%Y-%m-%d") for value in dates})
    if not date_values:
        return 0

    # 日期先經 pandas 正規化，這裡產生的 SQL literal 不含外部原始字串。
    date_sql = ", ".join(f"DATE '{value}'" for value in date_values)
    if db.query_single_value("SELECT to_regclass(?)", (FUTURES_DASHBOARD_CACHE,)) is None:
        refresh_futures_dashboard_cache()

    if not db.execute_sql(
        f"""
        DELETE FROM {FUTURES_DASHBOARD_CACHE}
         WHERE "日期" IN ({date_sql});

        INSERT INTO {FUTURES_DASHBOARD_CACHE}
        SELECT *
          FROM report.v_futures_report_daily_corrected
         WHERE "日期" IN ({date_sql});

        ANALYZE {FUTURES_DASHBOARD_CACHE};
        """
    ):
        raise RuntimeError("刷新指定日期的 Dashboard 期貨報表快取失敗")
    return len(date_values)


def refresh_futures_report_cache(*, concurrently: bool = True) -> CacheRefreshResult:
    """刷新期貨報表物化快取；一般讀取仍可使用上一版資料。"""
    if not futures_cache_available():
        return CacheRefreshResult(
            refreshed=False,
            row_count=0,
            elapsed_seconds=0.0,
            message="尚未建立期貨報表物化快取，請先執行 futures_report_performance.sql",
        )

    started = perf_counter()
    mode = "CONCURRENTLY " if concurrently else ""
    db.execute_sql_autocommit(f"REFRESH MATERIALIZED VIEW {mode}{FUTURES_CACHE}")
    db.execute_sql_autocommit(f"ANALYZE {FUTURES_CACHE}")
    elapsed = perf_counter() - started
    row_count = int(db.query_single_value(f"SELECT COUNT(*) FROM {FUTURES_CACHE}") or 0)

    db.execute_sql(
        """
        INSERT INTO report_cache_status (
            cache_name, refreshed_at, row_count, elapsed_seconds
        ) VALUES (?, CURRENT_TIMESTAMP, ?, ?)
        ON CONFLICT (cache_name) DO UPDATE SET
            refreshed_at = EXCLUDED.refreshed_at,
            row_count = EXCLUDED.row_count,
            elapsed_seconds = EXCLUDED.elapsed_seconds
        """,
        (FUTURES_CACHE, row_count, elapsed),
    )
    return CacheRefreshResult(
        refreshed=True,
        row_count=row_count,
        elapsed_seconds=elapsed,
        message=f"期貨報表快取刷新完成：{row_count:,} 筆，{elapsed:.2f} 秒",
    )
