"""加權報表成交金額規則的一次性全歷史升級標記。"""
from __future__ import annotations

from common import db


RULE_NAME = "taiex-price-volume-basis"
# 這個版本只代表「價量欄位改用成交金額計算」。欄位排列是 View 的版面
# 問題，不能再改動此版本，否則日常更新會誤判成需要重算 1999 年至今。
# v3 才完整涵蓋最大量／均量／量增率／價量等所有量能衍生欄位。
# v1、v2 可能只更新部分公式，不能視為相容，否則歷史列不會重算。
RULE_VERSION = "2026-10-23-v3-all-volume-derived-from-trading-money"
COMPATIBLE_RULE_VERSIONS = {RULE_VERSION}
_VIEW_READY = False


def ensure_rule_table() -> None:
    if db.query_single_value("SELECT to_regclass('report.report_rule_versions')") is None and not db.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS report.report_rule_versions (
            rule_name text PRIMARY KEY,
            rule_version text NOT NULL,
            applied_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    ):
        raise RuntimeError("無法建立報表規則版本表")

    ensure_turnover_view()


def ensure_turnover_view() -> None:
    """只在 View 不存在或欄位版本過舊時建立，不在查詢時反覆重建。"""
    global _VIEW_READY
    if _VIEW_READY:
        return

    order_is_correct = bool(db.query_single_value(
        """
        WITH positions AS (
          SELECT column_name, ordinal_position
          FROM information_schema.columns
          WHERE table_schema = 'report'
            AND table_name = 'v_taiex_report_turnover'
        )
        SELECT
          (SELECT ordinal_position FROM positions WHERE column_name = '成交金額') =
          (SELECT ordinal_position FROM positions WHERE column_name = '成交股數') + 1
          AND
          (SELECT ordinal_position FROM positions WHERE column_name = '總成交金額_億') =
          (SELECT ordinal_position FROM positions WHERE column_name = '布林下軌') + 1
        """
    ))
    if order_is_correct:
        _VIEW_READY = True
        return

    # PostgreSQL 的 CREATE OR REPLACE VIEW 不能調整既有欄位順序，因此只在
    # 欄位順序錯誤時重建一次。基底 View 原有的「總成交金額_億」位置會保留，
    # 也就是緊接在「布林下軌」之後。
    if not db.execute_sql(
        """
        DO $turnover$
        DECLARE fields text;
        BEGIN
          PERFORM pg_advisory_xact_lock(hashtext('report.v_taiex_report_turnover'));
          SELECT string_agg(
            CASE
              WHEN attname = '成交量' THEN
                format('b.%I AS %I, b.%I AS %I',
                       '成交量', '成交股數', '總成交金額_億', '成交金額')
              ELSE format('b.%I', attname)
            END,
            ', ' ORDER BY attnum
          ) INTO fields
          FROM pg_attribute
          WHERE attrelid = 'report.v_taiex_report'::regclass
            AND attnum > 0 AND NOT attisdropped;
          DROP VIEW IF EXISTS report.v_taiex_report_turnover;
          EXECUTE 'CREATE VIEW report.v_taiex_report_turnover AS SELECT '
                  || fields || ' FROM report.v_taiex_report b';
        END $turnover$;
        """
    ):
        raise RuntimeError("無法建立成交金額版加權報表 View")
    _VIEW_READY = True


def taiex_base_view_uses_trading_money() -> bool:
    """確認 View 的量能視窗公式已經使用總成交金額，而非成交股數。"""
    definition = db.query_single_value(
        "SELECT pg_get_viewdef('report.v_taiex_report_base'::regclass, true)"
    )
    if not definition:
        return False
    definition = str(definition)
    required = (
        'lag(b."總成交金額_億", 1)',
        'avg(b."總成交金額_億")',
        'max(x."總成交金額_億")',
        'c."總成交金額_億" - c."昨量"',
        'c."總成交金額_億" - c."VOLMA5_calc"',
    )
    forbidden = (
        'lag(b."成交量", 1)',
        'avg(b."成交量")',
        'max(x."成交量")',
        'c."成交量" - c."昨量"',
        'c."成交量" - c."VOLMA5_calc"',
    )
    return all(token in definition for token in required) and not any(
        token in definition for token in forbidden
    )


def migrate_taiex_base_view_to_trading_money() -> None:
    """一次性修正 v_taiex_report_base 內仍引用成交股數的量能公式。

    保留輸出欄位「成交量」作為成交股數；只替換計算運算元，因此不會
    改變既有 View 欄位順序、名稱或相依 View 的介面。
    """
    if taiex_base_view_uses_trading_money():
        print("[加權量能] SQL View 已使用成交金額，略過遷移。")
        return

    ok = db.execute_sql(
        r'''
        DO $migration$
        DECLARE
          definition text;
        BEGIN
          PERFORM pg_advisory_xact_lock(hashtext('report.v_taiex_report_base:turnover-v3'));
          definition := pg_get_viewdef('report.v_taiex_report_base'::regclass, true);

          definition := replace(definition,
            'lag(b."成交量", 1)', 'lag(b."總成交金額_億", 1)');
          definition := replace(definition,
            'count(b."成交量")', 'count(b."總成交金額_億")');
          definition := replace(definition,
            'avg(b."成交量")', 'avg(b."總成交金額_億")');
          definition := replace(definition,
            'max(x."成交量")', 'max(x."總成交金額_億")');
          definition := replace(definition,
            'ORDER BY x."成交量" DESC', 'ORDER BY x."總成交金額_億" DESC');
          definition := replace(definition,
            'c."成交量" - c."昨量"', 'c."總成交金額_億" - c."昨量"');
          definition := replace(definition,
            'c."成交量" - c."VOLMA5_calc"', 'c."總成交金額_億" - c."VOLMA5_calc"');
          definition := replace(definition,
            'c."成交量" - c."VOLMA10_calc"', 'c."總成交金額_億" - c."VOLMA10_calc"');
          definition := replace(definition,
            'c."成交量" - c."VOLMA20_calc"', 'c."總成交金額_億" - c."VOLMA20_calc"');
          definition := replace(definition,
            'c."成交量" - c."VOLMA60_calc"', 'c."總成交金額_億" - c."VOLMA60_calc"');

          EXECUTE 'CREATE OR REPLACE VIEW report.v_taiex_report_base AS ' || definition;
        END $migration$;
        ''',
    )
    if not ok or not taiex_base_view_uses_trading_money():
        raise RuntimeError("加權報表 SQL View 的成交金額量能公式遷移失敗")
    print("[加權量能] SQL View 已全面改用總成交金額_億。")


def needs_full_rebuild() -> bool:
    ensure_rule_table()
    current = db.query_single_value(
        "SELECT rule_version FROM report.report_rule_versions WHERE rule_name = ?",
        (RULE_NAME,),
    )
    return current not in COMPATIBLE_RULE_VERSIONS


def mark_applied() -> None:
    if not db.execute_sql(
        """
        INSERT INTO report.report_rule_versions (rule_name, rule_version, applied_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (rule_name) DO UPDATE SET
            rule_version = excluded.rule_version,
            applied_at = excluded.applied_at
        """,
        (RULE_NAME, RULE_VERSION),
    ):
        raise RuntimeError("無法寫入加權成交金額規則版本")
