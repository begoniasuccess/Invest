from __future__ import annotations

from common import db


CORE_STRATEGY_RULE_VERSION = "2026-09-15-v2"


def ensure_core_strategy_rule() -> bool:
    """套用加權與期貨共用的核心策略判定函式。

    回傳 True 代表本次有更新資料庫函式；相同版本重跑不會重建。
    """
    current_comment = str(
        db.query_single_value(
            """
            SELECT obj_description(
                to_regprocedure('public.fn_report_core_strategy(text,text,text,text,text)'),
                'pg_proc'
            )
            """
        )
        or ""
    )
    if CORE_STRATEGY_RULE_VERSION in current_comment:
        return False

    ok = db.execute_sql(
        f"""
        CREATE OR REPLACE FUNCTION public.fn_report_core_strategy(
            price_strategy text,
            trend_value text,
            position_value text,
            chip_strategy text,
            institutional_chip text
        ) RETURNS text
        LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $function$
            WITH normalized AS (
                SELECT
                    CASE price_strategy
                        WHEN '多' THEN 1
                        WHEN '空' THEN -1
                        WHEN '無' THEN 0
                    END AS price_score,
                    CASE
                        WHEN trend_value = '多' THEN 1
                        WHEN trend_value = '空' THEN -1
                        -- 盤整時必須搭配位置判斷：高檔偏空、低檔偏多。
                        WHEN trend_value = '盤整' AND position_value = '高檔' THEN -1
                        WHEN trend_value = '盤整' AND position_value = '中間' THEN 0
                        WHEN trend_value = '盤整' AND position_value = '低檔' THEN 1
                    END AS line_score,
                    CASE
                        WHEN chip_strategy = '多' THEN 1
                        WHEN chip_strategy = '空' THEN -1
                        WHEN chip_strategy = '無' AND institutional_chip = '買超' THEN 1
                        WHEN chip_strategy = '無' AND institutional_chip = '賣超' THEN -1
                    END AS chip_score
            ), scored AS (
                SELECT *, price_score + line_score + chip_score AS total_score
                FROM normalized
            )
            SELECT CASE
                WHEN price_strategy NOT IN ('多', '空', '無')
                  OR trend_value NOT IN ('多', '空', '盤整')
                  OR position_value NOT IN ('高檔', '中間', '低檔')
                  OR chip_strategy NOT IN ('多', '空', '無')
                  OR price_score IS NULL OR line_score IS NULL OR chip_score IS NULL
                THEN '資料不足'
                WHEN total_score > 0 THEN '多'
                WHEN total_score < 0 THEN '空'
                -- 平手時依圖表指定順序：籌碼 > 線型 > 價量。
                WHEN chip_score > 0 THEN '多'
                WHEN chip_score < 0 THEN '空'
                WHEN line_score > 0 THEN '多'
                WHEN line_score < 0 THEN '空'
                WHEN price_score > 0 THEN '多'
                WHEN price_score < 0 THEN '空'
                ELSE '資料不足'
            END
            FROM scored
        $function$;

        COMMENT ON FUNCTION public.fn_report_core_strategy(text, text, text, text, text) IS
        '{CORE_STRATEGY_RULE_VERSION} 核心策略：盤整依位置判定（高空、中無、低多）；平手依籌碼、線型、價量決勝。';
        """
    )
    if not ok:
        raise RuntimeError("更新加權／期貨核心策略判定函式失敗")
    return True
