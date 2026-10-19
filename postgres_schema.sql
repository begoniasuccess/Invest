--
-- PostgreSQL database dump
--

\restrict edyDRaZ5f6ChCtkS94jtKNHqmD1jz9zKHx7XrG9c5KIVJdbWld1zuqTBQdZqUKi

-- Dumped from database version 18.4
-- Dumped by pg_dump version 18.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: backtest; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA backtest;


--
-- Name: core; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA core;


--
-- Name: SCHEMA core; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA core IS '清理整合後的核心資料、報表原始表及系統狀態';


--
-- Name: ml; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA ml;


--
-- Name: SCHEMA ml; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA ml IS '機器學習模型、版本、執行紀錄與預測';


--
-- Name: raw_exchange; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA raw_exchange;


--
-- Name: SCHEMA raw_exchange; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA raw_exchange IS 'TWSE、TPEx、TAIFEX 等交易所原始資料';


--
-- Name: raw_finmind; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA raw_finmind;


--
-- Name: SCHEMA raw_finmind; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA raw_finmind IS 'FinMind API 原始資料及由其直接產生的 Tick／分鐘K';


--
-- Name: report; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA report;


--
-- Name: SCHEMA report; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA report IS '正式報表 View 與 Materialized View';


--
-- Name: research; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA research;


--
-- Name: SCHEMA research; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA research IS '可重建的研究、分層統計與策略實驗結果；不存放原始行情。';


--
-- Name: fn_futures_core_strategy(text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.fn_futures_core_strategy(price_strategy text, trend_value text, position_value text, chip_strategy text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT CASE
        WHEN price_strategy IS NULL OR price_strategy = '資料不足'
          OR trend_value NOT IN ('多', '空', '盤整')
          OR position_value NOT IN ('高檔', '中間', '低檔')
          OR chip_strategy NOT IN ('多', '空', '無') THEN '資料不足'
        WHEN price_strategy = '無' THEN '無'
        WHEN price_strategy = '多' AND trend_value = '多' THEN '多'
        WHEN price_strategy = '多' AND trend_value = '空' THEN
             CASE WHEN chip_strategy = '多' THEN '多' ELSE '空' END
        WHEN price_strategy = '多' AND trend_value = '盤整' AND position_value = '高檔' THEN
             CASE WHEN chip_strategy = '多' THEN '多' ELSE '空' END
        WHEN price_strategy = '多' AND trend_value = '盤整' AND position_value = '中間' THEN
             CASE WHEN chip_strategy = '空' THEN '空' ELSE '多' END
        WHEN price_strategy = '多' AND trend_value = '盤整' AND position_value = '低檔' THEN '多'
        WHEN price_strategy = '空' AND trend_value = '多' THEN
             CASE WHEN chip_strategy = '空' THEN '空' ELSE '多' END
        WHEN price_strategy = '空' AND trend_value = '空' THEN '空'
        WHEN price_strategy = '空' AND trend_value = '盤整' AND position_value = '高檔' THEN '空'
        WHEN price_strategy = '空' AND trend_value = '盤整' AND position_value = '中間' THEN
             CASE WHEN chip_strategy = '多' THEN '多' ELSE '空' END
        WHEN price_strategy = '空' AND trend_value = '盤整' AND position_value = '低檔' THEN
             CASE WHEN chip_strategy = '空' THEN '空' ELSE '多' END
        ELSE '資料不足'
    END
$$;


--
-- Name: fn_futures_full_position(text, numeric, numeric, numeric, numeric); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.fn_futures_full_position(trend_value text, close_value numeric, ma5_value numeric, bollinger_upper numeric, bollinger_lower numeric) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT public.fn_report_trend_position(
        trend_value, close_value, ma5_value, bollinger_upper, bollinger_lower
    )
$$;


--
-- Name: fn_futures_full_trend(text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.fn_futures_full_trend(direction_5 text, direction_10 text, direction_20 text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT CASE
        WHEN direction_5 IS NULL OR direction_10 IS NULL OR direction_20 IS NULL THEN '資料不足'
        WHEN direction_5 = '上' AND direction_10 = '上' AND direction_20 = '上' THEN '多'
        WHEN direction_5 = '上' AND direction_10 = '上' AND direction_20 = '下' THEN '多'
        WHEN direction_5 = '上' AND direction_10 = '下' AND direction_20 = '上' THEN '盤整'
        WHEN direction_5 = '上' AND direction_10 = '下' AND direction_20 = '下' THEN '空'
        WHEN direction_5 = '下' AND direction_10 = '上' AND direction_20 = '上' THEN '多'
        WHEN direction_5 = '下' AND direction_10 = '上' AND direction_20 = '下' THEN '盤整'
        WHEN direction_5 = '下' AND direction_10 = '下' AND direction_20 = '上' THEN '空'
        WHEN direction_5 = '下' AND direction_10 = '下' AND direction_20 = '下' THEN '空'
        ELSE '資料不足'
    END
$$;


--
-- Name: fn_futures_price_chip_strategy(text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.fn_futures_price_chip_strategy(price_strategy text, chip_strategy text, institutional_chip text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT CASE
        WHEN price_strategy IS NULL OR price_strategy = '資料不足'
          OR chip_strategy IS NULL THEN '資料不足'
        WHEN price_strategy = '無' THEN '無'
        WHEN chip_strategy = '無' AND institutional_chip IS NULL THEN '資料不足'
        WHEN price_strategy = '多' AND chip_strategy = '多' THEN '多'
        WHEN price_strategy = '空' AND chip_strategy = '空' THEN '空'
        WHEN price_strategy = '多' AND chip_strategy = '無' AND institutional_chip = '買超' THEN '多'
        WHEN price_strategy = '空' AND chip_strategy = '無' AND institutional_chip = '賣超' THEN '空'
        WHEN price_strategy IN ('多', '空') AND chip_strategy = '無'
             AND institutional_chip NOT IN ('買超', '賣超') THEN '無'
        WHEN price_strategy IN ('多', '空') AND chip_strategy IN ('多', '空', '無') THEN 'X'
        ELSE '資料不足'
    END
$$;


--
-- Name: fn_futures_price_line_strategy(text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.fn_futures_price_line_strategy(price_strategy text, trend_value text, position_value text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT CASE
        WHEN price_strategy IS NULL OR price_strategy = '資料不足'
          OR trend_value NOT IN ('多', '空', '盤整')
          OR position_value NOT IN ('高檔', '中間', '低檔') THEN '資料不足'
        WHEN price_strategy = '無' THEN '無'
        WHEN price_strategy = '多' AND trend_value = '多' THEN '多'
        WHEN price_strategy = '多' AND trend_value = '盤整' AND position_value = '低檔' THEN '多'
        WHEN price_strategy = '多' THEN 'X'
        WHEN price_strategy = '空' AND trend_value = '空' THEN '空'
        WHEN price_strategy = '空' AND trend_value = '盤整' AND position_value = '高檔' THEN '空'
        WHEN price_strategy = '空' THEN 'X'
        ELSE '資料不足'
    END
$$;


--
-- Name: fn_futures_price_volume_strategy(text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.fn_futures_price_volume_strategy(price_value text, volume_value text, trend_value text, position_value text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT CASE
        WHEN price_value IS NULL OR volume_value IS NULL
          OR trend_value NOT IN ('多', '空', '盤整')
          OR position_value NOT IN ('高檔', '中間', '低檔')
        THEN '資料不足'
        -- 價同或量同代表中性，不是資料缺失。
        WHEN price_value = '價同' OR volume_value = '量同' THEN '無'
        WHEN price_value NOT IN ('價漲', '價跌')
          OR volume_value NOT IN ('量增', '量縮')
        THEN '資料不足'
        WHEN price_value = '價漲' AND volume_value = '量增' AND trend_value = '多' THEN '多'
        WHEN price_value = '價漲' AND volume_value = '量增' AND trend_value = '盤整'
             AND position_value IN ('低檔', '中間') THEN '多'
        WHEN price_value = '價漲' AND volume_value = '量增' AND trend_value = '盤整'
             AND position_value = '高檔' THEN '空'
        WHEN price_value = '價漲' AND volume_value = '量增' AND trend_value = '空' THEN '多'
        WHEN price_value = '價漲' AND volume_value = '量縮' AND trend_value = '多'
             AND position_value IN ('低檔', '中間') THEN '多'
        WHEN price_value = '價漲' AND volume_value = '量縮' AND trend_value = '多'
             AND position_value = '高檔' THEN '空'
        WHEN price_value = '價漲' AND volume_value = '量縮' AND trend_value IN ('盤整', '空') THEN '空'
        WHEN price_value = '價跌' AND volume_value = '量增' AND trend_value = '多'
             AND position_value = '低檔' THEN '多'
        WHEN price_value = '價跌' AND volume_value = '量增' AND trend_value = '多'
             AND position_value IN ('中間', '高檔') THEN '空'
        WHEN price_value = '價跌' AND volume_value = '量增' AND trend_value = '盤整'
             AND position_value = '低檔' THEN '多'
        WHEN price_value = '價跌' AND volume_value = '量增' AND trend_value = '盤整'
             AND position_value IN ('中間', '高檔') THEN '空'
        WHEN price_value = '價跌' AND volume_value = '量增' AND trend_value = '空' THEN '空'
        WHEN price_value = '價跌' AND volume_value = '量縮' AND trend_value = '多' THEN '多'
        WHEN price_value = '價跌' AND volume_value = '量縮' AND trend_value = '盤整'
             AND position_value = '低檔' THEN '多'
        WHEN price_value = '價跌' AND volume_value = '量縮' AND trend_value = '盤整'
             AND position_value IN ('中間', '高檔') THEN '空'
        WHEN price_value = '價跌' AND volume_value = '量縮' AND trend_value = '空'
             AND position_value = '低檔' THEN '多'
        WHEN price_value = '價跌' AND volume_value = '量縮' AND trend_value = '空'
             AND position_value IN ('中間', '高檔') THEN '空'
        ELSE '資料不足'
    END
$$;


--
-- Name: fn_report_core_strategy(text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.fn_report_core_strategy(price_strategy text, trend_value text, position_value text, chip_strategy text, institutional_chip text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
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
        $$;


--
-- Name: FUNCTION fn_report_core_strategy(price_strategy text, trend_value text, position_value text, chip_strategy text, institutional_chip text); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.fn_report_core_strategy(price_strategy text, trend_value text, position_value text, chip_strategy text, institutional_chip text) IS '2026-09-15-v2 核心策略：盤整依位置判定（高空、中無、低多）；平手依籌碼、線型、價量決勝。';


--
-- Name: fn_report_line_chip_strategy(text, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.fn_report_line_chip_strategy(trend_value text, position_value text, chip_strategy text, institutional_chip text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT CASE
        WHEN trend_value NOT IN ('多', '空', '盤整')
          OR position_value NOT IN ('高檔', '中間', '低檔')
          OR chip_strategy NOT IN ('多', '空', '無')
        THEN '資料不足'
        WHEN chip_strategy = '無' AND institutional_chip NOT IN ('買超', '賣超')
        THEN '資料不足'
        WHEN trend_value = '多'
          AND (chip_strategy = '多' OR (chip_strategy = '無' AND institutional_chip = '買超'))
        THEN '多'
        WHEN trend_value = '多' THEN 'X'
        WHEN trend_value = '空'
          AND (chip_strategy = '空' OR (chip_strategy = '無' AND institutional_chip = '賣超'))
        THEN '空'
        WHEN trend_value = '空' THEN 'X'
        WHEN trend_value = '盤整' AND position_value = '高檔'
          AND (chip_strategy = '空' OR (chip_strategy = '無' AND institutional_chip = '賣超'))
        THEN '空'
        WHEN trend_value = '盤整' AND position_value = '高檔' THEN 'X'
        WHEN trend_value = '盤整' AND position_value = '中間' THEN 'X'
        WHEN trend_value = '盤整' AND position_value = '低檔'
          AND (chip_strategy = '多' OR (chip_strategy = '無' AND institutional_chip = '買超'))
        THEN '多'
        WHEN trend_value = '盤整' AND position_value = '低檔' THEN 'X'
        ELSE '資料不足'
    END
$$;


--
-- Name: fn_report_trend_position(text, numeric, numeric, numeric, numeric); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.fn_report_trend_position(trend_value text, close_value numeric, ma5_value numeric, bollinger_upper numeric, bollinger_lower numeric) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
    SELECT CASE
        -- 附圖「任一」資料不足規則：收盤、上下軌缺少或通道寬度無效。
        WHEN trend_value IS NULL OR trend_value NOT IN ('多', '空', '盤整')
          OR close_value IS NULL
          OR bollinger_upper IS NULL OR bollinger_lower IS NULL
          OR bollinger_upper <= bollinger_lower
        THEN '資料不足'

        -- 多／空必須能計算「收盤與 5MA 距離」。
        WHEN trend_value IN ('多', '空')
          AND (ma5_value IS NULL OR ma5_value = 0)
        THEN '資料不足'

        -- 多：> 2% 高檔；2% >= x > 1% 中間；<= 1% 低檔。
        WHEN trend_value = '多'
          AND (close_value - ma5_value) / ma5_value > 0.02
        THEN '高檔'
        WHEN trend_value = '多'
          AND (close_value - ma5_value) / ma5_value > 0.01
        THEN '中間'
        WHEN trend_value = '多' THEN '低檔'

        -- 空：>= -1% 高檔；-1% > x >= -2% 中間；< -2% 低檔。
        -- 附圖的 -1% 同時出現在兩列，依排列順序歸入高檔。
        WHEN trend_value = '空'
          AND (close_value - ma5_value) / ma5_value >= -0.01
        THEN '高檔'
        WHEN trend_value = '空'
          AND (close_value - ma5_value) / ma5_value >= -0.02
        THEN '中間'
        WHEN trend_value = '空' THEN '低檔'

        -- 盤整：通道上方 40%／中間 20%／下方 40%。
        WHEN trend_value = '盤整'
          AND close_value >= bollinger_lower
              + (bollinger_upper - bollinger_lower) * 0.60
        THEN '高檔'
        WHEN trend_value = '盤整'
          AND close_value <= bollinger_lower
              + (bollinger_upper - bollinger_lower) * 0.40
        THEN '低檔'
        WHEN trend_value = '盤整' THEN '中間'
        ELSE '資料不足'
    END
$$;


--
-- Name: FUNCTION fn_report_trend_position(trend_value text, close_value numeric, ma5_value numeric, bollinger_upper numeric, bollinger_lower numeric); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.fn_report_trend_position(trend_value text, close_value numeric, ma5_value numeric, bollinger_upper numeric, bollinger_lower numeric) IS '加權與期貨共用趨勢位置規則：多空採全日收與5MA乖離，盤整採布林通道40/20/40分區。';


--
-- Name: ml_delete_model(text, boolean); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.ml_delete_model(p_model_key text, p_confirm boolean DEFAULT false) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_model_id uuid;
    v_result jsonb;
BEGIN
    SELECT model_id INTO v_model_id FROM ml_model WHERE model_key = p_model_key;
    IF v_model_id IS NULL THEN
        RAISE EXCEPTION 'model_key % does not exist', p_model_key;
    END IF;

    SELECT jsonb_build_object(
        'model_key', p_model_key,
        'versions', (SELECT count(*) FROM ml_model_version WHERE model_id = v_model_id),
        'runs', (SELECT count(*) FROM ml_prediction_run r JOIN ml_model_version v USING (model_version_id) WHERE v.model_id = v_model_id),
        'predictions', (SELECT count(*) FROM ml_prediction p JOIN ml_model_version v USING (model_version_id) WHERE v.model_id = v_model_id),
        'deleted', p_confirm
    ) INTO v_result;

    IF p_confirm THEN
        DELETE FROM ml_model WHERE model_id = v_model_id;
    END IF;
    RETURN v_result;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: futures_core_next_day_details; Type: TABLE; Schema: backtest; Owner: -
--

CREATE TABLE backtest.futures_core_next_day_details (
    run_id uuid NOT NULL,
    signal_date date NOT NULL,
    evaluation_date date NOT NULL,
    signal_direction text NOT NULL,
    day_open numeric,
    day_high numeric,
    day_low numeric,
    day_close numeric,
    candle text NOT NULL,
    upper_shadow_points numeric NOT NULL,
    lower_shadow_points numeric NOT NULL,
    direction_hit boolean NOT NULL,
    upper_shadow_hit boolean NOT NULL,
    lower_shadow_hit boolean NOT NULL,
    any_shadow_hit boolean NOT NULL,
    overall_hit boolean NOT NULL,
    shadow_rescue boolean NOT NULL,
    open_to_close_points numeric NOT NULL,
    CONSTRAINT futures_core_next_day_details_signal_direction_check CHECK ((signal_direction = ANY (ARRAY['多'::text, '空'::text])))
);


--
-- Name: strategy_runs; Type: TABLE; Schema: backtest; Owner: -
--

CREATE TABLE backtest.strategy_runs (
    run_id uuid NOT NULL,
    strategy_code text NOT NULL,
    strategy_version text NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    settings jsonb DEFAULT '{}'::jsonb NOT NULL,
    valid_signal_count integer NOT NULL,
    direction_hit_rate numeric(7,2),
    overall_hit_rate numeric(7,2),
    total_points numeric(18,4),
    average_points numeric(18,4),
    summary jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT strategy_runs_check CHECK ((start_date <= end_date))
);


--
-- Name: trade_executions; Type: TABLE; Schema: backtest; Owner: -
--

CREATE TABLE backtest.trade_executions (
    plan_id bigint NOT NULL,
    entry_candidate_at timestamp without time zone,
    actual_entry_at timestamp without time zone,
    actual_entry_source_row_no bigint,
    theoretical_entry_price numeric,
    simulated_entry_price numeric,
    take_profit_first_at timestamp without time zone,
    take_profit_source_row_no bigint,
    stop_loss_first_at timestamp without time zone,
    stop_loss_source_row_no bigint,
    candidate_bar_at timestamp without time zone,
    candidate_bar_open numeric,
    candidate_bar_high numeric,
    candidate_bar_low numeric,
    candidate_bar_close numeric,
    same_bar_double_trigger boolean DEFAULT false NOT NULL,
    exit_reason text NOT NULL,
    resolution_level text NOT NULL,
    is_confirmed boolean NOT NULL,
    ambiguous_reason text,
    actual_exit_at timestamp without time zone,
    actual_exit_source_row_no bigint,
    theoretical_exit_price numeric,
    simulated_exit_price numeric,
    gross_points numeric,
    slippage_cost_points numeric DEFAULT 0 NOT NULL,
    fee_cost_amount numeric DEFAULT 0 NOT NULL,
    net_points numeric,
    worst_case_net_points numeric,
    fifty_fifty_net_points numeric,
    best_case_net_points numeric,
    data_quality_status text DEFAULT 'OK'::text NOT NULL,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    evaluated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_trade_executions_confirmation CHECK (((is_confirmed AND (exit_reason = ANY (ARRAY['TAKE_PROFIT'::text, 'STOP_LOSS'::text, 'CLOSE_EXIT'::text, 'NO_ENTRY'::text])) AND (ambiguous_reason IS NULL)) OR ((NOT is_confirmed) AND (exit_reason = ANY (ARRAY['AMBIGUOUS_SAME_BAR'::text, 'AMBIGUOUS_ENTRY_BAR'::text, 'MISSING_DATA'::text])) AND (ambiguous_reason IS NOT NULL)))),
    CONSTRAINT ck_trade_executions_data_quality CHECK ((data_quality_status = ANY (ARRAY['OK'::text, 'PARTIAL'::text, 'MISSING'::text]))),
    CONSTRAINT ck_trade_executions_exit_reason CHECK ((exit_reason = ANY (ARRAY['TAKE_PROFIT'::text, 'STOP_LOSS'::text, 'CLOSE_EXIT'::text, 'NO_ENTRY'::text, 'AMBIGUOUS_SAME_BAR'::text, 'AMBIGUOUS_ENTRY_BAR'::text, 'MISSING_DATA'::text]))),
    CONSTRAINT ck_trade_executions_resolution_level CHECK ((resolution_level = ANY (ARRAY['DAILY'::text, 'ONE_MINUTE'::text, 'TICK'::text, 'AMBIGUOUS'::text, 'NO_DATA'::text]))),
    CONSTRAINT ck_trade_executions_scenario_order CHECK (((worst_case_net_points IS NULL) OR (fifty_fifty_net_points IS NULL) OR (best_case_net_points IS NULL) OR ((worst_case_net_points <= fifty_fifty_net_points) AND (fifty_fifty_net_points <= best_case_net_points))))
);


--
-- Name: TABLE trade_executions; Type: COMMENT; Schema: backtest; Owner: -
--

COMMENT ON TABLE backtest.trade_executions IS '依日K、1分K、Tick三層判斷後的單次交易結果。';


--
-- Name: COLUMN trade_executions.resolution_level; Type: COMMENT; Schema: backtest; Owner: -
--

COMMENT ON COLUMN backtest.trade_executions.resolution_level IS '最終判定層級：DAILY、ONE_MINUTE、TICK、AMBIGUOUS或NO_DATA。';


--
-- Name: COLUMN trade_executions.is_confirmed; Type: COMMENT; Schema: backtest; Owner: -
--

COMMENT ON COLUMN backtest.trade_executions.is_confirmed IS 'true代表可確定成交先後；false代表仍需使用最好／五五波／最壞情境。';


--
-- Name: COLUMN trade_executions.fifty_fifty_net_points; Type: COMMENT; Schema: backtest; Owner: -
--

COMMENT ON COLUMN backtest.trade_executions.fifty_fifty_net_points IS '不確定單採50%停利、50%停損的期望淨點數；不是隨機分配結果。';


--
-- Name: trade_plans; Type: TABLE; Schema: backtest; Owner: -
--

CREATE TABLE backtest.trade_plans (
    plan_id bigint NOT NULL,
    run_id uuid NOT NULL,
    plan_no integer DEFAULT 1 NOT NULL,
    strategy_code text NOT NULL,
    strategy_version text NOT NULL,
    signal_date date NOT NULL,
    trade_date date NOT NULL,
    futures_id text DEFAULT 'TX'::text NOT NULL,
    contract_date text NOT NULL,
    trading_session text DEFAULT 'position'::text NOT NULL,
    direction text NOT NULL,
    entry_type text NOT NULL,
    planned_entry_at timestamp without time zone NOT NULL,
    planned_entry_price numeric,
    take_profit_price numeric NOT NULL,
    stop_loss_price numeric NOT NULL,
    forced_exit_at timestamp without time zone NOT NULL,
    quantity numeric DEFAULT 1 NOT NULL,
    parameters jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_trade_plans_dates CHECK ((signal_date <= trade_date)),
    CONSTRAINT ck_trade_plans_direction CHECK ((direction = ANY (ARRAY['LONG'::text, 'SHORT'::text]))),
    CONSTRAINT ck_trade_plans_entry_type CHECK ((entry_type = ANY (ARRAY['MARKET_AT_TIME'::text, 'LIMIT'::text, 'STOP'::text, 'STOP_LIMIT'::text]))),
    CONSTRAINT ck_trade_plans_plan_no CHECK ((plan_no > 0)),
    CONSTRAINT ck_trade_plans_price_direction CHECK (((planned_entry_price IS NULL) OR ((direction = 'LONG'::text) AND (take_profit_price > planned_entry_price) AND (stop_loss_price < planned_entry_price)) OR ((direction = 'SHORT'::text) AND (take_profit_price < planned_entry_price) AND (stop_loss_price > planned_entry_price)))),
    CONSTRAINT ck_trade_plans_quantity CHECK ((quantity > (0)::numeric)),
    CONSTRAINT ck_trade_plans_session CHECK ((trading_session = ANY (ARRAY['position'::text, 'after_market'::text]))),
    CONSTRAINT ck_trade_plans_time_range CHECK ((planned_entry_at <= forced_exit_at))
);


--
-- Name: TABLE trade_plans; Type: COMMENT; Schema: backtest; Owner: -
--

COMMENT ON TABLE backtest.trade_plans IS '策略在交易日前產生的每日交易計畫；一個 run 可保存多日、多筆計畫。';


--
-- Name: COLUMN trade_plans.contract_date; Type: COMMENT; Schema: backtest; Owner: -
--

COMMENT ON COLUMN backtest.trade_plans.contract_date IS '實際交易契約月份；回測不可將不同月份契約混在一起。';


--
-- Name: COLUMN trade_plans.planned_entry_at; Type: COMMENT; Schema: backtest; Owner: -
--

COMMENT ON COLUMN backtest.trade_plans.planned_entry_at IS '開始尋找進場的時間；停利停損只能從實際進場所在的分K／Tick以後判斷。';


--
-- Name: COLUMN trade_plans.parameters; Type: COMMENT; Schema: backtest; Owner: -
--

COMMENT ON COLUMN backtest.trade_plans.parameters IS '策略特有設定，例如訊號欄位、停利停損算法、成本及滑價模型。';


--
-- Name: trade_plans_plan_id_seq; Type: SEQUENCE; Schema: backtest; Owner: -
--

ALTER TABLE backtest.trade_plans ALTER COLUMN plan_id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME backtest.trade_plans_plan_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: v_trade_backtest_details; Type: VIEW; Schema: backtest; Owner: -
--

CREATE VIEW backtest.v_trade_backtest_details AS
 SELECT p.run_id,
    p.plan_id,
    p.plan_no,
    p.strategy_code,
    p.strategy_version,
    p.signal_date,
    p.trade_date,
    p.futures_id,
    p.contract_date,
    p.trading_session,
    p.direction,
    p.entry_type,
    p.planned_entry_at,
    p.planned_entry_price,
    p.take_profit_price,
    p.stop_loss_price,
    p.forced_exit_at,
    p.quantity,
    e.actual_entry_at,
    e.actual_entry_source_row_no,
    e.theoretical_entry_price,
    e.simulated_entry_price,
    e.take_profit_first_at,
    e.take_profit_source_row_no,
    e.stop_loss_first_at,
    e.stop_loss_source_row_no,
    e.same_bar_double_trigger,
    e.exit_reason,
    e.resolution_level,
    e.is_confirmed,
    e.ambiguous_reason,
    e.actual_exit_at,
    e.actual_exit_source_row_no,
    e.theoretical_exit_price,
    e.simulated_exit_price,
    e.gross_points,
    e.slippage_cost_points,
    e.fee_cost_amount,
    e.net_points,
    e.worst_case_net_points,
    e.fifty_fifty_net_points,
    e.best_case_net_points,
    e.data_quality_status,
    e.evaluated_at
   FROM (backtest.trade_plans p
     LEFT JOIN backtest.trade_executions e ON ((e.plan_id = p.plan_id)));


--
-- Name: VIEW v_trade_backtest_details; Type: COMMENT; Schema: backtest; Owner: -
--

COMMENT ON VIEW backtest.v_trade_backtest_details IS '交易計畫與三層成交模擬結果整合明細。';


--
-- Name: data_source; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE core.data_source (
    id bigint NOT NULL,
    name text NOT NULL,
    type text,
    storage_table text,
    src_link text,
    web_path text,
    api_path text,
    api_method text,
    api_params text,
    api_res_sample text,
    memo text,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    time_col text
);


--
-- Name: data_source_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

ALTER TABLE core.data_source ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME core.data_source_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: date_span; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE core.date_span (
    id bigint NOT NULL,
    target_table text NOT NULL,
    idx_key text,
    start_date date NOT NULL,
    end_date date NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: date_span_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

ALTER TABLE core.date_span ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME core.date_span_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: futures_report_daily_raw; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE core.futures_report_daily_raw (
    id bigint NOT NULL,
    futures_id text NOT NULL,
    "日期" date NOT NULL,
    "年" bigint,
    "月" bigint,
    "星期" text,
    "週次" bigint,
    "天別" text,
    "日盤開" double precision,
    "日盤收" double precision,
    "日盤高" double precision,
    "日盤低" double precision,
    "日漲跌" double precision,
    "日振幅" double precision,
    "日振幅開盤比" double precision,
    "日振幅昨收比" double precision,
    "日收減日開" double precision,
    "日收開昨收比" double precision,
    "夜盤開" double precision,
    "夜盤收" double precision,
    "夜盤高" double precision,
    "夜盤低" double precision,
    "日法人多倉" double precision,
    "日法人空倉" double precision,
    "夜法人多單量" double precision,
    "夜法人空單量" double precision,
    "日5MA" double precision,
    "日10MA" double precision,
    "日20MA" double precision,
    "日60MA" double precision,
    "日120MA" double precision,
    "夜5MA" double precision,
    "夜10MA" double precision,
    "夜20MA" double precision,
    "夜60MA" double precision,
    "夜120MA" double precision,
    created_at bigint DEFAULT (EXTRACT(epoch FROM CURRENT_TIMESTAMP))::bigint,
    "日成交量" double precision,
    "夜成交量" double precision,
    "日布林標準差" double precision,
    "日布林上軌" double precision,
    "日布林下軌" double precision,
    "日當日價量" text,
    "日5日價量" text,
    "日10日價量" text,
    "日20日價量" text,
    "日60日價量" text,
    "call_月選" double precision,
    "put_月選" double precision,
    "call_週三" double precision,
    "put_週三" double precision,
    "call_週五" double precision,
    "put_週五" double precision,
    "加權收盤價" double precision,
    "大台收盤價" double precision,
    "大台結算價" double precision,
    "日240MA" double precision,
    "K線型態" text,
    "夜漲跌" numeric,
    "夜價漲跌_pct" numeric,
    "夜價量結構" text,
    "夜K線型態" text,
    "夜5MA乖離率" numeric,
    "夜10MA乖離率" numeric,
    "夜20MA乖離率" numeric,
    "夜60MA乖離率" numeric,
    "夜布林標準差" numeric,
    "夜布林上軌" numeric,
    "夜布林下軌" numeric,
    "夜指標已計算" boolean DEFAULT false NOT NULL
);


--
-- Name: futures_report_daily_raw_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

ALTER TABLE core.futures_report_daily_raw ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME core.futures_report_daily_raw_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: relation_punish_notice; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE core.relation_punish_notice (
    id bigint NOT NULL,
    type text NOT NULL,
    punish_id bigint NOT NULL,
    notice_id bigint NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: relation_punish_notice_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

ALTER TABLE core.relation_punish_notice ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME core.relation_punish_notice_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: report_cache_status; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE core.report_cache_status (
    cache_name text NOT NULL,
    refreshed_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    row_count bigint DEFAULT 0 NOT NULL,
    elapsed_seconds double precision
);


--
-- Name: stock_price_involve_report; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE core.stock_price_involve_report (
    id bigint NOT NULL,
    report text NOT NULL,
    stock_id text NOT NULL,
    price double precision,
    "lastDt_close_distance" double precision,
    "lcd%" double precision,
    involve_days bigint,
    involve_date text,
    date date,
    date_distance bigint,
    price_type text,
    volume bigint,
    vol_weight double precision,
    vol_wei_pr double precision,
    vol_adj double precision,
    vol_wei_adj double precision,
    vol_wei_pr_adj double precision,
    remark text,
    created_at bigint DEFAULT (EXTRACT(epoch FROM CURRENT_TIMESTAMP))::bigint
);


--
-- Name: stock_price_involve_report_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

ALTER TABLE core.stock_price_involve_report ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME core.stock_price_involve_report_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: stock_report_daily; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE core.stock_report_daily (
    id bigint NOT NULL,
    "日期" date NOT NULL,
    "股票代號" text NOT NULL,
    "年" bigint,
    "月" bigint,
    "星期" text,
    "週次" bigint,
    "天別" text,
    "開盤價" double precision,
    "收盤價" double precision,
    "收盤_開盤" double precision,
    "最高價" double precision,
    "最低價" double precision,
    "漲跌" double precision,
    "日振幅" double precision,
    "漲跌幅_pct" double precision,
    "日振幅_昨收_pct" double precision,
    "日振福_開盤_pct" double precision,
    "成交量" double precision,
    "實體_pct" double precision,
    "上影_pct" double precision,
    "下影_pct" double precision,
    "K線型態" text,
    "跳空缺口" double precision,
    "跳空缺口_昨收_pct" double precision,
    "當日價量" text,
    "5日價量" text,
    "10日價量" text,
    "20日價量" text,
    "60日價量" text,
    "總成交金額_億" double precision,
    "法人總買超_億" double precision,
    "買超_外資_億" double precision,
    "買超_投信_億" double precision,
    "買超_自營商_億" double precision,
    "買超_融資_億" double precision,
    "法人淨買賣超_扣融資_億" double precision,
    "法人買賣超_總成交_pct" double precision,
    "漲跌幅百分位數" double precision,
    "漲跌幅級距" bigint,
    "三大法人買賣超百分位數" double precision,
    "三大法人買賣超級距" bigint,
    "資金走向判定" text,
    is_complete bigint DEFAULT 0,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "120MA" double precision,
    "240MA" double precision
);


--
-- Name: stock_report_daily_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

ALTER TABLE core.stock_report_daily ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME core.stock_report_daily_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: ml_model; Type: TABLE; Schema: ml; Owner: -
--

CREATE TABLE ml.ml_model (
    model_id uuid DEFAULT gen_random_uuid() NOT NULL,
    model_key text NOT NULL,
    model_name text NOT NULL,
    description text,
    target_market text NOT NULL,
    target_name text NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: ml_model_version; Type: TABLE; Schema: ml; Owner: -
--

CREATE TABLE ml.ml_model_version (
    model_version_id uuid DEFAULT gen_random_uuid() NOT NULL,
    model_id uuid NOT NULL,
    version_no integer NOT NULL,
    algorithm text NOT NULL,
    source_relation text NOT NULL,
    entity_filter jsonb DEFAULT '{}'::jsonb NOT NULL,
    feature_columns jsonb NOT NULL,
    target_config jsonb NOT NULL,
    model_params jsonb DEFAULT '{}'::jsonb NOT NULL,
    training_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    config_snapshot jsonb NOT NULL,
    config_hash text NOT NULL,
    code_version text,
    status text DEFAULT 'active'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ml_model_version_status_check CHECK ((status = ANY (ARRAY['draft'::text, 'active'::text, 'retired'::text, 'failed'::text]))),
    CONSTRAINT ml_model_version_version_no_check CHECK ((version_no > 0))
);


--
-- Name: ml_prediction; Type: TABLE; Schema: ml; Owner: -
--

CREATE TABLE ml.ml_prediction (
    prediction_id bigint NOT NULL,
    run_id uuid NOT NULL,
    model_version_id uuid NOT NULL,
    target_date date NOT NULL,
    feature_date date NOT NULL,
    trained_through_date date NOT NULL,
    predicted_class smallint NOT NULL,
    predicted_label text NOT NULL,
    probability_down double precision,
    probability_flat double precision,
    probability_up double precision,
    confidence double precision,
    feature_values jsonb NOT NULL,
    actual_class smallint,
    actual_label text,
    actual_return_pct double precision,
    is_correct boolean GENERATED ALWAYS AS (
CASE
    WHEN (actual_class IS NULL) THEN NULL::boolean
    ELSE (predicted_class = actual_class)
END) STORED,
    predicted_at timestamp with time zone DEFAULT now() NOT NULL,
    actual_updated_at timestamp with time zone,
    CONSTRAINT ml_prediction_actual_class_check CHECK (((actual_class IS NULL) OR (actual_class = ANY (ARRAY['-1'::integer, 0, 1])))),
    CONSTRAINT ml_prediction_confidence_check CHECK (((confidence IS NULL) OR ((confidence >= (0)::double precision) AND (confidence <= (1)::double precision)))),
    CONSTRAINT ml_prediction_predicted_class_check CHECK ((predicted_class = ANY (ARRAY['-1'::integer, 0, 1])))
);


--
-- Name: ml_prediction_prediction_id_seq; Type: SEQUENCE; Schema: ml; Owner: -
--

ALTER TABLE ml.ml_prediction ALTER COLUMN prediction_id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME ml.ml_prediction_prediction_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: ml_prediction_run; Type: TABLE; Schema: ml; Owner: -
--

CREATE TABLE ml.ml_prediction_run (
    run_id uuid DEFAULT gen_random_uuid() NOT NULL,
    model_version_id uuid NOT NULL,
    run_type text NOT NULL,
    requested_start_date date NOT NULL,
    requested_end_date date NOT NULL,
    train_start_date date,
    train_end_date date,
    row_count integer,
    random_seed integer,
    runtime_params jsonb DEFAULT '{}'::jsonb NOT NULL,
    metrics jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'running'::text NOT NULL,
    error_message text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT ml_prediction_run_run_type_check CHECK ((run_type = ANY (ARRAY['single'::text, 'backtest'::text, 'live'::text]))),
    CONSTRAINT ml_prediction_run_status_check CHECK ((status = ANY (ARRAY['running'::text, 'completed'::text, 'partial'::text, 'failed'::text])))
);


--
-- Name: taifex_futures_daily; Type: TABLE; Schema: raw_exchange; Owner: -
--

CREATE TABLE raw_exchange.taifex_futures_daily (
    id bigint NOT NULL,
    trade_date date NOT NULL,
    futures_id text NOT NULL,
    contract_month text NOT NULL,
    trading_session text NOT NULL,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume bigint,
    settlement_price double precision,
    open_interest bigint,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_taifex_futures_daily_ohlc CHECK (((high IS NULL) OR (low IS NULL) OR (open IS NULL) OR (close IS NULL) OR ((high >= GREATEST(open, close, low)) AND (low <= LEAST(open, close, high))))),
    CONSTRAINT ck_taifex_futures_daily_session CHECK ((trading_session = ANY (ARRAY['position'::text, 'after_market'::text]))),
    CONSTRAINT ck_taifex_futures_daily_volume CHECK (((volume IS NULL) OR (volume >= 0)))
);


--
-- Name: TABLE taifex_futures_daily; Type: COMMENT; Schema: raw_exchange; Owner: -
--

COMMENT ON TABLE raw_exchange.taifex_futures_daily IS '臺灣期貨交易所官方期貨每日交易行情；與 FinMind 分開保存，供缺檔備援與來源稽核。';


--
-- Name: taifex_futures_daily_id_seq; Type: SEQUENCE; Schema: raw_exchange; Owner: -
--

ALTER TABLE raw_exchange.taifex_futures_daily ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_exchange.taifex_futures_daily_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: tpex_bulletin_attention; Type: TABLE; Schema: raw_exchange; Owner: -
--

CREATE TABLE raw_exchange.tpex_bulletin_attention (
    id bigint NOT NULL,
    "證券代號" text,
    "證券名稱" text,
    "累計" bigint,
    "注意交易資訊" text,
    "公告日期" text,
    "收盤價" double precision,
    "本益比" double precision,
    link text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "公告日期_ts" bigint,
    law_src text
);


--
-- Name: tpex_bulletin_attention_id_seq; Type: SEQUENCE; Schema: raw_exchange; Owner: -
--

ALTER TABLE raw_exchange.tpex_bulletin_attention ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_exchange.tpex_bulletin_attention_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: tpex_bulletin_disposal; Type: TABLE; Schema: raw_exchange; Owner: -
--

CREATE TABLE raw_exchange.tpex_bulletin_disposal (
    id bigint NOT NULL,
    "公布日期" text,
    "證券代號" text,
    "證券名稱" text,
    "累計" bigint,
    "處置起訖時間" text,
    "處置原因" text,
    "處置內容" text,
    "收盤價" double precision,
    "本益比" double precision,
    memo text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "公布日期_ts" bigint,
    law_src text,
    last_notice bigint,
    notice_cnt bigint
);


--
-- Name: tpex_bulletin_disposal_id_seq; Type: SEQUENCE; Schema: raw_exchange; Owner: -
--

ALTER TABLE raw_exchange.tpex_bulletin_disposal ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_exchange.tpex_bulletin_disposal_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: twse_announcement_notice; Type: TABLE; Schema: raw_exchange; Owner: -
--

CREATE TABLE raw_exchange.twse_announcement_notice (
    id bigint NOT NULL,
    "證券代號" text,
    "證券名稱" text,
    "累計次數" bigint,
    "注意交易資訊" text,
    "日期" text,
    "收盤價" double precision,
    "本益比" double precision,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "日期_ts" bigint,
    law_src text
);


--
-- Name: twse_announcement_notice_id_seq; Type: SEQUENCE; Schema: raw_exchange; Owner: -
--

ALTER TABLE raw_exchange.twse_announcement_notice ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_exchange.twse_announcement_notice_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: twse_announcement_punish; Type: TABLE; Schema: raw_exchange; Owner: -
--

CREATE TABLE raw_exchange.twse_announcement_punish (
    id bigint NOT NULL,
    "公布日期" text,
    "證券代號" text,
    "證券名稱" text,
    "累計" bigint,
    "處置條件" text,
    "處置起迄時間" text,
    "處置措施" text,
    "處置內容" text,
    "備註" text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "公布日期_ts" bigint,
    law_src text,
    last_notice bigint,
    notice_cnt bigint
);


--
-- Name: twse_announcement_punish_id_seq; Type: SEQUENCE; Schema: raw_exchange; Owner: -
--

ALTER TABLE raw_exchange.twse_announcement_punish ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_exchange.twse_announcement_punish_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: twse_exchangeReport_fmtqik; Type: TABLE; Schema: raw_exchange; Owner: -
--

CREATE TABLE raw_exchange."twse_exchangeReport_fmtqik" (
    id bigint NOT NULL,
    date text NOT NULL,
    date_ad date NOT NULL,
    date_ts bigint NOT NULL,
    market_volume bigint NOT NULL,
    market_money bigint NOT NULL,
    trade_count bigint NOT NULL,
    taiex_close double precision NOT NULL,
    taiex_spread double precision NOT NULL,
    created_at bigint DEFAULT (EXTRACT(epoch FROM CURRENT_TIMESTAMP))::bigint NOT NULL
);


--
-- Name: twse_exchangeReport_fmtqik_id_seq; Type: SEQUENCE; Schema: raw_exchange; Owner: -
--

ALTER TABLE raw_exchange."twse_exchangeReport_fmtqik" ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_exchange."twse_exchangeReport_fmtqik_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: twse_indicesReport_mi_5mins_hist; Type: TABLE; Schema: raw_exchange; Owner: -
--

CREATE TABLE raw_exchange."twse_indicesReport_mi_5mins_hist" (
    id bigint NOT NULL,
    date text NOT NULL,
    date_ad date NOT NULL,
    date_ts bigint NOT NULL,
    open_index double precision NOT NULL,
    high_index double precision NOT NULL,
    low_index double precision NOT NULL,
    close_index double precision NOT NULL,
    created_at bigint DEFAULT (EXTRACT(epoch FROM CURRENT_TIMESTAMP))::bigint NOT NULL
);


--
-- Name: twse_indicesReport_mi_5mins_hist_id_seq; Type: SEQUENCE; Schema: raw_exchange; Owner: -
--

ALTER TABLE raw_exchange."twse_indicesReport_mi_5mins_hist" ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_exchange."twse_indicesReport_mi_5mins_hist_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: twse_marginTrading_miMargn; Type: TABLE; Schema: raw_exchange; Owner: -
--

CREATE TABLE raw_exchange."twse_marginTrading_miMargn" (
    id bigint NOT NULL,
    "日期" text,
    "項目" text,
    "買進" bigint,
    "賣出" bigint,
    "現金_券_償還" bigint,
    "前日餘額" bigint,
    "今日餘額" bigint,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: twse_marginTrading_miMargn_id_seq; Type: SEQUENCE; Schema: raw_exchange; Owner: -
--

ALTER TABLE raw_exchange."twse_marginTrading_miMargn" ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_exchange."twse_marginTrading_miMargn_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_futures_daily; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_futures_daily (
    id bigint NOT NULL,
    date date NOT NULL,
    futures_id text NOT NULL,
    contract_date text NOT NULL,
    open double precision,
    max double precision,
    min double precision,
    close double precision,
    spread double precision,
    spread_per double precision,
    volume bigint,
    settlement_price double precision,
    open_interest bigint,
    trading_session text NOT NULL,
    created_at bigint DEFAULT (EXTRACT(epoch FROM CURRENT_TIMESTAMP))::bigint NOT NULL
);


--
-- Name: fm_taiwan_futures_daily_id_seq; Type: SEQUENCE; Schema: raw_finmind; Owner: -
--

ALTER TABLE raw_finmind.fm_taiwan_futures_daily ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_finmind.fm_taiwan_futures_daily_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_futures_final_settlement_price; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_futures_final_settlement_price (
    id bigint NOT NULL,
    date date NOT NULL,
    contract_month text CONSTRAINT fm_taiwan_futures_final_settlement_pric_contract_month_not_null NOT NULL,
    futures_type text,
    futures_id text NOT NULL,
    futures_name text,
    settlement_price double precision,
    underlying_code text,
    notional_value double precision,
    created_at bigint DEFAULT (EXTRACT(epoch FROM CURRENT_TIMESTAMP))::bigint NOT NULL
);


--
-- Name: fm_taiwan_futures_final_settlement_price_id_seq; Type: SEQUENCE; Schema: raw_finmind; Owner: -
--

ALTER TABLE raw_finmind.fm_taiwan_futures_final_settlement_price ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_finmind.fm_taiwan_futures_final_settlement_price_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_futures_institutional_investors; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_futures_institutional_investors (
    id bigint NOT NULL,
    date date NOT NULL,
    data_id text NOT NULL,
    institutional_investors text CONSTRAINT fm_taiwan_futures_institutiona_institutional_investors_not_null NOT NULL,
    long_deal_volume bigint,
    long_deal_amount double precision,
    short_deal_volume bigint,
    short_deal_amount double precision,
    long_open_interest_balance_volume bigint,
    long_open_interest_balance_amount double precision,
    short_open_interest_balance_volume bigint,
    short_open_interest_balance_amount double precision,
    created_at bigint DEFAULT (EXTRACT(epoch FROM CURRENT_TIMESTAMP))::bigint NOT NULL
);


--
-- Name: fm_taiwan_futures_institutional_investors_after_hours; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_futures_institutional_investors_after_hours (
    id bigint CONSTRAINT fm_taiwan_futures_institutional_investors_after_hou_id_not_null NOT NULL,
    date date CONSTRAINT fm_taiwan_futures_institutional_investors_after_h_date_not_null NOT NULL,
    futures_id text CONSTRAINT fm_taiwan_futures_institutional_investors_a_futures_id_not_null NOT NULL,
    institutional_investors text CONSTRAINT fm_taiwan_futures_institution_institutional_investors_not_null1 NOT NULL,
    long_deal_volume bigint,
    long_deal_amount double precision,
    short_deal_volume bigint,
    short_deal_amount double precision,
    created_at bigint DEFAULT (EXTRACT(epoch FROM CURRENT_TIMESTAMP))::bigint CONSTRAINT fm_taiwan_futures_institutional_investors_a_created_at_not_null NOT NULL
);


--
-- Name: fm_taiwan_futures_institutional_investors_after_hours_id_seq; Type: SEQUENCE; Schema: raw_finmind; Owner: -
--

ALTER TABLE raw_finmind.fm_taiwan_futures_institutional_investors_after_hours ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_finmind.fm_taiwan_futures_institutional_investors_after_hours_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_futures_institutional_investors_id_seq; Type: SEQUENCE; Schema: raw_finmind; Owner: -
--

ALTER TABLE raw_finmind.fm_taiwan_futures_institutional_investors ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_finmind.fm_taiwan_futures_institutional_investors_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_futures_kbar_1m; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_futures_kbar_1m (
    id bigint NOT NULL,
    source_date date NOT NULL,
    trading_date date NOT NULL,
    bar_datetime timestamp without time zone NOT NULL,
    futures_id text NOT NULL,
    contract_date text NOT NULL,
    trading_session text NOT NULL,
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    volume bigint NOT NULL,
    raw_volume bigint NOT NULL,
    tick_count bigint NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_fm_taiwan_futures_kbar_1m_ohlc CHECK (((high >= low) AND (high >= open) AND (high >= close) AND (low <= open) AND (low <= close))),
    CONSTRAINT ck_fm_taiwan_futures_kbar_1m_session CHECK ((trading_session = ANY (ARRAY['position'::text, 'after_market'::text]))),
    CONSTRAINT ck_fm_taiwan_futures_kbar_1m_volume CHECK (((volume >= 0) AND (raw_volume >= 0) AND (tick_count > 0)))
);


--
-- Name: TABLE fm_taiwan_futures_kbar_1m; Type: COMMENT; Schema: raw_finmind; Owner: -
--

COMMENT ON TABLE raw_finmind.fm_taiwan_futures_kbar_1m IS '由 FinMind TaiwanFuturesTick 汇总的各实际契约一分钟 K';


--
-- Name: COLUMN fm_taiwan_futures_kbar_1m.source_date; Type: COMMENT; Schema: raw_finmind; Owner: -
--

COMMENT ON COLUMN raw_finmind.fm_taiwan_futures_kbar_1m.source_date IS '原始 Tick 的 API 请求日期／日历日期';


--
-- Name: COLUMN fm_taiwan_futures_kbar_1m.trading_date; Type: COMMENT; Schema: raw_finmind; Owner: -
--

COMMENT ON COLUMN raw_finmind.fm_taiwan_futures_kbar_1m.trading_date IS '依期交所夜盘归属规则换算后的交易日';


--
-- Name: COLUMN fm_taiwan_futures_kbar_1m.volume; Type: COMMENT; Schema: raw_finmind; Owner: -
--

COMMENT ON COLUMN raw_finmind.fm_taiwan_futures_kbar_1m.volume IS '由 FinMind 双边逐笔量除以 2 后的单边口数';


--
-- Name: COLUMN fm_taiwan_futures_kbar_1m.raw_volume; Type: COMMENT; Schema: raw_finmind; Owner: -
--

COMMENT ON COLUMN raw_finmind.fm_taiwan_futures_kbar_1m.raw_volume IS 'FinMind 原始双边逐笔量加总';


--
-- Name: fm_taiwan_futures_kbar_1m_id_seq; Type: SEQUENCE; Schema: raw_finmind; Owner: -
--

ALTER TABLE raw_finmind.fm_taiwan_futures_kbar_1m ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_finmind.fm_taiwan_futures_kbar_1m_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_futures_tick; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_futures_tick (
    id bigint NOT NULL,
    source_date date NOT NULL,
    trade_datetime timestamp without time zone NOT NULL,
    futures_id text NOT NULL,
    contract_date text NOT NULL,
    price double precision NOT NULL,
    volume bigint NOT NULL,
    source_row_no bigint NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_fm_taiwan_futures_tick_volume CHECK ((volume >= 0))
);


--
-- Name: COLUMN fm_taiwan_futures_tick.source_date; Type: COMMENT; Schema: raw_finmind; Owner: -
--

COMMENT ON COLUMN raw_finmind.fm_taiwan_futures_tick.source_date IS '呼叫 TaiwanFuturesTick API 时传入的 date，用于完整替换单日资料';


--
-- Name: COLUMN fm_taiwan_futures_tick.price; Type: COMMENT; Schema: raw_finmind; Owner: -
--

COMMENT ON COLUMN raw_finmind.fm_taiwan_futures_tick.price IS 'FinMind 原始成交价；跨月价差契约可能为 0 或负数';


--
-- Name: COLUMN fm_taiwan_futures_tick.volume; Type: COMMENT; Schema: raw_finmind; Owner: -
--

COMMENT ON COLUMN raw_finmind.fm_taiwan_futures_tick.volume IS 'FinMind 原始双边成交量；此原始层不除以 2';


--
-- Name: COLUMN fm_taiwan_futures_tick.source_row_no; Type: COMMENT; Schema: raw_finmind; Owner: -
--

COMMENT ON COLUMN raw_finmind.fm_taiwan_futures_tick.source_row_no IS 'API 单日回传顺序；用于保留内容完全相同的重复成交，同时避免重复汇入';


--
-- Name: fm_taiwan_futures_tick_id_seq; Type: SEQUENCE; Schema: raw_finmind; Owner: -
--

ALTER TABLE raw_finmind.fm_taiwan_futures_tick ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_finmind.fm_taiwan_futures_tick_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_option_daily; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_option_daily (
    id bigint NOT NULL,
    date date NOT NULL,
    option_id text NOT NULL,
    contract_date text NOT NULL,
    strike_price double precision NOT NULL,
    call_put text NOT NULL,
    open double precision,
    max double precision,
    min double precision,
    close double precision,
    volume bigint,
    settlement_price double precision,
    open_interest bigint,
    trading_session text NOT NULL,
    created_at bigint DEFAULT (EXTRACT(epoch FROM CURRENT_TIMESTAMP))::bigint NOT NULL
);


--
-- Name: fm_taiwan_option_daily_id_seq; Type: SEQUENCE; Schema: raw_finmind; Owner: -
--

ALTER TABLE raw_finmind.fm_taiwan_option_daily ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_finmind.fm_taiwan_option_daily_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_option_final_settlement_price; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_option_final_settlement_price (
    id bigint NOT NULL,
    date date NOT NULL,
    contract_month text NOT NULL,
    option_type text,
    option_id text NOT NULL,
    option_name text,
    settlement_price double precision,
    underlying_code text,
    notional_value double precision,
    created_at bigint DEFAULT (EXTRACT(epoch FROM CURRENT_TIMESTAMP))::bigint NOT NULL
);


--
-- Name: fm_taiwan_option_final_settlement_price_id_seq; Type: SEQUENCE; Schema: raw_finmind; Owner: -
--

ALTER TABLE raw_finmind.fm_taiwan_option_final_settlement_price ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_finmind.fm_taiwan_option_final_settlement_price_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_stock_daily; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_stock_daily (
    id bigint NOT NULL,
    date date NOT NULL,
    stock_id text NOT NULL,
    "Trading_Volume" bigint NOT NULL,
    "Trading_money" bigint NOT NULL,
    open double precision NOT NULL,
    max double precision NOT NULL,
    min double precision NOT NULL,
    close double precision NOT NULL,
    spread double precision NOT NULL,
    "Trading_turnover" bigint NOT NULL,
    created_at bigint DEFAULT (EXTRACT(epoch FROM CURRENT_TIMESTAMP))::bigint NOT NULL,
    updated_at bigint DEFAULT (EXTRACT(epoch FROM CURRENT_TIMESTAMP))::bigint NOT NULL
);


--
-- Name: fm_taiwan_stock_daily_id_seq; Type: SEQUENCE; Schema: raw_finmind; Owner: -
--

ALTER TABLE raw_finmind.fm_taiwan_stock_daily ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_finmind.fm_taiwan_stock_daily_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_stock_institutional_total; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_stock_institutional_total (
    id bigint NOT NULL,
    date date NOT NULL,
    name text NOT NULL,
    buy double precision,
    sell double precision,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: fm_taiwan_stock_institutional_total_id_seq; Type: SEQUENCE; Schema: raw_finmind; Owner: -
--

ALTER TABLE raw_finmind.fm_taiwan_stock_institutional_total ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_finmind.fm_taiwan_stock_institutional_total_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_stock_kbar_1m; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_stock_kbar_1m (
    id bigint NOT NULL,
    date date NOT NULL,
    minute time without time zone NOT NULL,
    stock_id text NOT NULL,
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    volume bigint DEFAULT 0 NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_fm_taiwan_stock_kbar_1m_ohlc CHECK (((high >= low) AND (high >= open) AND (high >= close) AND (low <= open) AND (low <= close))),
    CONSTRAINT ck_fm_taiwan_stock_kbar_1m_volume CHECK ((volume >= 0))
);


--
-- Name: TABLE fm_taiwan_stock_kbar_1m; Type: COMMENT; Schema: raw_finmind; Owner: -
--

COMMENT ON TABLE raw_finmind.fm_taiwan_stock_kbar_1m IS 'FinMind TaiwanStockKBar 一分钟 K；TAIEX 的 volume 固定为 0';


--
-- Name: fm_taiwan_stock_kbar_1m_id_seq; Type: SEQUENCE; Schema: raw_finmind; Owner: -
--

ALTER TABLE raw_finmind.fm_taiwan_stock_kbar_1m ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_finmind.fm_taiwan_stock_kbar_1m_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_stock_margin_total; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_stock_margin_total (
    id bigint NOT NULL,
    date date NOT NULL,
    name text NOT NULL,
    buy double precision,
    sell double precision,
    "TodayBalance" double precision,
    "YesBalance" double precision,
    "Return" double precision,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: fm_taiwan_stock_margin_total_id_seq; Type: SEQUENCE; Schema: raw_finmind; Owner: -
--

ALTER TABLE raw_finmind.fm_taiwan_stock_margin_total ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME raw_finmind.fm_taiwan_stock_margin_total_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: fm_taiwan_stock_trading_date; Type: TABLE; Schema: raw_finmind; Owner: -
--

CREATE TABLE raw_finmind.fm_taiwan_stock_trading_date (
    date date NOT NULL
);


--
-- Name: futures_report_daily_dashboard_cache; Type: TABLE; Schema: report; Owner: -
--

CREATE TABLE report.futures_report_daily_dashboard_cache (
    id bigint,
    futures_id text,
    "日期" date,
    "年" bigint,
    "月" bigint,
    "星期" text,
    "週次" bigint,
    "天別" text,
    "全日開" numeric,
    "全日收" numeric,
    "全日高" numeric,
    "全日低" numeric,
    "全漲跌" numeric,
    "全價漲跌_pct" numeric,
    "全收減全開" numeric,
    "全收開_昨收_pct" numeric,
    "全收開_開盤_pct" numeric,
    "全振幅" numeric,
    "全振幅_昨收_pct" numeric,
    "全振幅_開盤_pct" numeric,
    "全成交量" numeric,
    "全5最大量_日期" date,
    "全5最大量" numeric,
    "全10最大量_日期" date,
    "全10最大量" numeric,
    "全20最大量_日期" date,
    "全20最大量" numeric,
    "全60最大量_日期" date,
    "全60最大量" numeric,
    "全5均量" numeric,
    "全10均量" numeric,
    "全20均量" numeric,
    "全60均量" numeric,
    "全量增減_昨日_pct" numeric,
    "全量增減_五日_pct" numeric,
    "全量增減_十日_pct" numeric,
    "全量增減_二十日_pct" numeric,
    "全量增減_六十日_pct" numeric,
    "全當日價量" text,
    "全5日價量" text,
    "全10日價量" text,
    "全20日價量" text,
    "全60日價量" text,
    "全K線型態" text,
    "全跳空缺口" numeric,
    "全跳空缺口_昨收_pct" numeric,
    "全實體_pct" numeric,
    "全上影_pct" numeric,
    "全下影_pct" numeric,
    "全5MA" numeric,
    "全10MA" numeric,
    "全20MA" numeric,
    "全60MA" numeric,
    "全120MA" numeric,
    "全240MA" numeric,
    "全5均上升幅度_點" numeric,
    "全10均上升幅度_點" numeric,
    "全20均上升幅度_點" numeric,
    "全60均上升幅度_點" numeric,
    "全5均上升幅度_pct" numeric,
    "全10均上升幅度_pct" numeric,
    "全20均上升幅度_pct" numeric,
    "全60均上升幅度_pct" numeric,
    "全5_明日扣抵值" numeric,
    "全10_明日扣抵值" numeric,
    "全20_明日扣抵值" numeric,
    "全60_明日扣抵值" numeric,
    "全5_明日扣抵值影響_點" numeric,
    "全10_明日扣抵值影響_點" numeric,
    "全20_明日扣抵值影響_點" numeric,
    "全60_明日扣抵值影響_點" numeric,
    "全5_明日扣抵值影響_pct" numeric,
    "全10_明日扣抵值影響_pct" numeric,
    "全20_明日扣抵值影響_pct" numeric,
    "全60_明日扣抵值影響_pct" numeric,
    "全5MA乖離率" numeric,
    "全10MA乖離率" numeric,
    "全20MA乖離率" numeric,
    "全60MA乖離率" numeric,
    "全布林標準差" numeric,
    "全布林上軌" numeric,
    "全布林下軌" numeric,
    "日盤開" numeric,
    "日盤收" numeric,
    "日盤高" numeric,
    "日盤低" numeric,
    "日漲跌" numeric,
    "日價漲跌_pct" numeric,
    "日收減日開" numeric,
    "日收開_昨收_pct" numeric,
    "日收開_開盤_pct" numeric,
    "日振幅" numeric,
    "日振幅_昨收_pct" numeric,
    "日振幅_開盤_pct" numeric,
    "日成交量" numeric,
    "日5最大量_日期" date,
    "日5最大量" numeric,
    "日10最大量_日期" date,
    "日10最大量" numeric,
    "日20最大量_日期" date,
    "日20最大量" numeric,
    "日60最大量_日期" date,
    "日60最大量" numeric,
    "日5均量" numeric,
    "日10均量" numeric,
    "日20均量" numeric,
    "日60均量" numeric,
    "日量增減_昨日_pct" numeric,
    "日量增減_五日_pct" numeric,
    "日量增減_十日_pct" numeric,
    "日量增減_二十日_pct" numeric,
    "日量增減_六十日_pct" numeric,
    "日當日價量" text,
    "日5日價量" text,
    "日10日價量" text,
    "日20日價量" text,
    "日60日價量" text,
    "日K線型態" text,
    "日跳空缺口" double precision,
    "日跳空缺口_昨收_pct" double precision,
    "日實體_pct" double precision,
    "日上影_pct" double precision,
    "日下影_pct" double precision,
    "日5MA" numeric,
    "日10MA" numeric,
    "日20MA" numeric,
    "日60MA" numeric,
    "日120MA" numeric,
    "日240MA" double precision,
    "日5均上升幅度_點" numeric,
    "日10均上升幅度_點" numeric,
    "日20均上升幅度_點" numeric,
    "日60均上升幅度_點" numeric,
    "日5均上升幅度_pct" numeric,
    "日10均上升幅度_pct" numeric,
    "日20均上升幅度_pct" numeric,
    "日60均上升幅度_pct" numeric,
    "日5_明日扣抵值" numeric,
    "日10_明日扣抵值" numeric,
    "日20_明日扣抵值" numeric,
    "日60_明日扣抵值" numeric,
    "日5_明日扣抵值影響_點" numeric,
    "日10_明日扣抵值影響_點" numeric,
    "日20_明日扣抵值影響_點" numeric,
    "日60_明日扣抵值影響_點" numeric,
    "日5_明日扣抵值影響_pct" numeric,
    "日10_明日扣抵值影響_pct" numeric,
    "日20_明日扣抵值影響_pct" numeric,
    "日60_明日扣抵值影響_pct" numeric,
    "日5MA乖離率" numeric,
    "日10MA乖離率" numeric,
    "日20MA乖離率" numeric,
    "日60MA乖離率" numeric,
    "日布林標準差" numeric,
    "日布林上軌" numeric,
    "日布林下軌" numeric,
    "日法人多倉" double precision,
    "日法人空倉" double precision,
    "未沖銷_全" numeric,
    "未沖銷_近期" bigint,
    "未沖銷_遠期" bigint,
    "主力_月結算狀態" text,
    "主力新建多單" numeric,
    "主力新建空單" numeric,
    "主力淨多空和_5日" numeric,
    "主力淨多空和_10日" numeric,
    "主力淨多空和_20日" numeric,
    "主力淨多空和_60日" numeric,
    "夜盤開" double precision,
    "夜盤收" double precision,
    "夜盤高" double precision,
    "夜盤低" double precision,
    "夜漲跌" numeric,
    "夜價漲跌_pct" numeric,
    "夜成交量" double precision,
    "夜價量結構" text,
    "夜K線型態" text,
    "夜5MA乖離率" numeric,
    "夜10MA乖離率" numeric,
    "夜20MA乖離率" numeric,
    "夜60MA乖離率" numeric,
    "夜布林標準差" numeric,
    "夜布林上軌" numeric,
    "夜布林下軌" numeric,
    "夜法人多單量" double precision,
    "夜法人空單量" double precision,
    "全市場OI_小台" numeric,
    "三大法人多單_小台" numeric,
    "三大法人空單_小台" numeric,
    "散戶多單_小台" numeric,
    "散戶空單_小台" numeric,
    "散戶多空比_小台" numeric,
    "全市場OI_微台" numeric,
    "三大法人多單_微台" numeric,
    "三大法人空單_微台" numeric,
    "散戶多單_微台" numeric,
    "散戶空單_微台" numeric,
    "散戶多空比_微台" numeric,
    "法人總買超_億" numeric,
    "call_月選" numeric,
    "put_月選" numeric,
    "買賣權比_月選" numeric,
    "call_週三" numeric,
    "put_週三" numeric,
    "買賣權比_週三" numeric,
    "call_週五" numeric,
    "put_週五" numeric,
    "買賣權比_週五" numeric,
    "全位置" text,
    "策略_全價量昨日" text,
    "策略_全價量五日" text,
    "全MA方向_5" text,
    "全MA方向_10" text,
    "全MA方向_20" text,
    "策略_線型" text,
    "法人籌碼" text,
    "散戶籌碼" text,
    "加權籌碼" text,
    "期貨籌碼" text,
    "選權籌碼" text,
    "策略_籌碼" text,
    "策略_價量昨日_線型" text,
    "策略_價量五日_線型" text,
    "策略_價量昨日_籌碼" text,
    "策略_價量五日_籌碼" text,
    "策略_線型_籌碼" text,
    "策略_核心_昨價量" text,
    "策略_核心_五日價量" text,
    "建立時間" text,
    "夜盤資料來源" text
);


--
-- Name: TABLE futures_report_daily_dashboard_cache; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON TABLE report.futures_report_daily_dashboard_cache IS 'Dashboard 期貨日報表實體快取；核心策略版本 2026-09-15-v2。';


--
-- Name: futures_report_daily_tail_cache; Type: TABLE; Schema: report; Owner: -
--

CREATE TABLE report.futures_report_daily_tail_cache (
    id bigint,
    futures_id text,
    "日期" date,
    "年" bigint,
    "月" bigint,
    "星期" text,
    "週次" bigint,
    "天別" text,
    "全日開" numeric,
    "全日收" numeric,
    "全日高" numeric,
    "全日低" numeric,
    "全漲跌" numeric,
    "全價漲跌_pct" numeric,
    "全收減全開" numeric,
    "全收開_昨收_pct" numeric,
    "全收開_開盤_pct" numeric,
    "全振幅" numeric,
    "全振幅_昨收_pct" numeric,
    "全振幅_開盤_pct" numeric,
    "全成交量" numeric,
    "全5最大量_日期" date,
    "全5最大量" numeric,
    "全10最大量_日期" date,
    "全10最大量" numeric,
    "全20最大量_日期" date,
    "全20最大量" numeric,
    "全60最大量_日期" date,
    "全60最大量" numeric,
    "全5均量" numeric,
    "全10均量" numeric,
    "全20均量" numeric,
    "全60均量" numeric,
    "全量增減_昨日_pct" numeric,
    "全量增減_五日_pct" numeric,
    "全量增減_十日_pct" numeric,
    "全量增減_二十日_pct" numeric,
    "全量增減_六十日_pct" numeric,
    "全當日價量" text,
    "全5日價量" text,
    "全10日價量" text,
    "全20日價量" text,
    "全60日價量" text,
    "全K線型態" text,
    "全跳空缺口" numeric,
    "全跳空缺口_昨收_pct" numeric,
    "全實體_pct" numeric,
    "全上影_pct" numeric,
    "全下影_pct" numeric,
    "全5MA" numeric,
    "全10MA" numeric,
    "全20MA" numeric,
    "全60MA" numeric,
    "全120MA" numeric,
    "全240MA" numeric,
    "全5均上升幅度_點" numeric,
    "全10均上升幅度_點" numeric,
    "全20均上升幅度_點" numeric,
    "全60均上升幅度_點" numeric,
    "全5均上升幅度_pct" numeric,
    "全10均上升幅度_pct" numeric,
    "全20均上升幅度_pct" numeric,
    "全60均上升幅度_pct" numeric,
    "全5_明日扣抵值" numeric,
    "全10_明日扣抵值" numeric,
    "全20_明日扣抵值" numeric,
    "全60_明日扣抵值" numeric,
    "全5_明日扣抵值影響_點" numeric,
    "全10_明日扣抵值影響_點" numeric,
    "全20_明日扣抵值影響_點" numeric,
    "全60_明日扣抵值影響_點" numeric,
    "全5_明日扣抵值影響_pct" numeric,
    "全10_明日扣抵值影響_pct" numeric,
    "全20_明日扣抵值影響_pct" numeric,
    "全60_明日扣抵值影響_pct" numeric,
    "全5MA乖離率" numeric,
    "全10MA乖離率" numeric,
    "全20MA乖離率" numeric,
    "全60MA乖離率" numeric,
    "全布林標準差" numeric,
    "全布林上軌" numeric,
    "全布林下軌" numeric,
    "日盤開" numeric,
    "日盤收" numeric,
    "日盤高" numeric,
    "日盤低" numeric,
    "日漲跌" numeric,
    "日價漲跌_pct" numeric,
    "日收減日開" numeric,
    "日收開_昨收_pct" numeric,
    "日收開_開盤_pct" numeric,
    "日振幅" numeric,
    "日振幅_昨收_pct" numeric,
    "日振幅_開盤_pct" numeric,
    "日成交量" numeric,
    "日5最大量_日期" date,
    "日5最大量" numeric,
    "日10最大量_日期" date,
    "日10最大量" numeric,
    "日20最大量_日期" date,
    "日20最大量" numeric,
    "日60最大量_日期" date,
    "日60最大量" numeric,
    "日5均量" numeric,
    "日10均量" numeric,
    "日20均量" numeric,
    "日60均量" numeric,
    "日量增減_昨日_pct" numeric,
    "日量增減_五日_pct" numeric,
    "日量增減_十日_pct" numeric,
    "日量增減_二十日_pct" numeric,
    "日量增減_六十日_pct" numeric,
    "日當日價量" text,
    "日5日價量" text,
    "日10日價量" text,
    "日20日價量" text,
    "日60日價量" text,
    "日K線型態" text,
    "日跳空缺口" double precision,
    "日跳空缺口_昨收_pct" double precision,
    "日實體_pct" double precision,
    "日上影_pct" double precision,
    "日下影_pct" double precision,
    "日5MA" numeric,
    "日10MA" numeric,
    "日20MA" numeric,
    "日60MA" numeric,
    "日120MA" numeric,
    "日240MA" double precision,
    "日5均上升幅度_點" numeric,
    "日10均上升幅度_點" numeric,
    "日20均上升幅度_點" numeric,
    "日60均上升幅度_點" numeric,
    "日5均上升幅度_pct" numeric,
    "日10均上升幅度_pct" numeric,
    "日20均上升幅度_pct" numeric,
    "日60均上升幅度_pct" numeric,
    "日5_明日扣抵值" numeric,
    "日10_明日扣抵值" numeric,
    "日20_明日扣抵值" numeric,
    "日60_明日扣抵值" numeric,
    "日5_明日扣抵值影響_點" numeric,
    "日10_明日扣抵值影響_點" numeric,
    "日20_明日扣抵值影響_點" numeric,
    "日60_明日扣抵值影響_點" numeric,
    "日5_明日扣抵值影響_pct" numeric,
    "日10_明日扣抵值影響_pct" numeric,
    "日20_明日扣抵值影響_pct" numeric,
    "日60_明日扣抵值影響_pct" numeric,
    "日5MA乖離率" numeric,
    "日10MA乖離率" numeric,
    "日20MA乖離率" numeric,
    "日60MA乖離率" numeric,
    "日布林標準差" numeric,
    "日布林上軌" numeric,
    "日布林下軌" numeric,
    "日法人多倉" double precision,
    "日法人空倉" double precision,
    "未沖銷_全" numeric,
    "未沖銷_近期" bigint,
    "未沖銷_遠期" bigint,
    "主力_月結算狀態" text,
    "主力新建多單" numeric,
    "主力新建空單" numeric,
    "主力淨多空和_5日" numeric,
    "主力淨多空和_10日" numeric,
    "主力淨多空和_20日" numeric,
    "主力淨多空和_60日" numeric,
    "夜盤開" numeric,
    "夜盤收" numeric,
    "夜盤高" numeric,
    "夜盤低" numeric,
    "夜漲跌" numeric,
    "夜價漲跌_pct" numeric,
    "夜成交量" numeric,
    "夜價量結構" text,
    "夜K線型態" text,
    "夜5MA乖離率" numeric,
    "夜10MA乖離率" numeric,
    "夜20MA乖離率" numeric,
    "夜60MA乖離率" numeric,
    "夜布林標準差" numeric,
    "夜布林上軌" numeric,
    "夜布林下軌" numeric,
    "夜法人多單量" double precision,
    "夜法人空單量" double precision,
    "全市場OI_小台" numeric,
    "三大法人多單_小台" numeric,
    "三大法人空單_小台" numeric,
    "散戶多單_小台" numeric,
    "散戶空單_小台" numeric,
    "散戶多空比_小台" numeric,
    "全市場OI_微台" numeric,
    "三大法人多單_微台" numeric,
    "三大法人空單_微台" numeric,
    "散戶多單_微台" numeric,
    "散戶空單_微台" numeric,
    "散戶多空比_微台" numeric,
    "法人總買超_億" numeric,
    "call_月選" numeric,
    "put_月選" numeric,
    "買賣權比_月選" numeric,
    "call_週三" numeric,
    "put_週三" numeric,
    "買賣權比_週三" numeric,
    "call_週五" numeric,
    "put_週五" numeric,
    "買賣權比_週五" numeric,
    "全位置" text,
    "策略_全價量昨日" text,
    "策略_全價量五日" text,
    "全MA方向_5" text,
    "全MA方向_10" text,
    "全MA方向_20" text,
    "策略_線型" text,
    "法人籌碼" text,
    "散戶籌碼" text,
    "加權籌碼" text,
    "期貨籌碼" text,
    "選權籌碼" text,
    "策略_籌碼" text,
    "策略_價量昨日_線型" text,
    "策略_價量五日_線型" text,
    "策略_價量昨日_籌碼" text,
    "策略_價量五日_籌碼" text,
    "策略_線型_籌碼" text,
    "策略_核心" text,
    "建立時間" text
);


--
-- Name: futures_turning_5m_bars; Type: TABLE; Schema: report; Owner: -
--

CREATE TABLE report.futures_turning_5m_bars (
    bar_time timestamp without time zone NOT NULL,
    session_date date NOT NULL,
    session text NOT NULL,
    high numeric NOT NULL,
    low numeric NOT NULL,
    close numeric NOT NULL,
    last_time timestamp without time zone NOT NULL,
    dif double precision,
    direction smallint,
    ema_state jsonb NOT NULL
);


--
-- Name: futures_turning_levels; Type: TABLE; Schema: report; Owner: -
--

CREATE TABLE report.futures_turning_levels (
    futures_id text DEFAULT 'TX'::text NOT NULL,
    "日期" date NOT NULL,
    "日盤轉折支撐" numeric,
    "日盤轉折壓力" numeric,
    "夜盤轉折支撐" numeric,
    "夜盤轉折壓力" numeric
);


--
-- Name: v_futures_main_force; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_main_force AS
 WITH raw_tx AS (
         SELECT r.futures_id,
            r."日期",
            r."星期",
            r."日盤開",
            r."日盤收",
            r."日法人多倉",
            r."日法人空倉",
            to_char((r."日期")::timestamp with time zone, 'YYYYMM'::text) AS contract_month,
            (date_trunc('month'::text, (r."日期")::timestamp with time zone))::date AS month_start
           FROM core.futures_report_daily_raw r
          WHERE (r.futures_id = 'TX'::text)
        ), calendar_settlement AS (
         SELECT r.futures_id,
            r."日期",
            r."星期",
            r."日盤開",
            r."日盤收",
            r."日法人多倉",
            r."日法人空倉",
            r.contract_month,
            r.month_start,
            (r.month_start + ((((3 - (EXTRACT(dow FROM r.month_start))::integer) + 7) % 7) + 14)) AS "理論月結算日"
           FROM raw_tx r
        ), actual_settlement AS (
         SELECT f_1.contract_month,
            min(f_1.date) AS "實際月結算日"
           FROM raw_finmind.fm_taiwan_futures_final_settlement_price f_1
          WHERE ((f_1.futures_id = 'TX'::text) AND (f_1.futures_type = 'index'::text) AND (f_1.contract_month !~~ '%W%'::text) AND (length(f_1.contract_month) = 6))
          GROUP BY f_1.contract_month
        ), date_status AS (
         SELECT c.futures_id,
            c."日期",
            c."星期",
            c."日盤開",
            c."日盤收",
            c."日法人多倉",
            c."日法人空倉",
            c.contract_month,
            c.month_start,
            c."理論月結算日",
            COALESCE(a."實際月結算日", c."理論月結算日") AS "月結算日",
                CASE
                    WHEN (c."日期" = COALESCE(a."實際月結算日", c."理論月結算日")) THEN 1
                    ELSE 0
                END AS "是否月結算當日",
                CASE
                    WHEN ((c."日期" >= (COALESCE(a."實際月結算日", c."理論月結算日") - 7)) AND (c."日期" <= COALESCE(a."實際月結算日", c."理論月結算日"))) THEN 1
                    ELSE 0
                END AS "是否月結算週"
           FROM (calendar_settlement c
             LEFT JOIN actual_settlement a ON ((a.contract_month = c.contract_month)))
        ), open_interest_rank AS (
         SELECT f_1.date AS "日期",
            f_1.futures_id,
            f_1.contract_date,
            f_1.open_interest,
            row_number() OVER (PARTITION BY f_1.date, f_1.futures_id ORDER BY f_1.contract_date) AS rn
           FROM raw_finmind.fm_taiwan_futures_daily f_1
          WHERE ((f_1.futures_id = 'TX'::text) AND (f_1.trading_session = 'position'::text) AND (f_1.contract_date !~~ '%/%'::text) AND (f_1.contract_date !~~ '%W%'::text) AND (length(f_1.contract_date) = 6))
        ), open_interest_calc AS (
         SELECT open_interest_rank."日期",
            open_interest_rank.futures_id,
            sum(open_interest_rank.open_interest) AS "未沖銷_全",
            max(
                CASE
                    WHEN (open_interest_rank.rn = 1) THEN open_interest_rank.open_interest
                    ELSE NULL::bigint
                END) AS "未沖銷_近期",
            max(
                CASE
                    WHEN (open_interest_rank.rn = 2) THEN open_interest_rank.open_interest
                    ELSE NULL::bigint
                END) AS "未沖銷_遠期"
           FROM open_interest_rank
          GROUP BY open_interest_rank."日期", open_interest_rank.futures_id
        ), base AS (
         SELECT d.futures_id,
            d."日期",
            d."星期",
            d."日盤開",
            d."日盤收",
            d."日法人多倉",
            d."日法人空倉",
            d.contract_month,
            d.month_start,
            d."理論月結算日",
            d."月結算日",
            d."是否月結算當日",
            d."是否月結算週",
            oi."未沖銷_全",
            oi."未沖銷_近期",
            oi."未沖銷_遠期"
           FROM (date_status d
             LEFT JOIN open_interest_calc oi ON (((oi."日期" = d."日期") AND (oi.futures_id = d.futures_id))))
        ), lag_base AS (
         SELECT b.futures_id,
            b."日期",
            b."星期",
            b."日盤開",
            b."日盤收",
            b."日法人多倉",
            b."日法人空倉",
            b.contract_month,
            b.month_start,
            b."理論月結算日",
            b."月結算日",
            b."是否月結算當日",
            b."是否月結算週",
            b."未沖銷_全",
            b."未沖銷_近期",
            b."未沖銷_遠期",
            lag(b."日盤收") OVER (PARTITION BY b.futures_id ORDER BY b."日期") AS "昨日日盤收",
            lag(b."未沖銷_全") OVER (PARTITION BY b.futures_id ORDER BY b."日期") AS "昨日未沖銷_全",
            lag(b."未沖銷_近期") OVER (PARTITION BY b.futures_id ORDER BY b."日期") AS "昨日未沖銷_近期",
            lag(b."未沖銷_遠期") OVER (PARTITION BY b.futures_id ORDER BY b."日期") AS "昨日未沖銷_遠期"
           FROM base b
        ), oi_adjust AS (
         SELECT l.futures_id,
            l."日期",
            l."星期",
            l."日盤開",
            l."日盤收",
            l."日法人多倉",
            l."日法人空倉",
            l.contract_month,
            l.month_start,
            l."理論月結算日",
            l."月結算日",
            l."是否月結算當日",
            l."是否月結算週",
            l."未沖銷_全",
            l."未沖銷_近期",
            l."未沖銷_遠期",
            l."昨日日盤收",
            l."昨日未沖銷_全",
            l."昨日未沖銷_近期",
            l."昨日未沖銷_遠期",
            (l."未沖銷_全" - l."昨日未沖銷_全") AS "未沖銷_全_變化",
            (l."未沖銷_近期" - l."昨日未沖銷_近期") AS "未沖銷_近期_變化",
            (l."未沖銷_遠期" - l."昨日未沖銷_遠期") AS "未沖銷_遠期_變化",
                CASE
                    WHEN ((l."是否月結算當日" = 1) OR ((l."是否月結算週" = 1) AND (l."星期" = ANY (ARRAY['星期一'::text, '星期二'::text])))) THEN ((l."未沖銷_全" + ((l."未沖銷_近期" - l."昨日未沖銷_近期"))::numeric) + ((l."未沖銷_遠期" - l."昨日未沖銷_遠期"))::numeric)
                    ELSE l."未沖銷_全"
                END AS "未沖銷_全_修正"
           FROM lag_base l
        ), inst_adjust AS (
         SELECT o.futures_id,
            o."日期",
            o."星期",
            o."日盤開",
            o."日盤收",
            o."日法人多倉",
            o."日法人空倉",
            o.contract_month,
            o.month_start,
            o."理論月結算日",
            o."月結算日",
            o."是否月結算當日",
            o."是否月結算週",
            o."未沖銷_全",
            o."未沖銷_近期",
            o."未沖銷_遠期",
            o."昨日日盤收",
            o."昨日未沖銷_全",
            o."昨日未沖銷_近期",
            o."昨日未沖銷_遠期",
            o."未沖銷_全_變化",
            o."未沖銷_近期_變化",
            o."未沖銷_遠期_變化",
            o."未沖銷_全_修正",
                CASE
                    WHEN (o."是否月結算當日" = 1) THEN
                    CASE
                        WHEN ((o."未沖銷_全" IS NULL) OR (o."未沖銷_全" = (0)::numeric) OR ((1.0 - ((o."未沖銷_近期")::numeric / NULLIF(o."未沖銷_全", (0)::numeric))) = (0)::numeric)) THEN NULL::double precision
                        ELSE ((o."日法人多倉" + (((o."日法人多倉" / (NULLIF(o."未沖銷_全", (0)::numeric))::double precision) / (NULLIF((1.0 - ((o."未沖銷_近期")::numeric / NULLIF(o."未沖銷_全", (0)::numeric))), (0)::numeric))::double precision) * (o."未沖銷_近期")::double precision)) - (((o."日法人多倉" / (NULLIF(o."未沖銷_全", (0)::numeric))::double precision) / (NULLIF((1.0 - ((o."未沖銷_近期")::numeric / NULLIF(o."未沖銷_全", (0)::numeric))), (0)::numeric))::double precision) * ((o."未沖銷_近期_變化" + o."未沖銷_遠期_變化"))::double precision))
                    END
                    WHEN ((o."是否月結算週" = 1) AND (o."星期" = ANY (ARRAY['星期一'::text, '星期二'::text]))) THEN
                    CASE
                        WHEN ((o."未沖銷_全" IS NULL) OR (o."未沖銷_全" = (0)::numeric)) THEN NULL::double precision
                        ELSE (o."日法人多倉" + (((o."未沖銷_近期_變化" + o."未沖銷_遠期_變化"))::double precision * (o."日法人多倉" / (NULLIF(o."未沖銷_全", (0)::numeric))::double precision)))
                    END
                    ELSE o."日法人多倉"
                END AS "法人多倉_修正",
                CASE
                    WHEN (o."是否月結算當日" = 1) THEN
                    CASE
                        WHEN ((o."未沖銷_全" IS NULL) OR (o."未沖銷_全" = (0)::numeric) OR ((1.0 - ((o."未沖銷_近期")::numeric / NULLIF(o."未沖銷_全", (0)::numeric))) = (0)::numeric)) THEN NULL::double precision
                        ELSE ((o."日法人空倉" + (((o."日法人空倉" / (NULLIF(o."未沖銷_全", (0)::numeric))::double precision) / (NULLIF((1.0 - ((o."未沖銷_近期")::numeric / NULLIF(o."未沖銷_全", (0)::numeric))), (0)::numeric))::double precision) * (o."未沖銷_近期")::double precision)) - (((o."日法人空倉" / (NULLIF(o."未沖銷_全", (0)::numeric))::double precision) / (NULLIF((1.0 - ((o."未沖銷_近期")::numeric / NULLIF(o."未沖銷_全", (0)::numeric))), (0)::numeric))::double precision) * ((o."未沖銷_近期_變化" + o."未沖銷_遠期_變化"))::double precision))
                    END
                    WHEN ((o."是否月結算週" = 1) AND (o."星期" = ANY (ARRAY['星期一'::text, '星期二'::text]))) THEN
                    CASE
                        WHEN ((o."未沖銷_全" IS NULL) OR (o."未沖銷_全" = (0)::numeric)) THEN NULL::double precision
                        ELSE (o."日法人空倉" + (((o."未沖銷_近期_變化" + o."未沖銷_遠期_變化"))::double precision * (o."日法人空倉" / (NULLIF(o."未沖銷_全", (0)::numeric))::double precision)))
                    END
                    ELSE o."日法人空倉"
                END AS "法人空倉_修正"
           FROM oi_adjust o
        ), inst_change AS (
         SELECT i.futures_id,
            i."日期",
            i."星期",
            i."日盤開",
            i."日盤收",
            i."日法人多倉",
            i."日法人空倉",
            i.contract_month,
            i.month_start,
            i."理論月結算日",
            i."月結算日",
            i."是否月結算當日",
            i."是否月結算週",
            i."未沖銷_全",
            i."未沖銷_近期",
            i."未沖銷_遠期",
            i."昨日日盤收",
            i."昨日未沖銷_全",
            i."昨日未沖銷_近期",
            i."昨日未沖銷_遠期",
            i."未沖銷_全_變化",
            i."未沖銷_近期_變化",
            i."未沖銷_遠期_變化",
            i."未沖銷_全_修正",
            i."法人多倉_修正",
            i."法人空倉_修正",
            (i."法人多倉_修正" - i."法人空倉_修正") AS "法人淨多空_修正",
            (i."法人多倉_修正" - lag(i."法人多倉_修正") OVER (PARTITION BY i.futures_id ORDER BY i."日期")) AS "法人多倉_修正_變化",
            (i."法人空倉_修正" - lag(i."法人空倉_修正") OVER (PARTITION BY i.futures_id ORDER BY i."日期")) AS "法人空倉_修正_變化",
            ((i."法人多倉_修正" - i."法人空倉_修正") - lag((i."法人多倉_修正" - i."法人空倉_修正")) OVER (PARTITION BY i.futures_id ORDER BY i."日期")) AS "法人淨多空_修正_變化"
           FROM inst_adjust i
        ), non_inst AS (
         SELECT i.futures_id,
            i."日期",
            i."星期",
            i."日盤開",
            i."日盤收",
            i."日法人多倉",
            i."日法人空倉",
            i.contract_month,
            i.month_start,
            i."理論月結算日",
            i."月結算日",
            i."是否月結算當日",
            i."是否月結算週",
            i."未沖銷_全",
            i."未沖銷_近期",
            i."未沖銷_遠期",
            i."昨日日盤收",
            i."昨日未沖銷_全",
            i."昨日未沖銷_近期",
            i."昨日未沖銷_遠期",
            i."未沖銷_全_變化",
            i."未沖銷_近期_變化",
            i."未沖銷_遠期_變化",
            i."未沖銷_全_修正",
            i."法人多倉_修正",
            i."法人空倉_修正",
            i."法人淨多空_修正",
            i."法人多倉_修正_變化",
            i."法人空倉_修正_變化",
            i."法人淨多空_修正_變化",
            ((i."未沖銷_全_修正")::double precision - i."法人多倉_修正") AS "法人以外多倉",
            ((i."未沖銷_全_修正")::double precision - i."法人空倉_修正") AS "法人以外空倉",
            (((i."未沖銷_全_修正")::double precision - i."法人多倉_修正") - ((i."未沖銷_全_修正")::double precision - i."法人空倉_修正")) AS "法人以外淨多空"
           FROM inst_change i
        ), non_inst_change AS (
         SELECT n.futures_id,
            n."日期",
            n."星期",
            n."日盤開",
            n."日盤收",
            n."日法人多倉",
            n."日法人空倉",
            n.contract_month,
            n.month_start,
            n."理論月結算日",
            n."月結算日",
            n."是否月結算當日",
            n."是否月結算週",
            n."未沖銷_全",
            n."未沖銷_近期",
            n."未沖銷_遠期",
            n."昨日日盤收",
            n."昨日未沖銷_全",
            n."昨日未沖銷_近期",
            n."昨日未沖銷_遠期",
            n."未沖銷_全_變化",
            n."未沖銷_近期_變化",
            n."未沖銷_遠期_變化",
            n."未沖銷_全_修正",
            n."法人多倉_修正",
            n."法人空倉_修正",
            n."法人淨多空_修正",
            n."法人多倉_修正_變化",
            n."法人空倉_修正_變化",
            n."法人淨多空_修正_變化",
            n."法人以外多倉",
            n."法人以外空倉",
            n."法人以外淨多空",
            (n."法人以外多倉" - lag(n."法人以外多倉") OVER (PARTITION BY n.futures_id ORDER BY n."日期")) AS "法人以外多倉_變化",
            (n."法人以外空倉" - lag(n."法人以外空倉") OVER (PARTITION BY n.futures_id ORDER BY n."日期")) AS "法人以外空倉_變化",
            (n."法人以外淨多空" - lag(n."法人以外淨多空") OVER (PARTITION BY n.futures_id ORDER BY n."日期")) AS "法人以外淨多空_變化"
           FROM non_inst n
        ), direction_calc AS (
         SELECT n.futures_id,
            n."日期",
            n."星期",
            n."日盤開",
            n."日盤收",
            n."日法人多倉",
            n."日法人空倉",
            n.contract_month,
            n.month_start,
            n."理論月結算日",
            n."月結算日",
            n."是否月結算當日",
            n."是否月結算週",
            n."未沖銷_全",
            n."未沖銷_近期",
            n."未沖銷_遠期",
            n."昨日日盤收",
            n."昨日未沖銷_全",
            n."昨日未沖銷_近期",
            n."昨日未沖銷_遠期",
            n."未沖銷_全_變化",
            n."未沖銷_近期_變化",
            n."未沖銷_遠期_變化",
            n."未沖銷_全_修正",
            n."法人多倉_修正",
            n."法人空倉_修正",
            n."法人淨多空_修正",
            n."法人多倉_修正_變化",
            n."法人空倉_修正_變化",
            n."法人淨多空_修正_變化",
            n."法人以外多倉",
            n."法人以外空倉",
            n."法人以外淨多空",
            n."法人以外多倉_變化",
            n."法人以外空倉_變化",
            n."法人以外淨多空_變化",
            abs((n."日盤收" - n."日盤開")) AS "日盤實體",
            (n."日盤收" - n."昨日日盤收") AS "收盤平盤差",
                CASE
                    WHEN ((n."日盤收" IS NULL) OR (n."日盤開" IS NULL) OR (n."昨日日盤收" IS NULL)) THEN NULL::integer
                    WHEN (abs((n."日盤收" - n."昨日日盤收")) <= (50)::double precision) THEN 0
                    WHEN (n."日盤開" > n."日盤收") THEN 1
                    WHEN ((n."日盤開" < n."昨日日盤收") AND (n."日盤收" < n."昨日日盤收")) THEN 1
                    ELSE '-1'::integer
                END AS "散戶多方方向權重"
           FROM non_inst_change n
        ), direction_calc2 AS (
         SELECT d.futures_id,
            d."日期",
            d."星期",
            d."日盤開",
            d."日盤收",
            d."日法人多倉",
            d."日法人空倉",
            d.contract_month,
            d.month_start,
            d."理論月結算日",
            d."月結算日",
            d."是否月結算當日",
            d."是否月結算週",
            d."未沖銷_全",
            d."未沖銷_近期",
            d."未沖銷_遠期",
            d."昨日日盤收",
            d."昨日未沖銷_全",
            d."昨日未沖銷_近期",
            d."昨日未沖銷_遠期",
            d."未沖銷_全_變化",
            d."未沖銷_近期_變化",
            d."未沖銷_遠期_變化",
            d."未沖銷_全_修正",
            d."法人多倉_修正",
            d."法人空倉_修正",
            d."法人淨多空_修正",
            d."法人多倉_修正_變化",
            d."法人空倉_修正_變化",
            d."法人淨多空_修正_變化",
            d."法人以外多倉",
            d."法人以外空倉",
            d."法人以外淨多空",
            d."法人以外多倉_變化",
            d."法人以外空倉_變化",
            d."法人以外淨多空_變化",
            d."日盤實體",
            d."收盤平盤差",
            d."散戶多方方向權重",
                CASE
                    WHEN (d."散戶多方方向權重" IS NULL) THEN NULL::integer
                    ELSE ('-1'::integer * d."散戶多方方向權重")
                END AS "散戶空方方向權重",
                CASE
                    WHEN (d."法人多倉_修正_變化" > (0)::double precision) THEN 1
                    WHEN (d."法人多倉_修正_變化" < (0)::double precision) THEN '-1'::integer
                    WHEN (d."法人多倉_修正_變化" = (0)::double precision) THEN 0
                    ELSE NULL::integer
                END AS "法人多倉_修正_變化_sign",
                CASE
                    WHEN (d."法人空倉_修正_變化" > (0)::double precision) THEN 1
                    WHEN (d."法人空倉_修正_變化" < (0)::double precision) THEN '-1'::integer
                    WHEN (d."法人空倉_修正_變化" = (0)::double precision) THEN 0
                    ELSE NULL::integer
                END AS "法人空倉_修正_變化_sign"
           FROM direction_calc d
        ), retail_est AS (
         SELECT d.futures_id,
            d."日期",
            d."星期",
            d."日盤開",
            d."日盤收",
            d."日法人多倉",
            d."日法人空倉",
            d.contract_month,
            d.month_start,
            d."理論月結算日",
            d."月結算日",
            d."是否月結算當日",
            d."是否月結算週",
            d."未沖銷_全",
            d."未沖銷_近期",
            d."未沖銷_遠期",
            d."昨日日盤收",
            d."昨日未沖銷_全",
            d."昨日未沖銷_近期",
            d."昨日未沖銷_遠期",
            d."未沖銷_全_變化",
            d."未沖銷_近期_變化",
            d."未沖銷_遠期_變化",
            d."未沖銷_全_修正",
            d."法人多倉_修正",
            d."法人空倉_修正",
            d."法人淨多空_修正",
            d."法人多倉_修正_變化",
            d."法人空倉_修正_變化",
            d."法人淨多空_修正_變化",
            d."法人以外多倉",
            d."法人以外空倉",
            d."法人以外淨多空",
            d."法人以外多倉_變化",
            d."法人以外空倉_變化",
            d."法人以外淨多空_變化",
            d."日盤實體",
            d."收盤平盤差",
            d."散戶多方方向權重",
            d."散戶空方方向權重",
            d."法人多倉_修正_變化_sign",
            d."法人空倉_修正_變化_sign",
            (abs(
                CASE
                    WHEN (d."日盤實體" >= (125)::double precision) THEN
                    CASE
                        WHEN (abs(d."法人多倉_修正_變化") < (1000)::double precision) THEN ((((d."法人多倉_修正_變化_sign")::numeric * 1000.0) / 2.0))::double precision
                        ELSE (d."法人多倉_修正_變化" / (2.0)::double precision)
                    END
                    ELSE
                    CASE
                        WHEN (abs(d."法人多倉_修正_變化") < (1000)::double precision) THEN ((((d."法人多倉_修正_變化_sign")::numeric * 1000.0) / 3.0))::double precision
                        ELSE (d."法人多倉_修正_變化" / (3.0)::double precision)
                    END
                END) * (d."散戶多方方向權重")::double precision) AS "散戶多方估計",
            (abs(
                CASE
                    WHEN (d."日盤實體" >= (125)::double precision) THEN
                    CASE
                        WHEN (abs(d."法人空倉_修正_變化") < (1000)::double precision) THEN ((((d."法人空倉_修正_變化_sign")::numeric * 1000.0) / 2.0))::double precision
                        ELSE (d."法人空倉_修正_變化" / (2.0)::double precision)
                    END
                    ELSE
                    CASE
                        WHEN (abs(d."法人空倉_修正_變化") < (1000)::double precision) THEN ((((d."法人空倉_修正_變化_sign")::numeric * 1000.0) / 3.0))::double precision
                        ELSE (d."法人空倉_修正_變化" / (3.0)::double precision)
                    END
                END) * (d."散戶空方方向權重")::double precision) AS "散戶空方估計"
           FROM direction_calc2 d
        ), final_base AS (
         SELECT r.futures_id,
            r."日期",
            r."星期",
            r."理論月結算日",
            r."月結算日",
                CASE
                    WHEN (r."是否月結算當日" = 1) THEN '月結算當日'::text
                    WHEN (r."是否月結算週" = 1) THEN '月結算週'::text
                    ELSE '一般日'::text
                END AS "月結算狀態",
            r."是否月結算當日",
            r."是否月結算週",
            r."日盤開",
            r."日盤收",
            r."昨日日盤收",
            r."日盤實體",
            r."收盤平盤差",
            r."日法人多倉" AS "法人多倉_原始",
            r."日法人空倉" AS "法人空倉_原始",
            r."未沖銷_全",
            r."未沖銷_近期",
            r."未沖銷_遠期",
            r."未沖銷_全_變化",
            r."未沖銷_近期_變化",
            r."未沖銷_遠期_變化",
            r."未沖銷_全_修正",
            r."法人多倉_修正",
            r."法人空倉_修正",
            r."法人淨多空_修正",
            r."法人多倉_修正_變化",
            r."法人空倉_修正_變化",
            r."法人淨多空_修正_變化",
            r."法人以外多倉",
            r."法人以外空倉",
            r."法人以外淨多空",
            r."法人以外多倉_變化",
            r."法人以外空倉_變化",
            r."法人以外淨多空_變化",
            r."散戶多方方向權重",
            r."散戶空方方向權重",
            r."散戶多方估計",
            r."散戶空方估計",
            (r."散戶多方估計" - r."散戶空方估計") AS "散戶淨多空估計",
            (r."法人以外多倉_變化" - r."散戶多方估計") AS "主力新建多單",
            ((('-1'::integer)::double precision * r."法人以外空倉_變化") - r."散戶空方估計") AS "主力新建空單",
            ((r."法人以外多倉_變化" - r."散戶多方估計") - ((('-1'::integer)::double precision * r."法人以外空倉_變化") - r."散戶空方估計")) AS "主力淨多空"
           FROM retail_est r
        )
 SELECT futures_id,
    "日期",
    "星期",
    "理論月結算日",
    "月結算日",
    "月結算狀態",
    "是否月結算當日",
    "是否月結算週",
    "日盤開",
    "日盤收",
    "昨日日盤收",
    "日盤實體",
    "收盤平盤差",
    "法人多倉_原始",
    "法人空倉_原始",
    "未沖銷_全",
    "未沖銷_近期",
    "未沖銷_遠期",
    "未沖銷_全_變化",
    "未沖銷_近期_變化",
    "未沖銷_遠期_變化",
    "未沖銷_全_修正",
    "法人多倉_修正",
    "法人空倉_修正",
    "法人淨多空_修正",
    "法人多倉_修正_變化",
    "法人空倉_修正_變化",
    "法人淨多空_修正_變化",
    "法人以外多倉",
    "法人以外空倉",
    "法人以外淨多空",
    "法人以外多倉_變化",
    "法人以外空倉_變化",
    "法人以外淨多空_變化",
    "散戶多方方向權重",
    "散戶空方方向權重",
    "散戶多方估計",
    "散戶空方估計",
    "散戶淨多空估計",
    "主力新建多單",
    "主力新建空單",
    "主力淨多空",
    sum("主力淨多空") OVER (PARTITION BY futures_id ORDER BY "日期" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS "主力淨多空和_5日",
    sum("主力淨多空") OVER (PARTITION BY futures_id ORDER BY "日期" ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS "主力淨多空和_10日",
    sum("主力淨多空") OVER (PARTITION BY futures_id ORDER BY "日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS "主力淨多空和_20日",
    sum("主力淨多空") OVER (PARTITION BY futures_id ORDER BY "日期" ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS "主力淨多空和_60日"
   FROM final_base f;


--
-- Name: v_futures_report_daily_model; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_report_daily_model AS
 WITH settlement_calc AS (
         SELECT unnamed_subquery."日期",
            unnamed_subquery.futures_id,
                CASE
                    WHEN ((unnamed_subquery.settlement_price = (0)::double precision) OR (unnamed_subquery.settlement_price IS NULL)) THEN unnamed_subquery.close
                    ELSE unnamed_subquery.settlement_price
                END AS "全日收"
           FROM ( SELECT f.date AS "日期",
                    f.futures_id,
                    f.contract_date,
                    f.settlement_price,
                    f.close,
                    row_number() OVER (PARTITION BY f.date, f.futures_id ORDER BY f.contract_date) AS rn
                   FROM raw_finmind.fm_taiwan_futures_daily f
                  WHERE ((f.futures_id = 'TX'::text) AND (f.trading_session = 'position'::text) AND (length(f.contract_date) = 6))) unnamed_subquery
          WHERE (unnamed_subquery.rn = 1)
        ), base AS (
         SELECT r.id,
            r.futures_id,
            r."日期",
            r."年",
            r."月",
            r."星期",
            r."週次",
            r."天別",
            r."日盤開",
            r."日盤收",
            r."日盤高",
            r."日盤低",
            r."日漲跌",
            r."日振幅",
            r."日振幅開盤比",
            r."日振幅昨收比",
            r."日收減日開",
            r."日收開昨收比",
            r."夜盤開",
            r."夜盤收",
            r."夜盤高",
            r."夜盤低",
            r."日法人多倉",
            r."日法人空倉",
            r."夜法人多單量",
            r."夜法人空單量",
            r."call_月選",
            r."put_月選",
            r."call_週三",
            r."put_週三",
            r."call_週五",
            r."put_週五",
            r."日5MA",
            r."日10MA",
            r."日20MA",
            r."日60MA",
            r."日120MA",
            r."夜5MA",
            r."夜10MA",
            r."夜20MA",
            r."夜60MA",
            r."夜120MA",
            r."日成交量",
            r."夜成交量",
            r."日當日價量",
            r."日5日價量",
            r."日10日價量",
            r."日20日價量",
            r."日60日價量",
            r."日布林標準差",
            r."日布林上軌",
            r."日布林下軌",
            lag(r."日盤收") OVER (PARTITION BY r.futures_id ORDER BY r."日期") AS "昨日收盤",
            lag(r."日成交量") OVER (PARTITION BY r.futures_id ORDER BY r."日期") AS "昨日成交量",
            avg(r."日成交量") OVER (PARTITION BY r.futures_id ORDER BY r."日期" ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS "日5均量",
            avg(r."日成交量") OVER (PARTITION BY r.futures_id ORDER BY r."日期" ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS "日10均量",
            avg(r."日成交量") OVER (PARTITION BY r.futures_id ORDER BY r."日期" ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS "日20均量",
            avg(r."日成交量") OVER (PARTITION BY r.futures_id ORDER BY r."日期" ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) AS "日60均量",
            max(r."日成交量") OVER (PARTITION BY r.futures_id ORDER BY r."日期" ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS "日5最大量",
            max(r."日成交量") OVER (PARTITION BY r.futures_id ORDER BY r."日期" ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS "日10最大量",
            max(r."日成交量") OVER (PARTITION BY r.futures_id ORDER BY r."日期" ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS "日20最大量",
            max(r."日成交量") OVER (PARTITION BY r.futures_id ORDER BY r."日期" ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) AS "日60最大量",
            ( SELECT x."日期"
                   FROM ( SELECT r2."日期",
                            r2."日成交量"
                           FROM core.futures_report_daily_raw r2
                          WHERE ((r2.futures_id = r.futures_id) AND (r2."日期" < r."日期"))
                          ORDER BY r2."日期" DESC
                         LIMIT 5) x
                  ORDER BY x."日成交量" DESC, x."日期" DESC
                 LIMIT 1) AS "日5最大量_日期",
            ( SELECT x."日期"
                   FROM ( SELECT r2."日期",
                            r2."日成交量"
                           FROM core.futures_report_daily_raw r2
                          WHERE ((r2.futures_id = r.futures_id) AND (r2."日期" < r."日期"))
                          ORDER BY r2."日期" DESC
                         LIMIT 10) x
                  ORDER BY x."日成交量" DESC, x."日期" DESC
                 LIMIT 1) AS "日10最大量_日期",
            ( SELECT x."日期"
                   FROM ( SELECT r2."日期",
                            r2."日成交量"
                           FROM core.futures_report_daily_raw r2
                          WHERE ((r2.futures_id = r.futures_id) AND (r2."日期" < r."日期"))
                          ORDER BY r2."日期" DESC
                         LIMIT 20) x
                  ORDER BY x."日成交量" DESC, x."日期" DESC
                 LIMIT 1) AS "日20最大量_日期",
            ( SELECT x."日期"
                   FROM ( SELECT r2."日期",
                            r2."日成交量"
                           FROM core.futures_report_daily_raw r2
                          WHERE ((r2.futures_id = r.futures_id) AND (r2."日期" < r."日期"))
                          ORDER BY r2."日期" DESC
                         LIMIT 60) x
                  ORDER BY x."日成交量" DESC, x."日期" DESC
                 LIMIT 1) AS "日60最大量_日期",
            lag(r."日5MA") OVER (PARTITION BY r.futures_id ORDER BY r."日期") AS "昨日日5MA",
            lag(r."日10MA") OVER (PARTITION BY r.futures_id ORDER BY r."日期") AS "昨日日10MA",
            lag(r."日20MA") OVER (PARTITION BY r.futures_id ORDER BY r."日期") AS "昨日日20MA",
            lag(r."日盤收", 5) OVER (PARTITION BY r.futures_id ORDER BY r."日期") AS "日5_明日扣抵值",
            lag(r."日盤收", 10) OVER (PARTITION BY r.futures_id ORDER BY r."日期") AS "日10_明日扣抵值",
            lag(r."日盤收", 20) OVER (PARTITION BY r.futures_id ORDER BY r."日期") AS "日20_明日扣抵值",
            lag(r."日盤收", 60) OVER (PARTITION BY r.futures_id ORDER BY r."日期") AS "日60_明日扣抵值",
            to_char((to_timestamp((r.created_at)::double precision) AT TIME ZONE 'Asia/Taipei'::text), 'YYYY-MM-DD HH24:MI:SS'::text) AS "建立時間"
           FROM core.futures_report_daily_raw r
          WHERE (r.futures_id = 'TX'::text)
        ), full_day_base AS (
         SELECT b.id,
            b.futures_id,
            b."日期",
            b."年",
            b."月",
            b."星期",
            b."週次",
            b."天別",
            b."日盤開",
            b."日盤收",
            b."日盤高",
            b."日盤低",
            b."日漲跌",
            b."日振幅",
            b."日振幅開盤比",
            b."日振幅昨收比",
            b."日收減日開",
            b."日收開昨收比",
            b."夜盤開",
            b."夜盤收",
            b."夜盤高",
            b."夜盤低",
            b."日法人多倉",
            b."日法人空倉",
            b."夜法人多單量",
            b."夜法人空單量",
            b."call_月選",
            b."put_月選",
            b."call_週三",
            b."put_週三",
            b."call_週五",
            b."put_週五",
            b."日5MA",
            b."日10MA",
            b."日20MA",
            b."日60MA",
            b."日120MA",
            b."夜5MA",
            b."夜10MA",
            b."夜20MA",
            b."夜60MA",
            b."夜120MA",
            b."日成交量",
            b."夜成交量",
            b."日當日價量",
            b."日5日價量",
            b."日10日價量",
            b."日20日價量",
            b."日60日價量",
            b."日布林標準差",
            b."日布林上軌",
            b."日布林下軌",
            b."昨日收盤",
            b."昨日成交量",
            b."日5均量",
            b."日10均量",
            b."日20均量",
            b."日60均量",
            b."日5最大量",
            b."日10最大量",
            b."日20最大量",
            b."日60最大量",
            b."日5最大量_日期",
            b."日10最大量_日期",
            b."日20最大量_日期",
            b."日60最大量_日期",
            b."昨日日5MA",
            b."昨日日10MA",
            b."昨日日20MA",
            b."日5_明日扣抵值",
            b."日10_明日扣抵值",
            b."日20_明日扣抵值",
            b."日60_明日扣抵值",
            b."建立時間",
            lag(b."夜盤開") OVER (PARTITION BY b.futures_id ORDER BY b."日期") AS "前夜盤開",
            lag(b."夜盤高") OVER (PARTITION BY b.futures_id ORDER BY b."日期") AS "前夜盤高",
            lag(b."夜盤低") OVER (PARTITION BY b.futures_id ORDER BY b."日期") AS "前夜盤低",
            s."全日收"
           FROM (base b
             LEFT JOIN settlement_calc s ON (((s."日期" = b."日期") AND (s.futures_id = b.futures_id))))
        ), full_day_calc AS (
         SELECT f.id,
            f.futures_id,
            f."日期",
            f."年",
            f."月",
            f."星期",
            f."週次",
            f."天別",
            f."日盤開",
            f."日盤收",
            f."日盤高",
            f."日盤低",
            f."日漲跌",
            f."日振幅",
            f."日振幅開盤比",
            f."日振幅昨收比",
            f."日收減日開",
            f."日收開昨收比",
            f."夜盤開",
            f."夜盤收",
            f."夜盤高",
            f."夜盤低",
            f."日法人多倉",
            f."日法人空倉",
            f."夜法人多單量",
            f."夜法人空單量",
            f."call_月選",
            f."put_月選",
            f."call_週三",
            f."put_週三",
            f."call_週五",
            f."put_週五",
            f."日5MA",
            f."日10MA",
            f."日20MA",
            f."日60MA",
            f."日120MA",
            f."夜5MA",
            f."夜10MA",
            f."夜20MA",
            f."夜60MA",
            f."夜120MA",
            f."日成交量",
            f."夜成交量",
            f."日當日價量",
            f."日5日價量",
            f."日10日價量",
            f."日20日價量",
            f."日60日價量",
            f."日布林標準差",
            f."日布林上軌",
            f."日布林下軌",
            f."昨日收盤",
            f."昨日成交量",
            f."日5均量",
            f."日10均量",
            f."日20均量",
            f."日60均量",
            f."日5最大量",
            f."日10最大量",
            f."日20最大量",
            f."日60最大量",
            f."日5最大量_日期",
            f."日10最大量_日期",
            f."日20最大量_日期",
            f."日60最大量_日期",
            f."昨日日5MA",
            f."昨日日10MA",
            f."昨日日20MA",
            f."日5_明日扣抵值",
            f."日10_明日扣抵值",
            f."日20_明日扣抵值",
            f."日60_明日扣抵值",
            f."建立時間",
            f."前夜盤開",
            f."前夜盤高",
            f."前夜盤低",
            f."全日收",
            f."前夜盤開" AS "全日開",
                CASE
                    WHEN (f."前夜盤高" IS NULL) THEN f."日盤高"
                    WHEN (f."日盤高" IS NULL) THEN f."前夜盤高"
                    WHEN (f."前夜盤高" >= f."日盤高") THEN f."前夜盤高"
                    ELSE f."日盤高"
                END AS "全日高",
                CASE
                    WHEN (f."前夜盤低" IS NULL) THEN f."日盤低"
                    WHEN (f."日盤低" IS NULL) THEN f."前夜盤低"
                    WHEN (f."前夜盤低" <= f."日盤低") THEN f."前夜盤低"
                    ELSE f."日盤低"
                END AS "全日低",
                CASE
                    WHEN (count(f."全日收") OVER (PARTITION BY f.futures_id ORDER BY f."日期" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) = 5) THEN avg(f."全日收") OVER (PARTITION BY f.futures_id ORDER BY f."日期" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "全5MA",
                CASE
                    WHEN (count(f."全日收") OVER (PARTITION BY f.futures_id ORDER BY f."日期" ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) = 10) THEN avg(f."全日收") OVER (PARTITION BY f.futures_id ORDER BY f."日期" ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "全10MA",
                CASE
                    WHEN (count(f."全日收") OVER (PARTITION BY f.futures_id ORDER BY f."日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) = 20) THEN avg(f."全日收") OVER (PARTITION BY f.futures_id ORDER BY f."日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "全20MA",
                CASE
                    WHEN (count(f."全日收") OVER (PARTITION BY f.futures_id ORDER BY f."日期" ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) = 60) THEN avg(f."全日收") OVER (PARTITION BY f.futures_id ORDER BY f."日期" ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "全60MA",
                CASE
                    WHEN (count(f."全日收") OVER (PARTITION BY f.futures_id ORDER BY f."日期" ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) = 120) THEN avg(f."全日收") OVER (PARTITION BY f.futures_id ORDER BY f."日期" ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "全120MA"
           FROM full_day_base f
        ), calc AS (
         SELECT b.id,
            b.futures_id,
            b."日期",
            b."年",
            b."月",
            b."星期",
            b."週次",
            b."天別",
            b."日盤開",
            b."日盤收",
            b."日盤高",
            b."日盤低",
            b."日漲跌",
            b."日振幅",
            b."日振幅開盤比",
            b."日振幅昨收比",
            b."日收減日開",
            b."日收開昨收比",
            b."夜盤開",
            b."夜盤收",
            b."夜盤高",
            b."夜盤低",
            b."日法人多倉",
            b."日法人空倉",
            b."夜法人多單量",
            b."夜法人空單量",
            b."call_月選",
            b."put_月選",
            b."call_週三",
            b."put_週三",
            b."call_週五",
            b."put_週五",
            b."日5MA",
            b."日10MA",
            b."日20MA",
            b."日60MA",
            b."日120MA",
            b."夜5MA",
            b."夜10MA",
            b."夜20MA",
            b."夜60MA",
            b."夜120MA",
            b."日成交量",
            b."夜成交量",
            b."日當日價量",
            b."日5日價量",
            b."日10日價量",
            b."日20日價量",
            b."日60日價量",
            b."日布林標準差",
            b."日布林上軌",
            b."日布林下軌",
            b."昨日收盤",
            b."昨日成交量",
            b."日5均量",
            b."日10均量",
            b."日20均量",
            b."日60均量",
            b."日5最大量",
            b."日10最大量",
            b."日20最大量",
            b."日60最大量",
            b."日5最大量_日期",
            b."日10最大量_日期",
            b."日20最大量_日期",
            b."日60最大量_日期",
            b."昨日日5MA",
            b."昨日日10MA",
            b."昨日日20MA",
            b."日5_明日扣抵值",
            b."日10_明日扣抵值",
            b."日20_明日扣抵值",
            b."日60_明日扣抵值",
            b."建立時間",
            b."前夜盤開",
            b."前夜盤高",
            b."前夜盤低",
            b."全日收",
            b."全日開",
            b."全日高",
            b."全日低",
            b."全5MA",
            b."全10MA",
            b."全20MA",
            b."全60MA",
            b."全120MA",
                CASE
                    WHEN ((b."日盤收" - b."昨日收盤") > (0)::double precision) THEN '價漲'::text
                    WHEN ((b."日盤收" - b."昨日收盤") < (0)::double precision) THEN '價跌'::text
                    WHEN ((b."日盤收" - b."昨日收盤") = (0)::double precision) THEN '價同'::text
                    ELSE NULL::text
                END AS "日價漲跌",
                CASE
                    WHEN ((b."昨日收盤" IS NULL) OR (b."昨日收盤" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((b."日盤收" - b."昨日收盤") / b."昨日收盤")
                END AS "日價漲跌_pct",
                CASE
                    WHEN ((b."日成交量" - b."昨日成交量") > (0)::double precision) THEN '量增'::text
                    WHEN ((b."日成交量" - b."昨日成交量") < (0)::double precision) THEN '量縮'::text
                    WHEN ((b."日成交量" - b."昨日成交量") = (0)::double precision) THEN '量同'::text
                    ELSE NULL::text
                END AS "日量增減_昨日",
                CASE
                    WHEN ((b."昨日成交量" IS NULL) OR (b."昨日成交量" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((b."日成交量" - b."昨日成交量") / b."昨日成交量")
                END AS "日量增減_昨日_pct",
                CASE
                    WHEN ((b."日成交量" - b."日5均量") > (0)::double precision) THEN '量增'::text
                    WHEN ((b."日成交量" - b."日5均量") < (0)::double precision) THEN '量縮'::text
                    WHEN ((b."日成交量" - b."日5均量") = (0)::double precision) THEN '量同'::text
                    ELSE NULL::text
                END AS "日量增減_五日",
                CASE
                    WHEN ((b."日5均量" IS NULL) OR (b."日5均量" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((b."日成交量" - b."日5均量") / b."日5均量")
                END AS "日量增減_五日_pct",
                CASE
                    WHEN ((b."日10均量" IS NULL) OR (b."日10均量" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((b."日成交量" - b."日10均量") / b."日10均量")
                END AS "日量增減_十日_pct",
                CASE
                    WHEN ((b."日20均量" IS NULL) OR (b."日20均量" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((b."日成交量" - b."日20均量") / b."日20均量")
                END AS "日量增減_二十日_pct",
                CASE
                    WHEN ((b."日60均量" IS NULL) OR (b."日60均量" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((b."日成交量" - b."日60均量") / b."日60均量")
                END AS "日量增減_六十日_pct",
                CASE
                    WHEN ((b."日5MA" IS NULL) OR (b."日5MA" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((b."日盤收" - b."日5MA") / b."日5MA")
                END AS "日5MA乖離率",
                CASE
                    WHEN ((b."日10MA" IS NULL) OR (b."日10MA" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((b."日盤收" - b."日10MA") / b."日10MA")
                END AS "日10MA乖離率",
                CASE
                    WHEN ((b."日20MA" IS NULL) OR (b."日20MA" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((b."日盤收" - b."日20MA") / b."日20MA")
                END AS "日20MA乖離率",
                CASE
                    WHEN ((b."日60MA" IS NULL) OR (b."日60MA" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((b."日盤收" - b."日60MA") / b."日60MA")
                END AS "日60MA乖離率",
                CASE
                    WHEN ((b."日5MA" > b."昨日日5MA") AND (b."日10MA" > b."昨日日10MA") AND (b."日20MA" > b."昨日日20MA")) THEN '多'::text
                    WHEN ((b."日5MA" > b."昨日日5MA") AND (b."日10MA" > b."昨日日10MA") AND (b."日20MA" < b."昨日日20MA")) THEN '多'::text
                    WHEN ((b."日5MA" > b."昨日日5MA") AND (b."日10MA" < b."昨日日10MA") AND (b."日20MA" > b."昨日日20MA")) THEN '盤整'::text
                    WHEN ((b."日5MA" > b."昨日日5MA") AND (b."日10MA" < b."昨日日10MA") AND (b."日20MA" < b."昨日日20MA")) THEN '空'::text
                    WHEN ((b."日5MA" < b."昨日日5MA") AND (b."日10MA" > b."昨日日10MA") AND (b."日20MA" > b."昨日日20MA")) THEN '多'::text
                    WHEN ((b."日5MA" < b."昨日日5MA") AND (b."日10MA" > b."昨日日10MA") AND (b."日20MA" < b."昨日日20MA")) THEN '盤整'::text
                    WHEN ((b."日5MA" < b."昨日日5MA") AND (b."日10MA" < b."昨日日10MA") AND (b."日20MA" > b."昨日日20MA")) THEN '空'::text
                    WHEN ((b."日5MA" < b."昨日日5MA") AND (b."日10MA" < b."昨日日10MA") AND (b."日20MA" < b."昨日日20MA")) THEN '空'::text
                    ELSE NULL::text
                END AS "日趨勢",
            (b."日5MA" - b."昨日日5MA") AS "日5均上升幅度_點",
            (b."日10MA" - b."昨日日10MA") AS "日10均上升幅度_點",
            (b."日20MA" - b."昨日日20MA") AS "日20均上升幅度_點",
            (b."日60MA" - lag(b."日60MA") OVER (PARTITION BY b.futures_id ORDER BY b."日期")) AS "日60均上升幅度_點",
            ((b."日5MA" - b."昨日日5MA") / NULLIF(b."昨日日5MA", (0)::double precision)) AS "日5均上升幅度_pct",
            ((b."日10MA" - b."昨日日10MA") / NULLIF(b."昨日日10MA", (0)::double precision)) AS "日10均上升幅度_pct",
            ((b."日20MA" - b."昨日日20MA") / NULLIF(b."昨日日20MA", (0)::double precision)) AS "日20均上升幅度_pct",
            ((b."日60MA" - lag(b."日60MA") OVER (PARTITION BY b.futures_id ORDER BY b."日期")) / NULLIF(lag(b."日60MA") OVER (PARTITION BY b.futures_id ORDER BY b."日期"), (0)::double precision)) AS "日60均上升幅度_pct",
            (b."日盤收" - b."日5_明日扣抵值") AS "日5_明日扣抵值影響_點",
            (b."日盤收" - b."日10_明日扣抵值") AS "日10_明日扣抵值影響_點",
            (b."日盤收" - b."日20_明日扣抵值") AS "日20_明日扣抵值影響_點",
            (b."日盤收" - b."日60_明日扣抵值") AS "日60_明日扣抵值影響_點",
            ((b."日盤收" - b."日5_明日扣抵值") / NULLIF(b."日盤收", (0)::double precision)) AS "日5_明日扣抵值影響_pct",
            ((b."日盤收" - b."日10_明日扣抵值") / NULLIF(b."日盤收", (0)::double precision)) AS "日10_明日扣抵值影響_pct",
            ((b."日盤收" - b."日20_明日扣抵值") / NULLIF(b."日盤收", (0)::double precision)) AS "日20_明日扣抵值影響_pct",
            ((b."日盤收" - b."日60_明日扣抵值") / NULLIF(b."日盤收", (0)::double precision)) AS "日60_明日扣抵值影響_pct"
           FROM full_day_calc b
        ), gap_calc AS (
         SELECT c.id,
            c.futures_id,
            c."日期",
            c."年",
            c."月",
            c."星期",
            c."週次",
            c."天別",
            c."日盤開",
            c."日盤收",
            c."日盤高",
            c."日盤低",
            c."日漲跌",
            c."日振幅",
            c."日振幅開盤比",
            c."日振幅昨收比",
            c."日收減日開",
            c."日收開昨收比",
            c."夜盤開",
            c."夜盤收",
            c."夜盤高",
            c."夜盤低",
            c."日法人多倉",
            c."日法人空倉",
            c."夜法人多單量",
            c."夜法人空單量",
            c."call_月選",
            c."put_月選",
            c."call_週三",
            c."put_週三",
            c."call_週五",
            c."put_週五",
            c."日5MA",
            c."日10MA",
            c."日20MA",
            c."日60MA",
            c."日120MA",
            c."夜5MA",
            c."夜10MA",
            c."夜20MA",
            c."夜60MA",
            c."夜120MA",
            c."日成交量",
            c."夜成交量",
            c."日當日價量",
            c."日5日價量",
            c."日10日價量",
            c."日20日價量",
            c."日60日價量",
            c."日布林標準差",
            c."日布林上軌",
            c."日布林下軌",
            c."昨日收盤",
            c."昨日成交量",
            c."日5均量",
            c."日10均量",
            c."日20均量",
            c."日60均量",
            c."日5最大量",
            c."日10最大量",
            c."日20最大量",
            c."日60最大量",
            c."日5最大量_日期",
            c."日10最大量_日期",
            c."日20最大量_日期",
            c."日60最大量_日期",
            c."昨日日5MA",
            c."昨日日10MA",
            c."昨日日20MA",
            c."日5_明日扣抵值",
            c."日10_明日扣抵值",
            c."日20_明日扣抵值",
            c."日60_明日扣抵值",
            c."建立時間",
            c."前夜盤開",
            c."前夜盤高",
            c."前夜盤低",
            c."全日收",
            c."全日開",
            c."全日高",
            c."全日低",
            c."全5MA",
            c."全10MA",
            c."全20MA",
            c."全60MA",
            c."全120MA",
            c."日價漲跌",
            c."日價漲跌_pct",
            c."日量增減_昨日",
            c."日量增減_昨日_pct",
            c."日量增減_五日",
            c."日量增減_五日_pct",
            c."日量增減_十日_pct",
            c."日量增減_二十日_pct",
            c."日量增減_六十日_pct",
            c."日5MA乖離率",
            c."日10MA乖離率",
            c."日20MA乖離率",
            c."日60MA乖離率",
            c."日趨勢",
            c."日5均上升幅度_點",
            c."日10均上升幅度_點",
            c."日20均上升幅度_點",
            c."日60均上升幅度_點",
            c."日5均上升幅度_pct",
            c."日10均上升幅度_pct",
            c."日20均上升幅度_pct",
            c."日60均上升幅度_pct",
            c."日5_明日扣抵值影響_點",
            c."日10_明日扣抵值影響_點",
            c."日20_明日扣抵值影響_點",
            c."日60_明日扣抵值影響_點",
            c."日5_明日扣抵值影響_pct",
            c."日10_明日扣抵值影響_pct",
            c."日20_明日扣抵值影響_pct",
            c."日60_明日扣抵值影響_pct",
                CASE
                    WHEN (c."日盤低" > lag(c."日盤高") OVER (PARTITION BY c.futures_id ORDER BY c."日期")) THEN '上跳空'::text
                    WHEN (c."日盤高" < lag(c."日盤低") OVER (PARTITION BY c.futures_id ORDER BY c."日期")) THEN '下跳空'::text
                    ELSE '無跳空'::text
                END AS "跳空狀態",
                CASE
                    WHEN (c."日盤收" > c."日盤開") THEN c."日盤收"
                    ELSE c."日盤開"
                END AS "今上緣",
                CASE
                    WHEN (c."日盤收" > c."日盤開") THEN c."日盤開"
                    ELSE c."日盤收"
                END AS "今下緣"
           FROM calc c
        ), gap_calc2 AS (
         SELECT g.id,
            g.futures_id,
            g."日期",
            g."年",
            g."月",
            g."星期",
            g."週次",
            g."天別",
            g."日盤開",
            g."日盤收",
            g."日盤高",
            g."日盤低",
            g."日漲跌",
            g."日振幅",
            g."日振幅開盤比",
            g."日振幅昨收比",
            g."日收減日開",
            g."日收開昨收比",
            g."夜盤開",
            g."夜盤收",
            g."夜盤高",
            g."夜盤低",
            g."日法人多倉",
            g."日法人空倉",
            g."夜法人多單量",
            g."夜法人空單量",
            g."call_月選",
            g."put_月選",
            g."call_週三",
            g."put_週三",
            g."call_週五",
            g."put_週五",
            g."日5MA",
            g."日10MA",
            g."日20MA",
            g."日60MA",
            g."日120MA",
            g."夜5MA",
            g."夜10MA",
            g."夜20MA",
            g."夜60MA",
            g."夜120MA",
            g."日成交量",
            g."夜成交量",
            g."日當日價量",
            g."日5日價量",
            g."日10日價量",
            g."日20日價量",
            g."日60日價量",
            g."日布林標準差",
            g."日布林上軌",
            g."日布林下軌",
            g."昨日收盤",
            g."昨日成交量",
            g."日5均量",
            g."日10均量",
            g."日20均量",
            g."日60均量",
            g."日5最大量",
            g."日10最大量",
            g."日20最大量",
            g."日60最大量",
            g."日5最大量_日期",
            g."日10最大量_日期",
            g."日20最大量_日期",
            g."日60最大量_日期",
            g."昨日日5MA",
            g."昨日日10MA",
            g."昨日日20MA",
            g."日5_明日扣抵值",
            g."日10_明日扣抵值",
            g."日20_明日扣抵值",
            g."日60_明日扣抵值",
            g."建立時間",
            g."前夜盤開",
            g."前夜盤高",
            g."前夜盤低",
            g."全日收",
            g."全日開",
            g."全日高",
            g."全日低",
            g."全5MA",
            g."全10MA",
            g."全20MA",
            g."全60MA",
            g."全120MA",
            g."日價漲跌",
            g."日價漲跌_pct",
            g."日量增減_昨日",
            g."日量增減_昨日_pct",
            g."日量增減_五日",
            g."日量增減_五日_pct",
            g."日量增減_十日_pct",
            g."日量增減_二十日_pct",
            g."日量增減_六十日_pct",
            g."日5MA乖離率",
            g."日10MA乖離率",
            g."日20MA乖離率",
            g."日60MA乖離率",
            g."日趨勢",
            g."日5均上升幅度_點",
            g."日10均上升幅度_點",
            g."日20均上升幅度_點",
            g."日60均上升幅度_點",
            g."日5均上升幅度_pct",
            g."日10均上升幅度_pct",
            g."日20均上升幅度_pct",
            g."日60均上升幅度_pct",
            g."日5_明日扣抵值影響_點",
            g."日10_明日扣抵值影響_點",
            g."日20_明日扣抵值影響_點",
            g."日60_明日扣抵值影響_點",
            g."日5_明日扣抵值影響_pct",
            g."日10_明日扣抵值影響_pct",
            g."日20_明日扣抵值影響_pct",
            g."日60_明日扣抵值影響_pct",
            g."跳空狀態",
            g."今上緣",
            g."今下緣",
            lag(g."今上緣") OVER (PARTITION BY g.futures_id ORDER BY g."日期") AS "昨上緣",
            lag(g."今下緣") OVER (PARTITION BY g.futures_id ORDER BY g."日期") AS "昨下緣"
           FROM gap_calc g
        ), pos_calc AS (
         SELECT c.id,
            c.futures_id,
            c."日期",
            c."年",
            c."月",
            c."星期",
            c."週次",
            c."天別",
            c."日盤開",
            c."日盤收",
            c."日盤高",
            c."日盤低",
            c."日漲跌",
            c."日振幅",
            c."日振幅開盤比",
            c."日振幅昨收比",
            c."日收減日開",
            c."日收開昨收比",
            c."夜盤開",
            c."夜盤收",
            c."夜盤高",
            c."夜盤低",
            c."日法人多倉",
            c."日法人空倉",
            c."夜法人多單量",
            c."夜法人空單量",
            c."call_月選",
            c."put_月選",
            c."call_週三",
            c."put_週三",
            c."call_週五",
            c."put_週五",
            c."日5MA",
            c."日10MA",
            c."日20MA",
            c."日60MA",
            c."日120MA",
            c."夜5MA",
            c."夜10MA",
            c."夜20MA",
            c."夜60MA",
            c."夜120MA",
            c."日成交量",
            c."夜成交量",
            c."日當日價量",
            c."日5日價量",
            c."日10日價量",
            c."日20日價量",
            c."日60日價量",
            c."日布林標準差",
            c."日布林上軌",
            c."日布林下軌",
            c."昨日收盤",
            c."昨日成交量",
            c."日5均量",
            c."日10均量",
            c."日20均量",
            c."日60均量",
            c."日5最大量",
            c."日10最大量",
            c."日20最大量",
            c."日60最大量",
            c."日5最大量_日期",
            c."日10最大量_日期",
            c."日20最大量_日期",
            c."日60最大量_日期",
            c."昨日日5MA",
            c."昨日日10MA",
            c."昨日日20MA",
            c."日5_明日扣抵值",
            c."日10_明日扣抵值",
            c."日20_明日扣抵值",
            c."日60_明日扣抵值",
            c."建立時間",
            c."前夜盤開",
            c."前夜盤高",
            c."前夜盤低",
            c."全日收",
            c."全日開",
            c."全日高",
            c."全日低",
            c."全5MA",
            c."全10MA",
            c."全20MA",
            c."全60MA",
            c."全120MA",
            c."日價漲跌",
            c."日價漲跌_pct",
            c."日量增減_昨日",
            c."日量增減_昨日_pct",
            c."日量增減_五日",
            c."日量增減_五日_pct",
            c."日量增減_十日_pct",
            c."日量增減_二十日_pct",
            c."日量增減_六十日_pct",
            c."日5MA乖離率",
            c."日10MA乖離率",
            c."日20MA乖離率",
            c."日60MA乖離率",
            c."日趨勢",
            c."日5均上升幅度_點",
            c."日10均上升幅度_點",
            c."日20均上升幅度_點",
            c."日60均上升幅度_點",
            c."日5均上升幅度_pct",
            c."日10均上升幅度_pct",
            c."日20均上升幅度_pct",
            c."日60均上升幅度_pct",
            c."日5_明日扣抵值影響_點",
            c."日10_明日扣抵值影響_點",
            c."日20_明日扣抵值影響_點",
            c."日60_明日扣抵值影響_點",
            c."日5_明日扣抵值影響_pct",
            c."日10_明日扣抵值影響_pct",
            c."日20_明日扣抵值影響_pct",
            c."日60_明日扣抵值影響_pct",
            c."跳空狀態",
            c."今上緣",
            c."今下緣",
            c."昨上緣",
            c."昨下緣",
                CASE
                    WHEN ((c."日趨勢" = '多'::text) AND (c."日5MA乖離率" IS NOT NULL) AND (c."日5MA乖離率" > (0.02)::double precision)) THEN '高檔'::text
                    WHEN ((c."日趨勢" = '多'::text) AND (c."日5MA乖離率" IS NOT NULL) AND (c."日5MA乖離率" < (0.02)::double precision)) THEN '低檔'::text
                    WHEN ((c."日趨勢" = '空'::text) AND (c."日5MA乖離率" IS NOT NULL) AND (c."日5MA乖離率" > ('-0.02'::numeric)::double precision)) THEN '高檔'::text
                    WHEN ((c."日趨勢" = '空'::text) AND (c."日5MA乖離率" IS NOT NULL) AND (c."日5MA乖離率" < ('-0.02'::numeric)::double precision)) THEN '低檔'::text
                    WHEN ((c."日趨勢" = '盤整'::text) AND (c."日布林上軌" IS NOT NULL) AND (c."日布林下軌" IS NOT NULL) AND (c."日盤收" >= (c."日布林上軌" - ((c."日布林上軌" - c."日布林下軌") * (0.1)::double precision)))) THEN '高檔'::text
                    WHEN ((c."日趨勢" = '盤整'::text) AND (c."日布林上軌" IS NOT NULL) AND (c."日布林下軌" IS NOT NULL) AND (c."日盤收" <= (c."日布林下軌" + ((c."日布林上軌" - c."日布林下軌") * (0.1)::double precision)))) THEN '低檔'::text
                    ELSE '中間'::text
                END AS "日位置"
           FROM gap_calc2 c
        ), open_interest_rank AS (
         SELECT f.date AS "日期",
            f.futures_id,
            f.contract_date,
            f.open_interest,
            row_number() OVER (PARTITION BY f.date, f.futures_id ORDER BY f.contract_date) AS rn
           FROM raw_finmind.fm_taiwan_futures_daily f
          WHERE ((f.futures_id = 'TX'::text) AND (f.trading_session = 'position'::text) AND (length(f.contract_date) = 6))
        ), open_interest_calc AS (
         SELECT open_interest_rank."日期",
            open_interest_rank.futures_id,
            sum(open_interest_rank.open_interest) AS "未沖銷_全",
            max(
                CASE
                    WHEN (open_interest_rank.rn = 1) THEN open_interest_rank.open_interest
                    ELSE NULL::bigint
                END) AS "未沖銷_近期",
            max(
                CASE
                    WHEN (open_interest_rank.rn = 2) THEN open_interest_rank.open_interest
                    ELSE NULL::bigint
                END) AS "未沖銷_遠期"
           FROM open_interest_rank
          GROUP BY open_interest_rank."日期", open_interest_rank.futures_id
        ), small_micro_total_oi AS (
         SELECT f.date AS "日期",
            f.futures_id,
            sum(COALESCE(f.open_interest, (0)::bigint)) AS "全市場OI"
           FROM raw_finmind.fm_taiwan_futures_daily f
          WHERE ((f.futures_id = ANY (ARRAY['MTX'::text, 'TMF'::text])) AND (f.trading_session = 'position'::text) AND (f.contract_date !~~ '%/%'::text) AND (f.contract_date !~~ '%W%'::text) AND (length(f.contract_date) = 6))
          GROUP BY f.date, f.futures_id
        ), small_micro_inst AS (
         SELECT i.date AS "日期",
            i.data_id AS futures_id,
            sum(COALESCE(i.long_open_interest_balance_volume, (0)::bigint)) AS "三大法人多單",
            sum(COALESCE(i.short_open_interest_balance_volume, (0)::bigint)) AS "三大法人空單"
           FROM raw_finmind.fm_taiwan_futures_institutional_investors i
          WHERE (i.data_id = ANY (ARRAY['MTX'::text, 'TMF'::text]))
          GROUP BY i.date, i.data_id
        ), small_micro_retail AS (
         SELECT oi."日期",
            oi.futures_id,
            oi."全市場OI",
            inst."三大法人多單",
            inst."三大法人空單",
            (oi."全市場OI" - inst."三大法人多單") AS "散戶多單",
            (oi."全市場OI" - inst."三大法人空單") AS "散戶空單",
            (((inst."三大法人空單" - inst."三大法人多單") * 100.0) / NULLIF(oi."全市場OI", (0)::numeric)) AS "散戶多空比"
           FROM (small_micro_total_oi oi
             LEFT JOIN small_micro_inst inst ON (((inst."日期" = oi."日期") AND (inst.futures_id = oi.futures_id))))
        ), taiex_chip AS (
         SELECT stock_report_daily."日期",
            stock_report_daily."法人總買超_億"
           FROM core.stock_report_daily
          WHERE (stock_report_daily."股票代號" = 'TAIEX'::text)
        ), report_base AS (
         SELECT c.id,
            c.futures_id,
            c."日期",
            c."年",
            c."月",
            c."星期",
            c."週次",
            c."天別",
            round((c."日盤開")::numeric, 2) AS "日盤開",
            round((c."日盤收")::numeric, 2) AS "日盤收",
            round((c."日盤高")::numeric, 2) AS "日盤高",
            round((c."日盤低")::numeric, 2) AS "日盤低",
            round((c."昨日收盤")::numeric, 2) AS "昨日收盤",
            round((c."日漲跌")::numeric, 2) AS "日漲跌",
            round((c."日價漲跌_pct")::numeric, 4) AS "日價漲跌_pct",
            round(((c."日盤收" - c."日盤開"))::numeric, 2) AS "日收減日開",
            round((((c."日盤收" - c."日盤開") / NULLIF(c."昨日收盤", (0)::double precision)))::numeric, 4) AS "日收開_昨收_pct",
            round((((c."日盤收" - c."日盤開") / NULLIF(c."日盤開", (0)::double precision)))::numeric, 4) AS "日收開_開盤_pct",
            round(((c."日盤高" - c."日盤低"))::numeric, 2) AS "日振幅",
            round((((c."日盤高" - c."日盤低") / NULLIF(c."昨日收盤", (0)::double precision)))::numeric, 4) AS "日振幅_昨收_pct",
            round((((c."日盤高" - c."日盤低") / NULLIF(c."日盤開", (0)::double precision)))::numeric, 4) AS "日振幅_開盤_pct",
            round((c."日成交量")::numeric, 2) AS "日成交量",
            round((c."昨日成交量")::numeric, 2) AS "昨日成交量",
            c."日5最大量_日期",
            round((c."日5最大量")::numeric, 2) AS "日5最大量",
            c."日10最大量_日期",
            round((c."日10最大量")::numeric, 2) AS "日10最大量",
            c."日20最大量_日期",
            round((c."日20最大量")::numeric, 2) AS "日20最大量",
            c."日60最大量_日期",
            round((c."日60最大量")::numeric, 2) AS "日60最大量",
            round((c."日5均量")::numeric, 2) AS "日5均量",
            round((c."日10均量")::numeric, 2) AS "日10均量",
            round((c."日20均量")::numeric, 2) AS "日20均量",
            round((c."日60均量")::numeric, 2) AS "日60均量",
            round((c."日量增減_昨日_pct")::numeric, 4) AS "日量增減_昨日_pct",
            round((c."日量增減_五日_pct")::numeric, 4) AS "日量增減_五日_pct",
            round((c."日量增減_十日_pct")::numeric, 4) AS "日量增減_十日_pct",
            round((c."日量增減_二十日_pct")::numeric, 4) AS "日量增減_二十日_pct",
            round((c."日量增減_六十日_pct")::numeric, 4) AS "日量增減_六十日_pct",
            c."日當日價量",
            c."日5日價量",
            c."日10日價量",
            c."日20日價量",
            c."日60日價量",
                CASE
                    WHEN (c."跳空狀態" = '上跳空'::text) THEN (c."今下緣" - c."昨上緣")
                    WHEN (c."跳空狀態" = '下跳空'::text) THEN (c."今上緣" - c."昨下緣")
                    ELSE NULL::double precision
                END AS "跳空缺口",
                CASE
                    WHEN ((c."昨日收盤" IS NULL) OR (c."昨日收盤" = (0)::double precision)) THEN NULL::double precision
                    ELSE (
                    CASE
                        WHEN (c."跳空狀態" = '上跳空'::text) THEN (c."今下緣" - c."昨上緣")
                        WHEN (c."跳空狀態" = '下跳空'::text) THEN (c."今上緣" - c."昨下緣")
                        ELSE NULL::double precision
                    END / c."昨日收盤")
                END AS "跳空缺口_昨收_pct",
            (abs((c."日盤收" - c."日盤開")) / NULLIF((c."日盤高" - c."日盤低"), (0)::double precision)) AS "實體_pct",
            (abs((c."日盤高" -
                CASE
                    WHEN (c."日盤收" > c."日盤開") THEN c."日盤收"
                    ELSE c."日盤開"
                END)) / NULLIF((c."日盤高" - c."日盤低"), (0)::double precision)) AS "上影_pct",
            (abs((
                CASE
                    WHEN (c."日盤收" > c."日盤開") THEN c."日盤開"
                    ELSE c."日盤收"
                END - c."日盤低")) / NULLIF((c."日盤高" - c."日盤低"), (0)::double precision)) AS "下影_pct",
            round((c."日5MA")::numeric, 2) AS "日5MA",
            round((c."日10MA")::numeric, 2) AS "日10MA",
            round((c."日20MA")::numeric, 2) AS "日20MA",
            round((c."日60MA")::numeric, 2) AS "日60MA",
            round((c."日120MA")::numeric, 2) AS "日120MA",
            round((c."日5均上升幅度_點")::numeric, 2) AS "日5均上升幅度_點",
            round((c."日10均上升幅度_點")::numeric, 2) AS "日10均上升幅度_點",
            round((c."日20均上升幅度_點")::numeric, 2) AS "日20均上升幅度_點",
            round((c."日60均上升幅度_點")::numeric, 2) AS "日60均上升幅度_點",
            round((c."日5均上升幅度_pct")::numeric, 4) AS "日5均上升幅度_pct",
            round((c."日10均上升幅度_pct")::numeric, 4) AS "日10均上升幅度_pct",
            round((c."日20均上升幅度_pct")::numeric, 4) AS "日20均上升幅度_pct",
            round((c."日60均上升幅度_pct")::numeric, 4) AS "日60均上升幅度_pct",
            round((c."日5_明日扣抵值")::numeric, 2) AS "日5_明日扣抵值",
            round((c."日10_明日扣抵值")::numeric, 2) AS "日10_明日扣抵值",
            round((c."日20_明日扣抵值")::numeric, 2) AS "日20_明日扣抵值",
            round((c."日60_明日扣抵值")::numeric, 2) AS "日60_明日扣抵值",
            round((c."日5_明日扣抵值影響_點")::numeric, 2) AS "日5_明日扣抵值影響_點",
            round((c."日10_明日扣抵值影響_點")::numeric, 2) AS "日10_明日扣抵值影響_點",
            round((c."日20_明日扣抵值影響_點")::numeric, 2) AS "日20_明日扣抵值影響_點",
            round((c."日60_明日扣抵值影響_點")::numeric, 2) AS "日60_明日扣抵值影響_點",
            round((c."日5_明日扣抵值影響_pct")::numeric, 4) AS "日5_明日扣抵值影響_pct",
            round((c."日10_明日扣抵值影響_pct")::numeric, 4) AS "日10_明日扣抵值影響_pct",
            round((c."日20_明日扣抵值影響_pct")::numeric, 4) AS "日20_明日扣抵值影響_pct",
            round((c."日60_明日扣抵值影響_pct")::numeric, 4) AS "日60_明日扣抵值影響_pct",
            round((c."日5MA乖離率")::numeric, 4) AS "日5MA乖離率",
            round((c."日10MA乖離率")::numeric, 4) AS "日10MA乖離率",
            round((c."日20MA乖離率")::numeric, 4) AS "日20MA乖離率",
            round((c."日60MA乖離率")::numeric, 4) AS "日60MA乖離率",
            round((c."日布林標準差")::numeric, 2) AS "日布林標準差",
            round((c."日布林上軌")::numeric, 2) AS "日布林上軌",
            round((c."日布林下軌")::numeric, 2) AS "日布林下軌",
            c."日法人多倉",
            c."日法人空倉",
            oi."未沖銷_全",
            oi."未沖銷_近期",
            oi."未沖銷_遠期",
            mf."月結算狀態" AS "主力_月結算狀態",
            round((mf."主力新建多單")::numeric, 2) AS "主力新建多單",
            round((mf."主力新建空單")::numeric, 2) AS "主力新建空單",
            round((mf."主力淨多空和_5日")::numeric, 2) AS "主力淨多空和_5日",
            round((mf."主力淨多空和_10日")::numeric, 2) AS "主力淨多空和_10日",
            round((mf."主力淨多空和_20日")::numeric, 2) AS "主力淨多空和_20日",
            round((mf."主力淨多空和_60日")::numeric, 2) AS "主力淨多空和_60日",
            round((c."夜盤開")::numeric, 2) AS "夜盤開",
            round((c."夜盤收")::numeric, 2) AS "夜盤收",
            round((c."夜盤高")::numeric, 2) AS "夜盤高",
            round((c."夜盤低")::numeric, 2) AS "夜盤低",
            round((c."夜5MA")::numeric, 2) AS "夜5MA",
            round((c."夜10MA")::numeric, 2) AS "夜10MA",
            round((c."夜20MA")::numeric, 2) AS "夜20MA",
            round((c."夜60MA")::numeric, 2) AS "夜60MA",
            round((c."夜120MA")::numeric, 2) AS "夜120MA",
            round((c."夜成交量")::numeric, 2) AS "夜成交量",
            c."夜法人多單量",
            c."夜法人空單量",
            round((c."全日開")::numeric, 2) AS "全日開",
            round((c."全日收")::numeric, 2) AS "全日收",
            round((c."全日高")::numeric, 2) AS "全日高",
            round((c."全日低")::numeric, 2) AS "全日低",
            round((c."全5MA")::numeric, 2) AS "全5MA",
            round((c."全10MA")::numeric, 2) AS "全10MA",
            round((c."全20MA")::numeric, 2) AS "全20MA",
            round((c."全60MA")::numeric, 2) AS "全60MA",
            round((c."全120MA")::numeric, 2) AS "全120MA",
            round(mtx."全市場OI", 2) AS "全市場OI_小台",
            round(mtx."三大法人多單", 2) AS "三大法人多單_小台",
            round(mtx."三大法人空單", 2) AS "三大法人空單_小台",
            round(mtx."散戶多單", 2) AS "散戶多單_小台",
            round(mtx."散戶空單", 2) AS "散戶空單_小台",
            round(mtx."散戶多空比", 2) AS "散戶多空比_小台",
            round(tmf."全市場OI", 2) AS "全市場OI_微台",
            round(tmf."三大法人多單", 2) AS "三大法人多單_微台",
            round(tmf."三大法人空單", 2) AS "三大法人空單_微台",
            round(tmf."散戶多單", 2) AS "散戶多單_微台",
            round(tmf."散戶空單", 2) AS "散戶空單_微台",
            round(tmf."散戶多空比", 2) AS "散戶多空比_微台",
            round((tc."法人總買超_億")::numeric, 2) AS "法人總買超_億",
                CASE
                    WHEN (tc."法人總買超_億" > (0)::double precision) THEN '買超'::text
                    WHEN (tc."法人總買超_億" < (0)::double precision) THEN '賣超'::text
                    ELSE NULL::text
                END AS "法人籌碼",
                CASE
                    WHEN (COALESCE(tmf."散戶多空比", mtx."散戶多空比") > (0)::numeric) THEN '多單'::text
                    WHEN (COALESCE(tmf."散戶多空比", mtx."散戶多空比") < (0)::numeric) THEN '空單'::text
                    ELSE NULL::text
                END AS "散戶籌碼",
            round((c."call_月選")::numeric, 2) AS "call_月選",
            round((c."put_月選")::numeric, 2) AS "put_月選",
            round(((c."call_月選" / NULLIF(c."put_月選", (0)::double precision)))::numeric, 4) AS "買賣權比_月選",
            round((c."call_週三")::numeric, 2) AS "call_週三",
            round((c."put_週三")::numeric, 2) AS "put_週三",
            round(((c."call_週三" / NULLIF(c."put_週三", (0)::double precision)))::numeric, 4) AS "買賣權比_週三",
            round((c."call_週五")::numeric, 2) AS "call_週五",
            round((c."put_週五")::numeric, 2) AS "put_週五",
            round(((c."call_週五" / NULLIF(c."put_週五", (0)::double precision)))::numeric, 4) AS "買賣權比_週五",
            c."日價漲跌",
            c."日量增減_昨日",
            c."日量增減_五日",
                CASE
                    WHEN (c."日5MA" > c."昨日日5MA") THEN '上'::text
                    WHEN (c."日5MA" < c."昨日日5MA") THEN '下'::text
                    ELSE NULL::text
                END AS "日MA方向_5",
                CASE
                    WHEN (c."日10MA" > c."昨日日10MA") THEN '上'::text
                    WHEN (c."日10MA" < c."昨日日10MA") THEN '下'::text
                    ELSE NULL::text
                END AS "日MA方向_10",
                CASE
                    WHEN (c."日20MA" > c."昨日日20MA") THEN '上'::text
                    WHEN (c."日20MA" < c."昨日日20MA") THEN '下'::text
                    ELSE NULL::text
                END AS "日MA方向_20",
            c."日位置",
            c."日趨勢" AS "策略_線型",
                CASE
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_昨日" = '量增'::text) AND (c."日趨勢" = '多'::text) AND (c."日位置" = ANY (ARRAY['低檔'::text, '中間'::text, '高檔'::text]))) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_昨日" = '量增'::text) AND (c."日趨勢" = '盤整'::text) AND (c."日位置" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_昨日" = '量增'::text) AND (c."日趨勢" = '盤整'::text) AND (c."日位置" = '高檔'::text)) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_昨日" = '量增'::text) AND (c."日趨勢" = '空'::text) AND (c."日位置" = ANY (ARRAY['低檔'::text, '中間'::text, '高檔'::text]))) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_昨日" = '量縮'::text) AND (c."日趨勢" = '多'::text) AND (c."日位置" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_昨日" = '量縮'::text) AND (c."日趨勢" = '多'::text) AND (c."日位置" = '高檔'::text)) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_昨日" = '量縮'::text) AND (c."日趨勢" = ANY (ARRAY['盤整'::text, '空'::text]))) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_昨日" = '量增'::text) AND (c."日趨勢" = '多'::text) AND (c."日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_昨日" = '量增'::text) AND (c."日趨勢" = '多'::text) AND (c."日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_昨日" = '量增'::text) AND (c."日趨勢" = '盤整'::text) AND (c."日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_昨日" = '量增'::text) AND (c."日趨勢" = '盤整'::text) AND (c."日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_昨日" = '量增'::text) AND (c."日趨勢" = '空'::text)) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_昨日" = '量縮'::text) AND (c."日趨勢" = '多'::text)) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_昨日" = '量縮'::text) AND (c."日趨勢" = '盤整'::text) AND (c."日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_昨日" = '量縮'::text) AND (c."日趨勢" = '盤整'::text) AND (c."日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_昨日" = '量縮'::text) AND (c."日趨勢" = '空'::text) AND (c."日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_昨日" = '量縮'::text) AND (c."日趨勢" = '空'::text) AND (c."日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    ELSE NULL::text
                END AS "策略_價量昨日",
                CASE
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_五日" = '量增'::text) AND (c."日趨勢" = '多'::text) AND (c."日位置" = ANY (ARRAY['低檔'::text, '中間'::text, '高檔'::text]))) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_五日" = '量增'::text) AND (c."日趨勢" = '盤整'::text) AND (c."日位置" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_五日" = '量增'::text) AND (c."日趨勢" = '盤整'::text) AND (c."日位置" = '高檔'::text)) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_五日" = '量增'::text) AND (c."日趨勢" = '空'::text)) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_五日" = '量縮'::text) AND (c."日趨勢" = '多'::text) AND (c."日位置" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_五日" = '量縮'::text) AND (c."日趨勢" = '多'::text) AND (c."日位置" = '高檔'::text)) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價漲'::text) AND (c."日量增減_五日" = '量縮'::text) AND (c."日趨勢" = ANY (ARRAY['盤整'::text, '空'::text]))) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_五日" = '量增'::text) AND (c."日趨勢" = '多'::text) AND (c."日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_五日" = '量增'::text) AND (c."日趨勢" = '多'::text) AND (c."日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_五日" = '量增'::text) AND (c."日趨勢" = '盤整'::text) AND (c."日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_五日" = '量增'::text) AND (c."日趨勢" = '盤整'::text) AND (c."日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_五日" = '量增'::text) AND (c."日趨勢" = '空'::text)) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_五日" = '量縮'::text) AND (c."日趨勢" = '多'::text)) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_五日" = '量縮'::text) AND (c."日趨勢" = '盤整'::text) AND (c."日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_五日" = '量縮'::text) AND (c."日趨勢" = '盤整'::text) AND (c."日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_五日" = '量縮'::text) AND (c."日趨勢" = '空'::text) AND (c."日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."日價漲跌" = '價跌'::text) AND (c."日量增減_五日" = '量縮'::text) AND (c."日趨勢" = '空'::text) AND (c."日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    ELSE NULL::text
                END AS "策略_價量五日",
                CASE
                    WHEN ((tc."法人總買超_億" > (0)::double precision) AND (COALESCE(tmf."散戶多空比", mtx."散戶多空比") < (0)::numeric)) THEN '多'::text
                    WHEN ((tc."法人總買超_億" < (0)::double precision) AND (COALESCE(tmf."散戶多空比", mtx."散戶多空比") > (0)::numeric)) THEN '空'::text
                    ELSE '無'::text
                END AS "加權籌碼",
                CASE
                    WHEN ((mf."主力新建多單" < (0)::double precision) AND (mf."主力新建空單" < (0)::double precision)) THEN '無'::text
                    WHEN ((mf."主力新建多單" > (0)::double precision) AND (mf."主力新建空單" < (0)::double precision)) THEN '多方'::text
                    WHEN ((mf."主力新建多單" < (0)::double precision) AND (mf."主力新建空單" > (0)::double precision)) THEN '空方'::text
                    WHEN ((mf."主力新建多單" > (0)::double precision) AND (mf."主力新建空單" > (0)::double precision) AND (mf."主力新建多單" > mf."主力新建空單")) THEN '多空雙殺_偏多'::text
                    WHEN ((mf."主力新建多單" > (0)::double precision) AND (mf."主力新建空單" > (0)::double precision) AND (mf."主力新建空單" > mf."主力新建多單")) THEN '多空雙殺_偏空'::text
                    ELSE NULL::text
                END AS "期貨籌碼",
            ((('長'::text ||
                CASE
                    WHEN ((c."call_月選" IS NULL) OR (c."put_月選" IS NULL) OR (c."put_月選" = (0)::double precision)) THEN '?'::text
                    WHEN ((c."call_月選" / c."put_月選") > (1)::double precision) THEN '多'::text
                    WHEN ((c."call_月選" / c."put_月選") < (1)::double precision) THEN '空'::text
                    ELSE '?'::text
                END) || '短'::text) ||
                CASE
                    WHEN (((c."call_週三" IS NULL) OR (c."put_週三" IS NULL) OR (c."put_週三" = (0)::double precision)) AND ((c."call_週五" IS NULL) OR (c."put_週五" IS NULL) OR (c."put_週五" = (0)::double precision))) THEN '?'::text
                    WHEN (((c."call_週三" IS NULL) OR (c."put_週三" IS NULL) OR (c."put_週三" = (0)::double precision)) AND (NOT ((c."call_週五" IS NULL) OR (c."put_週五" IS NULL) OR (c."put_週五" = (0)::double precision)))) THEN
                    CASE
                        WHEN ((c."call_週五" / c."put_週五") > (1)::double precision) THEN '多'::text
                        WHEN ((c."call_週五" / c."put_週五") < (1)::double precision) THEN '空'::text
                        ELSE '?'::text
                    END
                    WHEN ((NOT ((c."call_週三" IS NULL) OR (c."put_週三" IS NULL) OR (c."put_週三" = (0)::double precision))) AND ((c."call_週五" IS NULL) OR (c."put_週五" IS NULL) OR (c."put_週五" = (0)::double precision))) THEN
                    CASE
                        WHEN ((c."call_週三" / c."put_週三") > (1)::double precision) THEN '多'::text
                        WHEN ((c."call_週三" / c."put_週三") < (1)::double precision) THEN '空'::text
                        ELSE '?'::text
                    END
                    WHEN (((c."call_週三" / c."put_週三") > (1)::double precision) AND ((c."call_週五" / c."put_週五") > (1)::double precision)) THEN '多'::text
                    WHEN (((c."call_週三" / c."put_週三") < (1)::double precision) AND ((c."call_週五" / c."put_週五") < (1)::double precision)) THEN '空'::text
                    WHEN ((((c."call_週三" / c."put_週三") > (1)::double precision) AND ((c."call_週五" / c."put_週五") < (1)::double precision)) OR (((c."call_週三" / c."put_週三") < (1)::double precision) AND ((c."call_週五" / c."put_週五") > (1)::double precision))) THEN
                    CASE
                        WHEN (c."星期" = ANY (ARRAY['星期一'::text, '星期二'::text])) THEN
                        CASE
                            WHEN ((c."call_週三" / c."put_週三") > (1)::double precision) THEN '多'::text
                            WHEN ((c."call_週三" / c."put_週三") < (1)::double precision) THEN '空'::text
                            ELSE '?'::text
                        END
                        WHEN (c."星期" = ANY (ARRAY['星期三'::text, '星期四'::text])) THEN
                        CASE
                            WHEN ((c."call_週五" / c."put_週五") > (1)::double precision) THEN '多'::text
                            WHEN ((c."call_週五" / c."put_週五") < (1)::double precision) THEN '空'::text
                            ELSE '?'::text
                        END
                        WHEN (c."星期" = '星期五'::text) THEN
                        CASE
                            WHEN ((c."call_週三" / c."put_週三") > (1)::double precision) THEN '多'::text
                            WHEN ((c."call_週三" / c."put_週三") < (1)::double precision) THEN '空'::text
                            ELSE '?'::text
                        END
                        ELSE '?'::text
                    END
                    ELSE '?'::text
                END) AS "選權籌碼"
           FROM (((((pos_calc c
             LEFT JOIN open_interest_calc oi ON (((oi."日期" = c."日期") AND (oi.futures_id = c.futures_id))))
             LEFT JOIN report.v_futures_main_force mf ON (((mf."日期" = c."日期") AND (mf.futures_id = c.futures_id))))
             LEFT JOIN small_micro_retail mtx ON (((mtx."日期" = c."日期") AND (mtx.futures_id = 'MTX'::text))))
             LEFT JOIN small_micro_retail tmf ON (((tmf."日期" = c."日期") AND (tmf.futures_id = 'TMF'::text))))
             LEFT JOIN taiex_chip tc ON ((tc."日期" = c."日期")))
        ), strategy_base AS (
         SELECT rb.id,
            rb.futures_id,
            rb."日期",
            rb."年",
            rb."月",
            rb."星期",
            rb."週次",
            rb."天別",
            rb."日盤開",
            rb."日盤收",
            rb."日盤高",
            rb."日盤低",
            rb."昨日收盤",
            rb."日漲跌",
            rb."日價漲跌_pct",
            rb."日收減日開",
            rb."日收開_昨收_pct",
            rb."日收開_開盤_pct",
            rb."日振幅",
            rb."日振幅_昨收_pct",
            rb."日振幅_開盤_pct",
            rb."日成交量",
            rb."昨日成交量",
            rb."日5最大量_日期",
            rb."日5最大量",
            rb."日10最大量_日期",
            rb."日10最大量",
            rb."日20最大量_日期",
            rb."日20最大量",
            rb."日60最大量_日期",
            rb."日60最大量",
            rb."日5均量",
            rb."日10均量",
            rb."日20均量",
            rb."日60均量",
            rb."日量增減_昨日_pct",
            rb."日量增減_五日_pct",
            rb."日量增減_十日_pct",
            rb."日量增減_二十日_pct",
            rb."日量增減_六十日_pct",
            rb."日當日價量",
            rb."日5日價量",
            rb."日10日價量",
            rb."日20日價量",
            rb."日60日價量",
            rb."跳空缺口",
            rb."跳空缺口_昨收_pct",
            rb."實體_pct",
            rb."上影_pct",
            rb."下影_pct",
            rb."日5MA",
            rb."日10MA",
            rb."日20MA",
            rb."日60MA",
            rb."日120MA",
            rb."日5均上升幅度_點",
            rb."日10均上升幅度_點",
            rb."日20均上升幅度_點",
            rb."日60均上升幅度_點",
            rb."日5均上升幅度_pct",
            rb."日10均上升幅度_pct",
            rb."日20均上升幅度_pct",
            rb."日60均上升幅度_pct",
            rb."日5_明日扣抵值",
            rb."日10_明日扣抵值",
            rb."日20_明日扣抵值",
            rb."日60_明日扣抵值",
            rb."日5_明日扣抵值影響_點",
            rb."日10_明日扣抵值影響_點",
            rb."日20_明日扣抵值影響_點",
            rb."日60_明日扣抵值影響_點",
            rb."日5_明日扣抵值影響_pct",
            rb."日10_明日扣抵值影響_pct",
            rb."日20_明日扣抵值影響_pct",
            rb."日60_明日扣抵值影響_pct",
            rb."日5MA乖離率",
            rb."日10MA乖離率",
            rb."日20MA乖離率",
            rb."日60MA乖離率",
            rb."日布林標準差",
            rb."日布林上軌",
            rb."日布林下軌",
            rb."日法人多倉",
            rb."日法人空倉",
            rb."未沖銷_全",
            rb."未沖銷_近期",
            rb."未沖銷_遠期",
            rb."主力_月結算狀態",
            rb."主力新建多單",
            rb."主力新建空單",
            rb."主力淨多空和_5日",
            rb."主力淨多空和_10日",
            rb."主力淨多空和_20日",
            rb."主力淨多空和_60日",
            rb."夜盤開",
            rb."夜盤收",
            rb."夜盤高",
            rb."夜盤低",
            rb."夜5MA",
            rb."夜10MA",
            rb."夜20MA",
            rb."夜60MA",
            rb."夜120MA",
            rb."夜成交量",
            rb."夜法人多單量",
            rb."夜法人空單量",
            rb."全日開",
            rb."全日收",
            rb."全日高",
            rb."全日低",
            rb."全5MA",
            rb."全10MA",
            rb."全20MA",
            rb."全60MA",
            rb."全120MA",
            rb."全市場OI_小台",
            rb."三大法人多單_小台",
            rb."三大法人空單_小台",
            rb."散戶多單_小台",
            rb."散戶空單_小台",
            rb."散戶多空比_小台",
            rb."全市場OI_微台",
            rb."三大法人多單_微台",
            rb."三大法人空單_微台",
            rb."散戶多單_微台",
            rb."散戶空單_微台",
            rb."散戶多空比_微台",
            rb."法人總買超_億",
            rb."法人籌碼",
            rb."散戶籌碼",
            rb."call_月選",
            rb."put_月選",
            rb."買賣權比_月選",
            rb."call_週三",
            rb."put_週三",
            rb."買賣權比_週三",
            rb."call_週五",
            rb."put_週五",
            rb."買賣權比_週五",
            rb."日價漲跌",
            rb."日量增減_昨日",
            rb."日量增減_五日",
            rb."日MA方向_5",
            rb."日MA方向_10",
            rb."日MA方向_20",
            rb."日位置",
            rb."策略_線型",
            rb."策略_價量昨日",
            rb."策略_價量五日",
            rb."加權籌碼",
            rb."期貨籌碼",
            rb."選權籌碼",
                CASE
                    WHEN (rb."加權籌碼" = '多'::text) THEN
                    CASE
                        WHEN (rb."期貨籌碼" = ANY (ARRAY['多方'::text, '多空雙殺_偏多'::text])) THEN '多'::text
                        WHEN (rb."期貨籌碼" = ANY (ARRAY['空方'::text, '多空雙殺_偏空'::text])) THEN
                        CASE
                            WHEN (rb."選權籌碼" ~~ '%短多'::text) THEN '多'::text
                            ELSE '空'::text
                        END
                        ELSE '多'::text
                    END
                    WHEN (rb."加權籌碼" = '空'::text) THEN
                    CASE
                        WHEN (rb."期貨籌碼" = ANY (ARRAY['多方'::text, '多空雙殺_偏多'::text])) THEN
                        CASE
                            WHEN (rb."選權籌碼" ~~ '%短空'::text) THEN '空'::text
                            ELSE '多'::text
                        END
                        WHEN (rb."期貨籌碼" = ANY (ARRAY['空方'::text, '多空雙殺_偏空'::text])) THEN '空'::text
                        ELSE '空'::text
                    END
                    WHEN (rb."加權籌碼" = '無'::text) THEN
                    CASE
                        WHEN (rb."期貨籌碼" = ANY (ARRAY['多方'::text, '多空雙殺_偏多'::text])) THEN '多'::text
                        WHEN (rb."期貨籌碼" = ANY (ARRAY['空方'::text, '多空雙殺_偏空'::text])) THEN '空'::text
                        ELSE
                        CASE
                            WHEN (rb."選權籌碼" ~~ '%短多'::text) THEN '多'::text
                            WHEN (rb."選權籌碼" ~~ '%短空'::text) THEN '空'::text
                            ELSE '無'::text
                        END
                    END
                    ELSE '無'::text
                END AS "策略_籌碼"
           FROM report_base rb
        )
 SELECT sb.id,
    sb.futures_id,
    sb."日期",
    sb."年",
    sb."月",
    sb."星期",
    sb."週次",
    sb."天別",
    sb."日盤開",
    sb."日盤收",
    sb."日盤高",
    sb."日盤低",
    sb."昨日收盤",
    sb."日漲跌",
    sb."日價漲跌_pct",
    sb."日收減日開",
    sb."日收開_昨收_pct",
    sb."日收開_開盤_pct",
    sb."日振幅",
    sb."日振幅_昨收_pct",
    sb."日振幅_開盤_pct",
    sb."日成交量",
    sb."昨日成交量",
    sb."日5最大量_日期",
    sb."日5最大量",
    sb."日10最大量_日期",
    sb."日10最大量",
    sb."日20最大量_日期",
    sb."日20最大量",
    sb."日60最大量_日期",
    sb."日60最大量",
    sb."日5均量",
    sb."日10均量",
    sb."日20均量",
    sb."日60均量",
    sb."日量增減_昨日_pct",
    sb."日量增減_五日_pct",
    sb."日量增減_十日_pct",
    sb."日量增減_二十日_pct",
    sb."日量增減_六十日_pct",
    sb."日當日價量",
    sb."日5日價量",
    sb."日10日價量",
    sb."日20日價量",
    sb."日60日價量",
    sb."跳空缺口",
    sb."跳空缺口_昨收_pct",
    sb."實體_pct",
    sb."上影_pct",
    sb."下影_pct",
    sb."日5MA",
    sb."日10MA",
    sb."日20MA",
    sb."日60MA",
    sb."日120MA",
    sb."日5均上升幅度_點",
    sb."日10均上升幅度_點",
    sb."日20均上升幅度_點",
    sb."日60均上升幅度_點",
    sb."日5均上升幅度_pct",
    sb."日10均上升幅度_pct",
    sb."日20均上升幅度_pct",
    sb."日60均上升幅度_pct",
    sb."日5_明日扣抵值",
    sb."日10_明日扣抵值",
    sb."日20_明日扣抵值",
    sb."日60_明日扣抵值",
    sb."日5_明日扣抵值影響_點",
    sb."日10_明日扣抵值影響_點",
    sb."日20_明日扣抵值影響_點",
    sb."日60_明日扣抵值影響_點",
    sb."日5_明日扣抵值影響_pct",
    sb."日10_明日扣抵值影響_pct",
    sb."日20_明日扣抵值影響_pct",
    sb."日60_明日扣抵值影響_pct",
    sb."日5MA乖離率",
    sb."日10MA乖離率",
    sb."日20MA乖離率",
    sb."日60MA乖離率",
    sb."日布林標準差",
    sb."日布林上軌",
    sb."日布林下軌",
    sb."日法人多倉",
    sb."日法人空倉",
    sb."未沖銷_全",
    sb."未沖銷_近期",
    sb."未沖銷_遠期",
    sb."主力_月結算狀態",
    sb."主力新建多單",
    sb."主力新建空單",
    sb."主力淨多空和_5日",
    sb."主力淨多空和_10日",
    sb."主力淨多空和_20日",
    sb."主力淨多空和_60日",
    sb."夜盤開",
    sb."夜盤收",
    sb."夜盤高",
    sb."夜盤低",
    sb."夜5MA",
    sb."夜10MA",
    sb."夜20MA",
    sb."夜60MA",
    sb."夜120MA",
    sb."夜成交量",
    sb."夜法人多單量",
    sb."夜法人空單量",
    sb."全日開",
    sb."全日收",
    sb."全日高",
    sb."全日低",
    sb."全5MA",
    sb."全10MA",
    sb."全20MA",
    sb."全60MA",
    sb."全120MA",
    sb."全市場OI_小台",
    sb."三大法人多單_小台",
    sb."三大法人空單_小台",
    sb."散戶多單_小台",
    sb."散戶空單_小台",
    sb."散戶多空比_小台",
    sb."全市場OI_微台",
    sb."三大法人多單_微台",
    sb."三大法人空單_微台",
    sb."散戶多單_微台",
    sb."散戶空單_微台",
    sb."散戶多空比_微台",
    sb."法人總買超_億",
    sb."法人籌碼",
    sb."散戶籌碼",
    sb."call_月選",
    sb."put_月選",
    sb."買賣權比_月選",
    sb."call_週三",
    sb."put_週三",
    sb."買賣權比_週三",
    sb."call_週五",
    sb."put_週五",
    sb."買賣權比_週五",
    sb."日價漲跌",
    sb."日量增減_昨日",
    sb."日量增減_五日",
    sb."日MA方向_5",
    sb."日MA方向_10",
    sb."日MA方向_20",
    sb."日位置",
    sb."策略_線型",
    sb."策略_價量昨日",
    sb."策略_價量五日",
    sb."加權籌碼",
    sb."期貨籌碼",
    sb."選權籌碼",
    sb."策略_籌碼",
        CASE
            WHEN ((sb."策略_價量昨日" = '多'::text) AND (sb."策略_線型" = '多'::text)) THEN '多'::text
            WHEN ((sb."策略_價量昨日" = '空'::text) AND (sb."策略_線型" = '空'::text)) THEN '空'::text
            WHEN ((sb."策略_價量昨日" = ANY (ARRAY['多'::text, '空'::text])) AND (sb."策略_線型" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
            ELSE '無'::text
        END AS "策略_價量昨日_線型",
        CASE
            WHEN ((sb."策略_價量五日" = '多'::text) AND (sb."策略_線型" = '多'::text)) THEN '多'::text
            WHEN ((sb."策略_價量五日" = '空'::text) AND (sb."策略_線型" = '空'::text)) THEN '空'::text
            WHEN ((sb."策略_價量五日" = ANY (ARRAY['多'::text, '空'::text])) AND (sb."策略_線型" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
            ELSE '無'::text
        END AS "策略_價量五日_線型",
        CASE
            WHEN ((sb."策略_價量昨日" = '多'::text) AND (sb."策略_籌碼" = '多'::text)) THEN '多'::text
            WHEN ((sb."策略_價量昨日" = '空'::text) AND (sb."策略_籌碼" = '空'::text)) THEN '空'::text
            WHEN ((sb."策略_價量昨日" = ANY (ARRAY['多'::text, '空'::text])) AND (sb."策略_籌碼" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
            ELSE '無'::text
        END AS "策略_價量昨日_籌碼",
        CASE
            WHEN ((sb."策略_價量五日" = '多'::text) AND (sb."策略_籌碼" = '多'::text)) THEN '多'::text
            WHEN ((sb."策略_價量五日" = '空'::text) AND (sb."策略_籌碼" = '空'::text)) THEN '空'::text
            WHEN ((sb."策略_價量五日" = ANY (ARRAY['多'::text, '空'::text])) AND (sb."策略_籌碼" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
            ELSE '無'::text
        END AS "策略_價量五日_籌碼",
        CASE
            WHEN ((sb."策略_線型" = '多'::text) AND (sb."策略_籌碼" = '多'::text)) THEN '多'::text
            WHEN ((sb."策略_線型" = '空'::text) AND (sb."策略_籌碼" = '空'::text)) THEN '空'::text
            WHEN ((sb."策略_線型" = ANY (ARRAY['多'::text, '空'::text])) AND (sb."策略_籌碼" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
            ELSE '無'::text
        END AS "策略_線型_籌碼",
        CASE
            WHEN (sb."策略_價量五日" = '多'::text) THEN
            CASE
                WHEN (sb."策略_線型" = '多'::text) THEN '多'::text
                WHEN (sb."策略_線型" = '空'::text) THEN
                CASE
                    WHEN (sb."策略_籌碼" = '多'::text) THEN '多'::text
                    ELSE '空'::text
                END
                WHEN (sb."策略_線型" = '無'::text) THEN '多'::text
                ELSE '多'::text
            END
            WHEN (sb."策略_價量五日" = '空'::text) THEN
            CASE
                WHEN (sb."策略_線型" = '多'::text) THEN
                CASE
                    WHEN (sb."策略_籌碼" = '空'::text) THEN '空'::text
                    ELSE '多'::text
                END
                WHEN (sb."策略_線型" = '空'::text) THEN '空'::text
                WHEN (sb."策略_線型" = '無'::text) THEN '空'::text
                ELSE '空'::text
            END
            WHEN (sb."策略_價量五日" = '無'::text) THEN
            CASE
                WHEN (sb."策略_線型" = '多'::text) THEN '多'::text
                WHEN (sb."策略_線型" = '空'::text) THEN '空'::text
                WHEN (sb."策略_線型" = '無'::text) THEN
                CASE
                    WHEN (sb."策略_籌碼" = '多'::text) THEN '多'::text
                    WHEN (sb."策略_籌碼" = '空'::text) THEN '空'::text
                    ELSE '無'::text
                END
                ELSE '無'::text
            END
            ELSE '無'::text
        END AS "策略_核心",
    bt."建立時間"
   FROM (strategy_base sb
     LEFT JOIN ( SELECT base.id,
            base."建立時間"
           FROM base) bt ON ((bt.id = sb.id)));


--
-- Name: v_futures_report_daily_core; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_report_daily_core AS
 WITH original AS (
         SELECT src.id,
            src.futures_id,
            src."日期",
            src."年",
            src."月",
            src."星期",
            src."週次",
            src."天別",
            src."全日開",
            src."全日收",
            src."全日高",
            src."全日低",
            src."全5MA",
            src."全10MA",
            src."全20MA",
            src."全60MA",
            src."全120MA",
            src."日盤開",
            src."日盤收",
            src."日盤高",
            src."日盤低",
            src."昨日收盤",
            src."日漲跌",
            src."日價漲跌_pct",
            src."日收減日開",
            src."日收開_昨收_pct",
            src."日收開_開盤_pct",
            src."日振幅",
            src."日振幅_昨收_pct",
            src."日振幅_開盤_pct",
            src."日成交量",
            src."日5最大量_日期",
            src."日5最大量",
            src."日10最大量_日期",
            src."日10最大量",
            src."日20最大量_日期",
            src."日20最大量",
            src."日60最大量_日期",
            src."日60最大量",
            src."日5均量",
            src."日10均量",
            src."日20均量",
            src."日60均量",
            src."日量增減_昨日_pct",
            src."日量增減_五日_pct",
            src."日量增減_十日_pct",
            src."日量增減_二十日_pct",
            src."日量增減_六十日_pct",
            src."日當日價量",
            src."日5日價量",
            src."日10日價量",
            src."日20日價量",
            src."日60日價量",
            src."K線型態",
            src."跳空缺口",
            src."跳空缺口_昨收_pct",
            src."實體_pct",
            src."上影_pct",
            src."下影_pct",
            src."日5MA",
            src."日10MA",
            src."日20MA",
            src."日60MA",
            src."日120MA",
            src."日240MA",
            src."日5均上升幅度_點",
            src."日10均上升幅度_點",
            src."日20均上升幅度_點",
            src."日60均上升幅度_點",
            src."日5均上升幅度_pct",
            src."日10均上升幅度_pct",
            src."日20均上升幅度_pct",
            src."日60均上升幅度_pct",
            src."日5_明日扣抵值",
            src."日10_明日扣抵值",
            src."日20_明日扣抵值",
            src."日60_明日扣抵值",
            src."日5_明日扣抵值影響_點",
            src."日10_明日扣抵值影響_點",
            src."日20_明日扣抵值影響_點",
            src."日60_明日扣抵值影響_點",
            src."日5_明日扣抵值影響_pct",
            src."日10_明日扣抵值影響_pct",
            src."日20_明日扣抵值影響_pct",
            src."日60_明日扣抵值影響_pct",
            src."日5MA乖離率",
            src."日10MA乖離率",
            src."日20MA乖離率",
            src."日60MA乖離率",
            src."日布林標準差",
            src."日布林上軌",
            src."日布林下軌",
            src."日法人多倉",
            src."日法人空倉",
            src."未沖銷_全",
            src."未沖銷_近期",
            src."未沖銷_遠期",
            src."主力_月結算狀態",
            src."主力新建多單",
            src."主力新建空單",
            src."主力淨多空和_5日",
            src."主力淨多空和_10日",
            src."主力淨多空和_20日",
            src."主力淨多空和_60日",
            src."夜盤開",
            src."夜盤收",
            src."夜盤高",
            src."夜盤低",
            src."夜5MA",
            src."夜成交量",
            src."夜法人多單量",
            src."夜法人空單量",
            src."全市場OI_小台",
            src."三大法人多單_小台",
            src."三大法人空單_小台",
            src."散戶多單_小台",
            src."散戶空單_小台",
            src."散戶多空比_小台",
            src."全市場OI_微台",
            src."三大法人多單_微台",
            src."三大法人空單_微台",
            src."散戶多單_微台",
            src."散戶空單_微台",
            src."散戶多空比_微台",
            src."法人總買超_億",
            src."call_月選",
            src."put_月選",
            src."買賣權比_月選",
            src."call_週三",
            src."put_週三",
            src."買賣權比_週三",
            src."call_週五",
            src."put_週五",
            src."買賣權比_週五",
            src."日位置",
            src."策略_價量昨日",
            src."策略_價量五日",
            src."日MA方向_5",
            src."日MA方向_10",
            src."日MA方向_20",
            src."策略_線型",
            src."法人籌碼",
            src."散戶籌碼",
            src."加權籌碼",
            src."期貨籌碼",
            src."選權籌碼",
            src."策略_籌碼",
            src."策略_價量昨日_線型",
            src."策略_價量五日_線型",
            src."策略_價量昨日_籌碼",
            src."策略_價量五日_籌碼",
            src."策略_線型_籌碼",
            src."策略_核心",
            src."建立時間"
           FROM ( SELECT src_1.id,
                    src_1.futures_id,
                    src_1."日期",
                    src_1."年",
                    src_1."月",
                    src_1."星期",
                    src_1."週次",
                    src_1."天別",
                    src_1."全日開",
                    src_1."全日收",
                    src_1."全日高",
                    src_1."全日低",
                    src_1."全5MA",
                    src_1."全10MA",
                    src_1."全20MA",
                    src_1."全60MA",
                    src_1."全120MA",
                    src_1."日盤開",
                    src_1."日盤收",
                    src_1."日盤高",
                    src_1."日盤低",
                    src_1."昨日收盤",
                    src_1."日漲跌",
                    src_1."日價漲跌_pct",
                    src_1."日收減日開",
                    src_1."日收開_昨收_pct",
                    src_1."日收開_開盤_pct",
                    src_1."日振幅",
                    src_1."日振幅_昨收_pct",
                    src_1."日振幅_開盤_pct",
                    src_1."日成交量",
                    src_1."日5最大量_日期",
                    src_1."日5最大量",
                    src_1."日10最大量_日期",
                    src_1."日10最大量",
                    src_1."日20最大量_日期",
                    src_1."日20最大量",
                    src_1."日60最大量_日期",
                    src_1."日60最大量",
                    src_1."日5均量",
                    src_1."日10均量",
                    src_1."日20均量",
                    src_1."日60均量",
                    src_1."日量增減_昨日_pct",
                    src_1."日量增減_五日_pct",
                    src_1."日量增減_十日_pct",
                    src_1."日量增減_二十日_pct",
                    src_1."日量增減_六十日_pct",
                    src_1."日當日價量",
                    src_1."日5日價量",
                    src_1."日10日價量",
                    src_1."日20日價量",
                    src_1."日60日價量",
                    src_1."K線型態",
                    src_1."跳空缺口",
                    src_1."跳空缺口_昨收_pct",
                    src_1."實體_pct",
                    src_1."上影_pct",
                    src_1."下影_pct",
                    src_1."日5MA",
                    src_1."日10MA",
                    src_1."日20MA",
                    src_1."日60MA",
                    src_1."日120MA",
                    src_1."日240MA",
                    src_1."日5均上升幅度_點",
                    src_1."日10均上升幅度_點",
                    src_1."日20均上升幅度_點",
                    src_1."日60均上升幅度_點",
                    src_1."日5均上升幅度_pct",
                    src_1."日10均上升幅度_pct",
                    src_1."日20均上升幅度_pct",
                    src_1."日60均上升幅度_pct",
                    src_1."日5_明日扣抵值",
                    src_1."日10_明日扣抵值",
                    src_1."日20_明日扣抵值",
                    src_1."日60_明日扣抵值",
                    src_1."日5_明日扣抵值影響_點",
                    src_1."日10_明日扣抵值影響_點",
                    src_1."日20_明日扣抵值影響_點",
                    src_1."日60_明日扣抵值影響_點",
                    src_1."日5_明日扣抵值影響_pct",
                    src_1."日10_明日扣抵值影響_pct",
                    src_1."日20_明日扣抵值影響_pct",
                    src_1."日60_明日扣抵值影響_pct",
                    src_1."日5MA乖離率",
                    src_1."日10MA乖離率",
                    src_1."日20MA乖離率",
                    src_1."日60MA乖離率",
                    src_1."日布林標準差",
                    src_1."日布林上軌",
                    src_1."日布林下軌",
                    src_1."日法人多倉",
                    src_1."日法人空倉",
                    src_1."未沖銷_全",
                    src_1."未沖銷_近期",
                    src_1."未沖銷_遠期",
                    src_1."主力_月結算狀態",
                    src_1."主力新建多單",
                    src_1."主力新建空單",
                    src_1."主力淨多空和_5日",
                    src_1."主力淨多空和_10日",
                    src_1."主力淨多空和_20日",
                    src_1."主力淨多空和_60日",
                    src_1."夜盤開",
                    src_1."夜盤收",
                    src_1."夜盤高",
                    src_1."夜盤低",
                    src_1."夜5MA",
                    src_1."夜成交量",
                    src_1."夜法人多單量",
                    src_1."夜法人空單量",
                    src_1."全市場OI_小台",
                    src_1."三大法人多單_小台",
                    src_1."三大法人空單_小台",
                    src_1."散戶多單_小台",
                    src_1."散戶空單_小台",
                    src_1."散戶多空比_小台",
                    src_1."全市場OI_微台",
                    src_1."三大法人多單_微台",
                    src_1."三大法人空單_微台",
                    src_1."散戶多單_微台",
                    src_1."散戶空單_微台",
                    src_1."散戶多空比_微台",
                    src_1."法人總買超_億",
                    src_1."法人籌碼",
                    src_1."散戶籌碼",
                    src_1."call_月選",
                    src_1."put_月選",
                    src_1."買賣權比_月選",
                    src_1."call_週三",
                    src_1."put_週三",
                    src_1."買賣權比_週三",
                    src_1."call_週五",
                    src_1."put_週五",
                    src_1."買賣權比_週五",
                    src_1."日MA方向_5",
                    src_1."日MA方向_10",
                    src_1."日MA方向_20",
                    src_1."日位置",
                    src_1."策略_線型",
                    src_1."策略_價量昨日",
                    src_1."策略_價量五日",
                    src_1."加權籌碼",
                    src_1."期貨籌碼",
                    src_1."選權籌碼",
                    src_1."策略_籌碼",
                    src_1."策略_價量昨日_線型",
                    src_1."策略_價量五日_線型",
                    src_1."策略_價量昨日_籌碼",
                    src_1."策略_價量五日_籌碼",
                    src_1."策略_線型_籌碼",
                    src_1."策略_核心",
                    src_1."建立時間"
                   FROM ( SELECT s.id,
                            s.futures_id,
                            s."日期",
                            s."年",
                            s."月",
                            s."星期",
                            s."週次",
                            r."天別",
                            s."日盤開",
                            s."日盤收",
                            s."日盤高",
                            s."日盤低",
                            s."昨日收盤",
                            s."日漲跌",
                            s."日價漲跌_pct",
                            s."日收減日開",
                            s."日收開_昨收_pct",
                            s."日收開_開盤_pct",
                            s."日振幅",
                            s."日振幅_昨收_pct",
                            s."日振幅_開盤_pct",
                            s."日成交量",
                            s."日5最大量_日期",
                            s."日5最大量",
                            s."日10最大量_日期",
                            s."日10最大量",
                            s."日20最大量_日期",
                            s."日20最大量",
                            s."日60最大量_日期",
                            s."日60最大量",
                            s."日5均量",
                            s."日10均量",
                            s."日20均量",
                            s."日60均量",
                            s."日量增減_昨日_pct",
                            s."日量增減_五日_pct",
                            s."日量增減_十日_pct",
                            s."日量增減_二十日_pct",
                            s."日量增減_六十日_pct",
                            s."日當日價量",
                            s."日5日價量",
                            s."日10日價量",
                            s."日20日價量",
                            s."日60日價量",
                            r."K線型態",
                            s."跳空缺口",
                            s."跳空缺口_昨收_pct",
                            s."實體_pct",
                            s."上影_pct",
                            s."下影_pct",
                            s."日5MA",
                            s."日10MA",
                            s."日20MA",
                            s."日60MA",
                            s."日120MA",
                            r."日240MA",
                            s."日5均上升幅度_點",
                            s."日10均上升幅度_點",
                            s."日20均上升幅度_點",
                            s."日60均上升幅度_點",
                            s."日5均上升幅度_pct",
                            s."日10均上升幅度_pct",
                            s."日20均上升幅度_pct",
                            s."日60均上升幅度_pct",
                            s."日5_明日扣抵值",
                            s."日10_明日扣抵值",
                            s."日20_明日扣抵值",
                            s."日60_明日扣抵值",
                            s."日5_明日扣抵值影響_點",
                            s."日10_明日扣抵值影響_點",
                            s."日20_明日扣抵值影響_點",
                            s."日60_明日扣抵值影響_點",
                            s."日5_明日扣抵值影響_pct",
                            s."日10_明日扣抵值影響_pct",
                            s."日20_明日扣抵值影響_pct",
                            s."日60_明日扣抵值影響_pct",
                            s."日5MA乖離率",
                            s."日10MA乖離率",
                            s."日20MA乖離率",
                            s."日60MA乖離率",
                            s."日布林標準差",
                            s."日布林上軌",
                            s."日布林下軌",
                            s."日法人多倉",
                            s."日法人空倉",
                            s."未沖銷_全",
                            s."未沖銷_近期",
                            s."未沖銷_遠期",
                            s."主力_月結算狀態",
                            s."主力新建多單",
                            s."主力新建空單",
                            s."主力淨多空和_5日",
                            s."主力淨多空和_10日",
                            s."主力淨多空和_20日",
                            s."主力淨多空和_60日",
                            s."夜盤開",
                            s."夜盤收",
                            s."夜盤高",
                            s."夜盤低",
                            s."夜5MA",
                            s."夜成交量",
                            s."夜法人多單量",
                            s."夜法人空單量",
                            s."全日開",
                            s."全日收",
                            s."全日高",
                            s."全日低",
                            s."全5MA",
                            s."全10MA",
                            s."全20MA",
                            s."全60MA",
                            s."全120MA",
                            s."全市場OI_小台",
                            s."三大法人多單_小台",
                            s."三大法人空單_小台",
                            s."散戶多單_小台",
                            s."散戶空單_小台",
                            s."散戶多空比_小台",
                            s."全市場OI_微台",
                            s."三大法人多單_微台",
                            s."三大法人空單_微台",
                            s."散戶多單_微台",
                            s."散戶空單_微台",
                            s."散戶多空比_微台",
                            s."法人總買超_億",
                            s."法人籌碼",
                            s."散戶籌碼",
                            s."call_月選",
                            s."put_月選",
                            s."買賣權比_月選",
                            s."call_週三",
                            s."put_週三",
                            s."買賣權比_週三",
                            s."call_週五",
                            s."put_週五",
                            s."買賣權比_週五",
                            s."日MA方向_5",
                            s."日MA方向_10",
                            s."日MA方向_20",
                            s."日位置",
                            s."策略_線型",
                            s."策略_價量昨日",
                            s."策略_價量五日",
                            s."加權籌碼",
                            s."期貨籌碼",
                            s."選權籌碼",
                            s."策略_籌碼",
                            s."策略_價量昨日_線型",
                            s."策略_價量五日_線型",
                            s."策略_價量昨日_籌碼",
                            s."策略_價量五日_籌碼",
                            s."策略_線型_籌碼",
                            s."策略_核心",
                            s."建立時間"
                           FROM (report.v_futures_report_daily_model s
                             JOIN core.futures_report_daily_raw r ON (((r.futures_id = s.futures_id) AND (r."日期" = s."日期"))))) src_1) src
        ), full_metrics AS (
         SELECT o.id,
            o.futures_id,
            o."日期",
            o."年",
            o."月",
            o."星期",
            o."週次",
            o."天別",
            o."全日開",
            o."全日收",
            o."全日高",
            o."全日低",
            o."全5MA",
            o."全10MA",
            o."全20MA",
            o."全60MA",
            o."全120MA",
            o."日盤開",
            o."日盤收",
            o."日盤高",
            o."日盤低",
            o."昨日收盤",
            o."日漲跌",
            o."日價漲跌_pct",
            o."日收減日開",
            o."日收開_昨收_pct",
            o."日收開_開盤_pct",
            o."日振幅",
            o."日振幅_昨收_pct",
            o."日振幅_開盤_pct",
            o."日成交量",
            o."日5最大量_日期",
            o."日5最大量",
            o."日10最大量_日期",
            o."日10最大量",
            o."日20最大量_日期",
            o."日20最大量",
            o."日60最大量_日期",
            o."日60最大量",
            o."日5均量",
            o."日10均量",
            o."日20均量",
            o."日60均量",
            o."日量增減_昨日_pct",
            o."日量增減_五日_pct",
            o."日量增減_十日_pct",
            o."日量增減_二十日_pct",
            o."日量增減_六十日_pct",
            o."日當日價量",
            o."日5日價量",
            o."日10日價量",
            o."日20日價量",
            o."日60日價量",
            o."K線型態",
            o."跳空缺口",
            o."跳空缺口_昨收_pct",
            o."實體_pct",
            o."上影_pct",
            o."下影_pct",
            o."日5MA",
            o."日10MA",
            o."日20MA",
            o."日60MA",
            o."日120MA",
            o."日240MA",
            o."日5均上升幅度_點",
            o."日10均上升幅度_點",
            o."日20均上升幅度_點",
            o."日60均上升幅度_點",
            o."日5均上升幅度_pct",
            o."日10均上升幅度_pct",
            o."日20均上升幅度_pct",
            o."日60均上升幅度_pct",
            o."日5_明日扣抵值",
            o."日10_明日扣抵值",
            o."日20_明日扣抵值",
            o."日60_明日扣抵值",
            o."日5_明日扣抵值影響_點",
            o."日10_明日扣抵值影響_點",
            o."日20_明日扣抵值影響_點",
            o."日60_明日扣抵值影響_點",
            o."日5_明日扣抵值影響_pct",
            o."日10_明日扣抵值影響_pct",
            o."日20_明日扣抵值影響_pct",
            o."日60_明日扣抵值影響_pct",
            o."日5MA乖離率",
            o."日10MA乖離率",
            o."日20MA乖離率",
            o."日60MA乖離率",
            o."日布林標準差",
            o."日布林上軌",
            o."日布林下軌",
            o."日法人多倉",
            o."日法人空倉",
            o."未沖銷_全",
            o."未沖銷_近期",
            o."未沖銷_遠期",
            o."主力_月結算狀態",
            o."主力新建多單",
            o."主力新建空單",
            o."主力淨多空和_5日",
            o."主力淨多空和_10日",
            o."主力淨多空和_20日",
            o."主力淨多空和_60日",
            o."夜盤開",
            o."夜盤收",
            o."夜盤高",
            o."夜盤低",
            o."夜5MA",
            o."夜成交量",
            o."夜法人多單量",
            o."夜法人空單量",
            o."全市場OI_小台",
            o."三大法人多單_小台",
            o."三大法人空單_小台",
            o."散戶多單_小台",
            o."散戶空單_小台",
            o."散戶多空比_小台",
            o."全市場OI_微台",
            o."三大法人多單_微台",
            o."三大法人空單_微台",
            o."散戶多單_微台",
            o."散戶空單_微台",
            o."散戶多空比_微台",
            o."法人總買超_億",
            o."call_月選",
            o."put_月選",
            o."買賣權比_月選",
            o."call_週三",
            o."put_週三",
            o."買賣權比_週三",
            o."call_週五",
            o."put_週五",
            o."買賣權比_週五",
            o."日位置",
            o."策略_價量昨日",
            o."策略_價量五日",
            o."日MA方向_5",
            o."日MA方向_10",
            o."日MA方向_20",
            o."策略_線型",
            o."法人籌碼",
            o."散戶籌碼",
            o."加權籌碼",
            o."期貨籌碼",
            o."選權籌碼",
            o."策略_籌碼",
            o."策略_價量昨日_線型",
            o."策略_價量五日_線型",
            o."策略_價量昨日_籌碼",
            o."策略_價量五日_籌碼",
            o."策略_線型_籌碼",
            o."策略_核心",
            o."建立時間",
            lag(o."全日收") OVER w AS "_昨全日收",
            lag(o."全5MA") OVER w AS "_昨全5MA",
            lag(o."全10MA") OVER w AS "_昨全10MA",
            lag(o."全20MA") OVER w AS "_昨全20MA",
            stddev_samp(o."全日收") OVER (PARTITION BY o.futures_id ORDER BY o."日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS "_全布林標準差"
           FROM original o
          WINDOW w AS (PARTITION BY o.futures_id ORDER BY o."日期")
        ), full_state AS (
         SELECT m.id,
            m.futures_id,
            m."日期",
            m."年",
            m."月",
            m."星期",
            m."週次",
            m."天別",
            m."全日開",
            m."全日收",
            m."全日高",
            m."全日低",
            m."全5MA",
            m."全10MA",
            m."全20MA",
            m."全60MA",
            m."全120MA",
            m."日盤開",
            m."日盤收",
            m."日盤高",
            m."日盤低",
            m."昨日收盤",
            m."日漲跌",
            m."日價漲跌_pct",
            m."日收減日開",
            m."日收開_昨收_pct",
            m."日收開_開盤_pct",
            m."日振幅",
            m."日振幅_昨收_pct",
            m."日振幅_開盤_pct",
            m."日成交量",
            m."日5最大量_日期",
            m."日5最大量",
            m."日10最大量_日期",
            m."日10最大量",
            m."日20最大量_日期",
            m."日20最大量",
            m."日60最大量_日期",
            m."日60最大量",
            m."日5均量",
            m."日10均量",
            m."日20均量",
            m."日60均量",
            m."日量增減_昨日_pct",
            m."日量增減_五日_pct",
            m."日量增減_十日_pct",
            m."日量增減_二十日_pct",
            m."日量增減_六十日_pct",
            m."日當日價量",
            m."日5日價量",
            m."日10日價量",
            m."日20日價量",
            m."日60日價量",
            m."K線型態",
            m."跳空缺口",
            m."跳空缺口_昨收_pct",
            m."實體_pct",
            m."上影_pct",
            m."下影_pct",
            m."日5MA",
            m."日10MA",
            m."日20MA",
            m."日60MA",
            m."日120MA",
            m."日240MA",
            m."日5均上升幅度_點",
            m."日10均上升幅度_點",
            m."日20均上升幅度_點",
            m."日60均上升幅度_點",
            m."日5均上升幅度_pct",
            m."日10均上升幅度_pct",
            m."日20均上升幅度_pct",
            m."日60均上升幅度_pct",
            m."日5_明日扣抵值",
            m."日10_明日扣抵值",
            m."日20_明日扣抵值",
            m."日60_明日扣抵值",
            m."日5_明日扣抵值影響_點",
            m."日10_明日扣抵值影響_點",
            m."日20_明日扣抵值影響_點",
            m."日60_明日扣抵值影響_點",
            m."日5_明日扣抵值影響_pct",
            m."日10_明日扣抵值影響_pct",
            m."日20_明日扣抵值影響_pct",
            m."日60_明日扣抵值影響_pct",
            m."日5MA乖離率",
            m."日10MA乖離率",
            m."日20MA乖離率",
            m."日60MA乖離率",
            m."日布林標準差",
            m."日布林上軌",
            m."日布林下軌",
            m."日法人多倉",
            m."日法人空倉",
            m."未沖銷_全",
            m."未沖銷_近期",
            m."未沖銷_遠期",
            m."主力_月結算狀態",
            m."主力新建多單",
            m."主力新建空單",
            m."主力淨多空和_5日",
            m."主力淨多空和_10日",
            m."主力淨多空和_20日",
            m."主力淨多空和_60日",
            m."夜盤開",
            m."夜盤收",
            m."夜盤高",
            m."夜盤低",
            m."夜5MA",
            m."夜成交量",
            m."夜法人多單量",
            m."夜法人空單量",
            m."全市場OI_小台",
            m."三大法人多單_小台",
            m."三大法人空單_小台",
            m."散戶多單_小台",
            m."散戶空單_小台",
            m."散戶多空比_小台",
            m."全市場OI_微台",
            m."三大法人多單_微台",
            m."三大法人空單_微台",
            m."散戶多單_微台",
            m."散戶空單_微台",
            m."散戶多空比_微台",
            m."法人總買超_億",
            m."call_月選",
            m."put_月選",
            m."買賣權比_月選",
            m."call_週三",
            m."put_週三",
            m."買賣權比_週三",
            m."call_週五",
            m."put_週五",
            m."買賣權比_週五",
            m."日位置",
            m."策略_價量昨日",
            m."策略_價量五日",
            m."日MA方向_5",
            m."日MA方向_10",
            m."日MA方向_20",
            m."策略_線型",
            m."法人籌碼",
            m."散戶籌碼",
            m."加權籌碼",
            m."期貨籌碼",
            m."選權籌碼",
            m."策略_籌碼",
            m."策略_價量昨日_線型",
            m."策略_價量五日_線型",
            m."策略_價量昨日_籌碼",
            m."策略_價量五日_籌碼",
            m."策略_線型_籌碼",
            m."策略_核心",
            m."建立時間",
            m."_昨全日收",
            m."_昨全5MA",
            m."_昨全10MA",
            m."_昨全20MA",
            m."_全布林標準差",
                CASE
                    WHEN (m."全日收" > m."_昨全日收") THEN '價漲'::text
                    WHEN (m."全日收" < m."_昨全日收") THEN '價跌'::text
                    WHEN (m."全日收" = m."_昨全日收") THEN '價同'::text
                    ELSE NULL::text
                END AS "_全日價漲跌",
                CASE
                    WHEN (m."日量增減_昨日_pct" > (0)::numeric) THEN '量增'::text
                    WHEN (m."日量增減_昨日_pct" < (0)::numeric) THEN '量縮'::text
                    WHEN (m."日量增減_昨日_pct" = (0)::numeric) THEN '量同'::text
                    ELSE NULL::text
                END AS "日量增減_昨日",
                CASE
                    WHEN (m."日量增減_五日_pct" > (0)::numeric) THEN '量增'::text
                    WHEN (m."日量增減_五日_pct" < (0)::numeric) THEN '量縮'::text
                    WHEN (m."日量增減_五日_pct" = (0)::numeric) THEN '量同'::text
                    ELSE NULL::text
                END AS "日量增減_五日",
                CASE
                    WHEN (m."全5MA" > m."_昨全5MA") THEN '上'::text
                    WHEN (m."全5MA" < m."_昨全5MA") THEN '下'::text
                    ELSE NULL::text
                END AS "_新日MA方向_5",
                CASE
                    WHEN (m."全10MA" > m."_昨全10MA") THEN '上'::text
                    WHEN (m."全10MA" < m."_昨全10MA") THEN '下'::text
                    ELSE NULL::text
                END AS "_新日MA方向_10",
                CASE
                    WHEN (m."全20MA" > m."_昨全20MA") THEN '上'::text
                    WHEN (m."全20MA" < m."_昨全20MA") THEN '下'::text
                    ELSE NULL::text
                END AS "_新日MA方向_20",
                CASE
                    WHEN ((m."全5MA" > m."_昨全5MA") AND (m."全10MA" > m."_昨全10MA") AND (m."全20MA" > m."_昨全20MA")) THEN '多'::text
                    WHEN ((m."全5MA" > m."_昨全5MA") AND (m."全10MA" > m."_昨全10MA") AND (m."全20MA" < m."_昨全20MA")) THEN '多'::text
                    WHEN ((m."全5MA" > m."_昨全5MA") AND (m."全10MA" < m."_昨全10MA") AND (m."全20MA" > m."_昨全20MA")) THEN '盤整'::text
                    WHEN ((m."全5MA" > m."_昨全5MA") AND (m."全10MA" < m."_昨全10MA") AND (m."全20MA" < m."_昨全20MA")) THEN '空'::text
                    WHEN ((m."全5MA" < m."_昨全5MA") AND (m."全10MA" > m."_昨全10MA") AND (m."全20MA" > m."_昨全20MA")) THEN '多'::text
                    WHEN ((m."全5MA" < m."_昨全5MA") AND (m."全10MA" > m."_昨全10MA") AND (m."全20MA" < m."_昨全20MA")) THEN '盤整'::text
                    WHEN ((m."全5MA" < m."_昨全5MA") AND (m."全10MA" < m."_昨全10MA") AND (m."全20MA" > m."_昨全20MA")) THEN '空'::text
                    WHEN ((m."全5MA" < m."_昨全5MA") AND (m."全10MA" < m."_昨全10MA") AND (m."全20MA" < m."_昨全20MA")) THEN '空'::text
                    ELSE NULL::text
                END AS "_新策略_線型"
           FROM full_metrics m
        ), full_position AS (
         SELECT s.id,
            s.futures_id,
            s."日期",
            s."年",
            s."月",
            s."星期",
            s."週次",
            s."天別",
            s."全日開",
            s."全日收",
            s."全日高",
            s."全日低",
            s."全5MA",
            s."全10MA",
            s."全20MA",
            s."全60MA",
            s."全120MA",
            s."日盤開",
            s."日盤收",
            s."日盤高",
            s."日盤低",
            s."昨日收盤",
            s."日漲跌",
            s."日價漲跌_pct",
            s."日收減日開",
            s."日收開_昨收_pct",
            s."日收開_開盤_pct",
            s."日振幅",
            s."日振幅_昨收_pct",
            s."日振幅_開盤_pct",
            s."日成交量",
            s."日5最大量_日期",
            s."日5最大量",
            s."日10最大量_日期",
            s."日10最大量",
            s."日20最大量_日期",
            s."日20最大量",
            s."日60最大量_日期",
            s."日60最大量",
            s."日5均量",
            s."日10均量",
            s."日20均量",
            s."日60均量",
            s."日量增減_昨日_pct",
            s."日量增減_五日_pct",
            s."日量增減_十日_pct",
            s."日量增減_二十日_pct",
            s."日量增減_六十日_pct",
            s."日當日價量",
            s."日5日價量",
            s."日10日價量",
            s."日20日價量",
            s."日60日價量",
            s."K線型態",
            s."跳空缺口",
            s."跳空缺口_昨收_pct",
            s."實體_pct",
            s."上影_pct",
            s."下影_pct",
            s."日5MA",
            s."日10MA",
            s."日20MA",
            s."日60MA",
            s."日120MA",
            s."日240MA",
            s."日5均上升幅度_點",
            s."日10均上升幅度_點",
            s."日20均上升幅度_點",
            s."日60均上升幅度_點",
            s."日5均上升幅度_pct",
            s."日10均上升幅度_pct",
            s."日20均上升幅度_pct",
            s."日60均上升幅度_pct",
            s."日5_明日扣抵值",
            s."日10_明日扣抵值",
            s."日20_明日扣抵值",
            s."日60_明日扣抵值",
            s."日5_明日扣抵值影響_點",
            s."日10_明日扣抵值影響_點",
            s."日20_明日扣抵值影響_點",
            s."日60_明日扣抵值影響_點",
            s."日5_明日扣抵值影響_pct",
            s."日10_明日扣抵值影響_pct",
            s."日20_明日扣抵值影響_pct",
            s."日60_明日扣抵值影響_pct",
            s."日5MA乖離率",
            s."日10MA乖離率",
            s."日20MA乖離率",
            s."日60MA乖離率",
            s."日布林標準差",
            s."日布林上軌",
            s."日布林下軌",
            s."日法人多倉",
            s."日法人空倉",
            s."未沖銷_全",
            s."未沖銷_近期",
            s."未沖銷_遠期",
            s."主力_月結算狀態",
            s."主力新建多單",
            s."主力新建空單",
            s."主力淨多空和_5日",
            s."主力淨多空和_10日",
            s."主力淨多空和_20日",
            s."主力淨多空和_60日",
            s."夜盤開",
            s."夜盤收",
            s."夜盤高",
            s."夜盤低",
            s."夜5MA",
            s."夜成交量",
            s."夜法人多單量",
            s."夜法人空單量",
            s."全市場OI_小台",
            s."三大法人多單_小台",
            s."三大法人空單_小台",
            s."散戶多單_小台",
            s."散戶空單_小台",
            s."散戶多空比_小台",
            s."全市場OI_微台",
            s."三大法人多單_微台",
            s."三大法人空單_微台",
            s."散戶多單_微台",
            s."散戶空單_微台",
            s."散戶多空比_微台",
            s."法人總買超_億",
            s."call_月選",
            s."put_月選",
            s."買賣權比_月選",
            s."call_週三",
            s."put_週三",
            s."買賣權比_週三",
            s."call_週五",
            s."put_週五",
            s."買賣權比_週五",
            s."日位置",
            s."策略_價量昨日",
            s."策略_價量五日",
            s."日MA方向_5",
            s."日MA方向_10",
            s."日MA方向_20",
            s."策略_線型",
            s."法人籌碼",
            s."散戶籌碼",
            s."加權籌碼",
            s."期貨籌碼",
            s."選權籌碼",
            s."策略_籌碼",
            s."策略_價量昨日_線型",
            s."策略_價量五日_線型",
            s."策略_價量昨日_籌碼",
            s."策略_價量五日_籌碼",
            s."策略_線型_籌碼",
            s."策略_核心",
            s."建立時間",
            s."_昨全日收",
            s."_昨全5MA",
            s."_昨全10MA",
            s."_昨全20MA",
            s."_全布林標準差",
            s."_全日價漲跌",
            s."日量增減_昨日",
            s."日量增減_五日",
            s."_新日MA方向_5",
            s."_新日MA方向_10",
            s."_新日MA方向_20",
            s."_新策略_線型",
                CASE
                    WHEN ((s."_新策略_線型" = '多'::text) AND (((s."全日收" - s."全5MA") / NULLIF(s."全5MA", (0)::numeric)) > 0.02)) THEN '高檔'::text
                    WHEN ((s."_新策略_線型" = '多'::text) AND (((s."全日收" - s."全5MA") / NULLIF(s."全5MA", (0)::numeric)) < 0.02)) THEN '低檔'::text
                    WHEN ((s."_新策略_線型" = '空'::text) AND (((s."全日收" - s."全5MA") / NULLIF(s."全5MA", (0)::numeric)) > '-0.02'::numeric)) THEN '高檔'::text
                    WHEN ((s."_新策略_線型" = '空'::text) AND (((s."全日收" - s."全5MA") / NULLIF(s."全5MA", (0)::numeric)) < '-0.02'::numeric)) THEN '低檔'::text
                    WHEN ((s."_新策略_線型" = '盤整'::text) AND (s."全20MA" IS NOT NULL) AND (s."_全布林標準差" IS NOT NULL) AND (s."全日收" >= ((s."全20MA" + ((2)::numeric * s."_全布林標準差")) - (((s."全20MA" + ((2)::numeric * s."_全布林標準差")) - (s."全20MA" - ((2)::numeric * s."_全布林標準差"))) * 0.1)))) THEN '高檔'::text
                    WHEN ((s."_新策略_線型" = '盤整'::text) AND (s."全20MA" IS NOT NULL) AND (s."_全布林標準差" IS NOT NULL) AND (s."全日收" <= ((s."全20MA" - ((2)::numeric * s."_全布林標準差")) + (((s."全20MA" + ((2)::numeric * s."_全布林標準差")) - (s."全20MA" - ((2)::numeric * s."_全布林標準差"))) * 0.1)))) THEN '低檔'::text
                    ELSE '中間'::text
                END AS "_新日位置"
           FROM full_state s
        ), price_volume AS (
         SELECT p.id,
            p.futures_id,
            p."日期",
            p."年",
            p."月",
            p."星期",
            p."週次",
            p."天別",
            p."全日開",
            p."全日收",
            p."全日高",
            p."全日低",
            p."全5MA",
            p."全10MA",
            p."全20MA",
            p."全60MA",
            p."全120MA",
            p."日盤開",
            p."日盤收",
            p."日盤高",
            p."日盤低",
            p."昨日收盤",
            p."日漲跌",
            p."日價漲跌_pct",
            p."日收減日開",
            p."日收開_昨收_pct",
            p."日收開_開盤_pct",
            p."日振幅",
            p."日振幅_昨收_pct",
            p."日振幅_開盤_pct",
            p."日成交量",
            p."日5最大量_日期",
            p."日5最大量",
            p."日10最大量_日期",
            p."日10最大量",
            p."日20最大量_日期",
            p."日20最大量",
            p."日60最大量_日期",
            p."日60最大量",
            p."日5均量",
            p."日10均量",
            p."日20均量",
            p."日60均量",
            p."日量增減_昨日_pct",
            p."日量增減_五日_pct",
            p."日量增減_十日_pct",
            p."日量增減_二十日_pct",
            p."日量增減_六十日_pct",
            p."日當日價量",
            p."日5日價量",
            p."日10日價量",
            p."日20日價量",
            p."日60日價量",
            p."K線型態",
            p."跳空缺口",
            p."跳空缺口_昨收_pct",
            p."實體_pct",
            p."上影_pct",
            p."下影_pct",
            p."日5MA",
            p."日10MA",
            p."日20MA",
            p."日60MA",
            p."日120MA",
            p."日240MA",
            p."日5均上升幅度_點",
            p."日10均上升幅度_點",
            p."日20均上升幅度_點",
            p."日60均上升幅度_點",
            p."日5均上升幅度_pct",
            p."日10均上升幅度_pct",
            p."日20均上升幅度_pct",
            p."日60均上升幅度_pct",
            p."日5_明日扣抵值",
            p."日10_明日扣抵值",
            p."日20_明日扣抵值",
            p."日60_明日扣抵值",
            p."日5_明日扣抵值影響_點",
            p."日10_明日扣抵值影響_點",
            p."日20_明日扣抵值影響_點",
            p."日60_明日扣抵值影響_點",
            p."日5_明日扣抵值影響_pct",
            p."日10_明日扣抵值影響_pct",
            p."日20_明日扣抵值影響_pct",
            p."日60_明日扣抵值影響_pct",
            p."日5MA乖離率",
            p."日10MA乖離率",
            p."日20MA乖離率",
            p."日60MA乖離率",
            p."日布林標準差",
            p."日布林上軌",
            p."日布林下軌",
            p."日法人多倉",
            p."日法人空倉",
            p."未沖銷_全",
            p."未沖銷_近期",
            p."未沖銷_遠期",
            p."主力_月結算狀態",
            p."主力新建多單",
            p."主力新建空單",
            p."主力淨多空和_5日",
            p."主力淨多空和_10日",
            p."主力淨多空和_20日",
            p."主力淨多空和_60日",
            p."夜盤開",
            p."夜盤收",
            p."夜盤高",
            p."夜盤低",
            p."夜5MA",
            p."夜成交量",
            p."夜法人多單量",
            p."夜法人空單量",
            p."全市場OI_小台",
            p."三大法人多單_小台",
            p."三大法人空單_小台",
            p."散戶多單_小台",
            p."散戶空單_小台",
            p."散戶多空比_小台",
            p."全市場OI_微台",
            p."三大法人多單_微台",
            p."三大法人空單_微台",
            p."散戶多單_微台",
            p."散戶空單_微台",
            p."散戶多空比_微台",
            p."法人總買超_億",
            p."call_月選",
            p."put_月選",
            p."買賣權比_月選",
            p."call_週三",
            p."put_週三",
            p."買賣權比_週三",
            p."call_週五",
            p."put_週五",
            p."買賣權比_週五",
            p."日位置",
            p."策略_價量昨日",
            p."策略_價量五日",
            p."日MA方向_5",
            p."日MA方向_10",
            p."日MA方向_20",
            p."策略_線型",
            p."法人籌碼",
            p."散戶籌碼",
            p."加權籌碼",
            p."期貨籌碼",
            p."選權籌碼",
            p."策略_籌碼",
            p."策略_價量昨日_線型",
            p."策略_價量五日_線型",
            p."策略_價量昨日_籌碼",
            p."策略_價量五日_籌碼",
            p."策略_線型_籌碼",
            p."策略_核心",
            p."建立時間",
            p."_昨全日收",
            p."_昨全5MA",
            p."_昨全10MA",
            p."_昨全20MA",
            p."_全布林標準差",
            p."_全日價漲跌",
            p."日量增減_昨日",
            p."日量增減_五日",
            p."_新日MA方向_5",
            p."_新日MA方向_10",
            p."_新日MA方向_20",
            p."_新策略_線型",
            p."_新日位置",
                CASE
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_昨日" = '量增'::text) AND (p."_新策略_線型" = '多'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_昨日" = '量增'::text) AND (p."_新策略_線型" = '盤整'::text) AND (p."_新日位置" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_昨日" = '量增'::text) AND (p."_新策略_線型" = '盤整'::text) AND (p."_新日位置" = '高檔'::text)) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_昨日" = '量增'::text) AND (p."_新策略_線型" = '空'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_昨日" = '量縮'::text) AND (p."_新策略_線型" = '多'::text) AND (p."_新日位置" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_昨日" = '量縮'::text) AND (p."_新策略_線型" = '多'::text) AND (p."_新日位置" = '高檔'::text)) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_昨日" = '量縮'::text) AND (p."_新策略_線型" = ANY (ARRAY['盤整'::text, '空'::text]))) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_昨日" = '量增'::text) AND (p."_新策略_線型" = '多'::text) AND (p."_新日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_昨日" = '量增'::text) AND (p."_新策略_線型" = '多'::text) AND (p."_新日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_昨日" = '量增'::text) AND (p."_新策略_線型" = '盤整'::text) AND (p."_新日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_昨日" = '量增'::text) AND (p."_新策略_線型" = '盤整'::text) AND (p."_新日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_昨日" = '量增'::text) AND (p."_新策略_線型" = '空'::text)) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_昨日" = '量縮'::text) AND (p."_新策略_線型" = '多'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_昨日" = '量縮'::text) AND (p."_新策略_線型" = '盤整'::text) AND (p."_新日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_昨日" = '量縮'::text) AND (p."_新策略_線型" = '盤整'::text) AND (p."_新日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_昨日" = '量縮'::text) AND (p."_新策略_線型" = '空'::text) AND (p."_新日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_昨日" = '量縮'::text) AND (p."_新策略_線型" = '空'::text) AND (p."_新日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    ELSE NULL::text
                END AS "_新策略_價量昨日",
                CASE
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_五日" = '量增'::text) AND (p."_新策略_線型" = '多'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_五日" = '量增'::text) AND (p."_新策略_線型" = '盤整'::text) AND (p."_新日位置" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_五日" = '量增'::text) AND (p."_新策略_線型" = '盤整'::text) AND (p."_新日位置" = '高檔'::text)) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_五日" = '量增'::text) AND (p."_新策略_線型" = '空'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_五日" = '量縮'::text) AND (p."_新策略_線型" = '多'::text) AND (p."_新日位置" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_五日" = '量縮'::text) AND (p."_新策略_線型" = '多'::text) AND (p."_新日位置" = '高檔'::text)) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價漲'::text) AND (p."日量增減_五日" = '量縮'::text) AND (p."_新策略_線型" = ANY (ARRAY['盤整'::text, '空'::text]))) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_五日" = '量增'::text) AND (p."_新策略_線型" = '多'::text) AND (p."_新日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_五日" = '量增'::text) AND (p."_新策略_線型" = '多'::text) AND (p."_新日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_五日" = '量增'::text) AND (p."_新策略_線型" = '盤整'::text) AND (p."_新日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_五日" = '量增'::text) AND (p."_新策略_線型" = '盤整'::text) AND (p."_新日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_五日" = '量增'::text) AND (p."_新策略_線型" = '空'::text)) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_五日" = '量縮'::text) AND (p."_新策略_線型" = '多'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_五日" = '量縮'::text) AND (p."_新策略_線型" = '盤整'::text) AND (p."_新日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_五日" = '量縮'::text) AND (p."_新策略_線型" = '盤整'::text) AND (p."_新日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_五日" = '量縮'::text) AND (p."_新策略_線型" = '空'::text) AND (p."_新日位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_全日價漲跌" = '價跌'::text) AND (p."日量增減_五日" = '量縮'::text) AND (p."_新策略_線型" = '空'::text) AND (p."_新日位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    ELSE NULL::text
                END AS "_新策略_價量五日"
           FROM full_position p
        ), combined AS (
         SELECT v.id,
            v.futures_id,
            v."日期",
            v."年",
            v."月",
            v."星期",
            v."週次",
            v."天別",
            v."全日開",
            v."全日收",
            v."全日高",
            v."全日低",
            v."全5MA",
            v."全10MA",
            v."全20MA",
            v."全60MA",
            v."全120MA",
            v."日盤開",
            v."日盤收",
            v."日盤高",
            v."日盤低",
            v."昨日收盤",
            v."日漲跌",
            v."日價漲跌_pct",
            v."日收減日開",
            v."日收開_昨收_pct",
            v."日收開_開盤_pct",
            v."日振幅",
            v."日振幅_昨收_pct",
            v."日振幅_開盤_pct",
            v."日成交量",
            v."日5最大量_日期",
            v."日5最大量",
            v."日10最大量_日期",
            v."日10最大量",
            v."日20最大量_日期",
            v."日20最大量",
            v."日60最大量_日期",
            v."日60最大量",
            v."日5均量",
            v."日10均量",
            v."日20均量",
            v."日60均量",
            v."日量增減_昨日_pct",
            v."日量增減_五日_pct",
            v."日量增減_十日_pct",
            v."日量增減_二十日_pct",
            v."日量增減_六十日_pct",
            v."日當日價量",
            v."日5日價量",
            v."日10日價量",
            v."日20日價量",
            v."日60日價量",
            v."K線型態",
            v."跳空缺口",
            v."跳空缺口_昨收_pct",
            v."實體_pct",
            v."上影_pct",
            v."下影_pct",
            v."日5MA",
            v."日10MA",
            v."日20MA",
            v."日60MA",
            v."日120MA",
            v."日240MA",
            v."日5均上升幅度_點",
            v."日10均上升幅度_點",
            v."日20均上升幅度_點",
            v."日60均上升幅度_點",
            v."日5均上升幅度_pct",
            v."日10均上升幅度_pct",
            v."日20均上升幅度_pct",
            v."日60均上升幅度_pct",
            v."日5_明日扣抵值",
            v."日10_明日扣抵值",
            v."日20_明日扣抵值",
            v."日60_明日扣抵值",
            v."日5_明日扣抵值影響_點",
            v."日10_明日扣抵值影響_點",
            v."日20_明日扣抵值影響_點",
            v."日60_明日扣抵值影響_點",
            v."日5_明日扣抵值影響_pct",
            v."日10_明日扣抵值影響_pct",
            v."日20_明日扣抵值影響_pct",
            v."日60_明日扣抵值影響_pct",
            v."日5MA乖離率",
            v."日10MA乖離率",
            v."日20MA乖離率",
            v."日60MA乖離率",
            v."日布林標準差",
            v."日布林上軌",
            v."日布林下軌",
            v."日法人多倉",
            v."日法人空倉",
            v."未沖銷_全",
            v."未沖銷_近期",
            v."未沖銷_遠期",
            v."主力_月結算狀態",
            v."主力新建多單",
            v."主力新建空單",
            v."主力淨多空和_5日",
            v."主力淨多空和_10日",
            v."主力淨多空和_20日",
            v."主力淨多空和_60日",
            v."夜盤開",
            v."夜盤收",
            v."夜盤高",
            v."夜盤低",
            v."夜5MA",
            v."夜成交量",
            v."夜法人多單量",
            v."夜法人空單量",
            v."全市場OI_小台",
            v."三大法人多單_小台",
            v."三大法人空單_小台",
            v."散戶多單_小台",
            v."散戶空單_小台",
            v."散戶多空比_小台",
            v."全市場OI_微台",
            v."三大法人多單_微台",
            v."三大法人空單_微台",
            v."散戶多單_微台",
            v."散戶空單_微台",
            v."散戶多空比_微台",
            v."法人總買超_億",
            v."call_月選",
            v."put_月選",
            v."買賣權比_月選",
            v."call_週三",
            v."put_週三",
            v."買賣權比_週三",
            v."call_週五",
            v."put_週五",
            v."買賣權比_週五",
            v."日位置",
            v."策略_價量昨日",
            v."策略_價量五日",
            v."日MA方向_5",
            v."日MA方向_10",
            v."日MA方向_20",
            v."策略_線型",
            v."法人籌碼",
            v."散戶籌碼",
            v."加權籌碼",
            v."期貨籌碼",
            v."選權籌碼",
            v."策略_籌碼",
            v."策略_價量昨日_線型",
            v."策略_價量五日_線型",
            v."策略_價量昨日_籌碼",
            v."策略_價量五日_籌碼",
            v."策略_線型_籌碼",
            v."策略_核心",
            v."建立時間",
            v."_昨全日收",
            v."_昨全5MA",
            v."_昨全10MA",
            v."_昨全20MA",
            v."_全布林標準差",
            v."_全日價漲跌",
            v."日量增減_昨日",
            v."日量增減_五日",
            v."_新日MA方向_5",
            v."_新日MA方向_10",
            v."_新日MA方向_20",
            v."_新策略_線型",
            v."_新日位置",
            v."_新策略_價量昨日",
            v."_新策略_價量五日",
                CASE
                    WHEN ((v."_新策略_價量昨日" = '多'::text) AND (v."_新策略_線型" = '多'::text)) THEN '多'::text
                    WHEN ((v."_新策略_價量昨日" = '空'::text) AND (v."_新策略_線型" = '空'::text)) THEN '空'::text
                    WHEN ((v."_新策略_價量昨日" = ANY (ARRAY['多'::text, '空'::text])) AND (v."_新策略_線型" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
                    ELSE '無'::text
                END AS "_新策略_價量昨日_線型",
                CASE
                    WHEN ((v."_新策略_價量五日" = '多'::text) AND (v."_新策略_線型" = '多'::text)) THEN '多'::text
                    WHEN ((v."_新策略_價量五日" = '空'::text) AND (v."_新策略_線型" = '空'::text)) THEN '空'::text
                    WHEN ((v."_新策略_價量五日" = ANY (ARRAY['多'::text, '空'::text])) AND (v."_新策略_線型" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
                    ELSE '無'::text
                END AS "_新策略_價量五日_線型",
                CASE
                    WHEN ((v."_新策略_價量昨日" = '多'::text) AND (v."策略_籌碼" = '多'::text)) THEN '多'::text
                    WHEN ((v."_新策略_價量昨日" = '空'::text) AND (v."策略_籌碼" = '空'::text)) THEN '空'::text
                    WHEN ((v."_新策略_價量昨日" = ANY (ARRAY['多'::text, '空'::text])) AND (v."策略_籌碼" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
                    ELSE '無'::text
                END AS "_新策略_價量昨日_籌碼",
                CASE
                    WHEN ((v."_新策略_價量五日" = '多'::text) AND (v."策略_籌碼" = '多'::text)) THEN '多'::text
                    WHEN ((v."_新策略_價量五日" = '空'::text) AND (v."策略_籌碼" = '空'::text)) THEN '空'::text
                    WHEN ((v."_新策略_價量五日" = ANY (ARRAY['多'::text, '空'::text])) AND (v."策略_籌碼" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
                    ELSE '無'::text
                END AS "_新策略_價量五日_籌碼",
                CASE
                    WHEN ((v."_新策略_線型" = '多'::text) AND (v."策略_籌碼" = '多'::text)) THEN '多'::text
                    WHEN ((v."_新策略_線型" = '空'::text) AND (v."策略_籌碼" = '空'::text)) THEN '空'::text
                    WHEN ((v."_新策略_線型" = ANY (ARRAY['多'::text, '空'::text])) AND (v."策略_籌碼" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
                    ELSE '無'::text
                END AS "_新策略_線型_籌碼",
                CASE
                    WHEN (v."_新策略_價量五日" = '多'::text) THEN
                    CASE
                        WHEN (v."_新策略_線型" = '多'::text) THEN '多'::text
                        WHEN (v."_新策略_線型" = '空'::text) THEN
                        CASE
                            WHEN (v."策略_籌碼" = '多'::text) THEN '多'::text
                            ELSE '空'::text
                        END
                        ELSE '多'::text
                    END
                    WHEN (v."_新策略_價量五日" = '空'::text) THEN
                    CASE
                        WHEN (v."_新策略_線型" = '多'::text) THEN
                        CASE
                            WHEN (v."策略_籌碼" = '空'::text) THEN '空'::text
                            ELSE '多'::text
                        END
                        ELSE '空'::text
                    END
                    WHEN (v."_新策略_價量五日" = '無'::text) THEN
                    CASE
                        WHEN (v."_新策略_線型" = '多'::text) THEN '多'::text
                        WHEN (v."_新策略_線型" = '空'::text) THEN '空'::text
                        WHEN (v."_新策略_線型" = '無'::text) THEN
                        CASE
                            WHEN (v."策略_籌碼" = '多'::text) THEN '多'::text
                            WHEN (v."策略_籌碼" = '空'::text) THEN '空'::text
                            ELSE '無'::text
                        END
                        ELSE '無'::text
                    END
                    ELSE '無'::text
                END AS "_新策略_核心"
           FROM price_volume v
        )
 SELECT id,
    futures_id,
    "日期",
    "年",
    "月",
    "星期",
    "週次",
    "天別",
    "全日開",
    "全日收",
    "全日高",
    "全日低",
    "全5MA",
    "全10MA",
    "全20MA",
    "全60MA",
    "全120MA",
    "日盤開",
    "日盤收",
    "日盤高",
    "日盤低",
    "昨日收盤",
    "日漲跌",
    "日價漲跌_pct",
    "日收減日開",
    "日收開_昨收_pct",
    "日收開_開盤_pct",
    "日振幅",
    "日振幅_昨收_pct",
    "日振幅_開盤_pct",
    "日成交量",
    "日5最大量_日期",
    "日5最大量",
    "日10最大量_日期",
    "日10最大量",
    "日20最大量_日期",
    "日20最大量",
    "日60最大量_日期",
    "日60最大量",
    "日5均量",
    "日10均量",
    "日20均量",
    "日60均量",
    "日量增減_昨日_pct",
    "日量增減_五日_pct",
    "日量增減_十日_pct",
    "日量增減_二十日_pct",
    "日量增減_六十日_pct",
    "日當日價量",
    "日5日價量",
    "日10日價量",
    "日20日價量",
    "日60日價量",
    "K線型態",
    "跳空缺口",
    "跳空缺口_昨收_pct",
    "實體_pct",
    "上影_pct",
    "下影_pct",
    "日5MA",
    "日10MA",
    "日20MA",
    "日60MA",
    "日120MA",
    "日240MA",
    "日5均上升幅度_點",
    "日10均上升幅度_點",
    "日20均上升幅度_點",
    "日60均上升幅度_點",
    "日5均上升幅度_pct",
    "日10均上升幅度_pct",
    "日20均上升幅度_pct",
    "日60均上升幅度_pct",
    "日5_明日扣抵值",
    "日10_明日扣抵值",
    "日20_明日扣抵值",
    "日60_明日扣抵值",
    "日5_明日扣抵值影響_點",
    "日10_明日扣抵值影響_點",
    "日20_明日扣抵值影響_點",
    "日60_明日扣抵值影響_點",
    "日5_明日扣抵值影響_pct",
    "日10_明日扣抵值影響_pct",
    "日20_明日扣抵值影響_pct",
    "日60_明日扣抵值影響_pct",
    "日5MA乖離率",
    "日10MA乖離率",
    "日20MA乖離率",
    "日60MA乖離率",
    "日布林標準差",
    "日布林上軌",
    "日布林下軌",
    "日法人多倉",
    "日法人空倉",
    "未沖銷_全",
    "未沖銷_近期",
    "未沖銷_遠期",
    "主力_月結算狀態",
    "主力新建多單",
    "主力新建空單",
    "主力淨多空和_5日",
    "主力淨多空和_10日",
    "主力淨多空和_20日",
    "主力淨多空和_60日",
    "夜盤開",
    "夜盤收",
    "夜盤高",
    "夜盤低",
    "夜5MA",
    "夜成交量",
    "夜法人多單量",
    "夜法人空單量",
    "全市場OI_小台",
    "三大法人多單_小台",
    "三大法人空單_小台",
    "散戶多單_小台",
    "散戶空單_小台",
    "散戶多空比_小台",
    "全市場OI_微台",
    "三大法人多單_微台",
    "三大法人空單_微台",
    "散戶多單_微台",
    "散戶空單_微台",
    "散戶多空比_微台",
    "法人總買超_億",
    "call_月選",
    "put_月選",
    "買賣權比_月選",
    "call_週三",
    "put_週三",
    "買賣權比_週三",
    "call_週五",
    "put_週五",
    "買賣權比_週五",
    "_新日位置" AS "日位置",
    "_新策略_價量昨日" AS "策略_價量昨日",
    "_新策略_價量五日" AS "策略_價量五日",
    "_新日MA方向_5" AS "日MA方向_5",
    "_新日MA方向_10" AS "日MA方向_10",
    "_新日MA方向_20" AS "日MA方向_20",
    "_新策略_線型" AS "策略_線型",
    "法人籌碼",
    "散戶籌碼",
    "加權籌碼",
    "期貨籌碼",
    "選權籌碼",
    "策略_籌碼",
    "_新策略_價量昨日_線型" AS "策略_價量昨日_線型",
    "_新策略_價量五日_線型" AS "策略_價量五日_線型",
    "_新策略_價量昨日_籌碼" AS "策略_價量昨日_籌碼",
    "_新策略_價量五日_籌碼" AS "策略_價量五日_籌碼",
    "_新策略_線型_籌碼" AS "策略_線型_籌碼",
    "_新策略_核心" AS "策略_核心",
    "建立時間"
   FROM combined z;


--
-- Name: v_futures_report_daily_before_full_technical; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_report_daily_before_full_technical AS
 WITH full_base AS (
         SELECT r.id,
            r.futures_id,
            r."日期",
            r."年",
            r."月",
            r."星期",
            r."週次",
            r."天別",
            r."全日開",
            r."全日收",
            r."全日高",
            r."全日低",
            r."全5MA",
            r."全10MA",
            r."全20MA",
            r."全60MA",
            r."全120MA",
            r."日盤開",
            r."日盤收",
            r."日盤高",
            r."日盤低",
            r."昨日收盤",
            r."日漲跌",
            r."日價漲跌_pct",
            r."日收減日開",
            r."日收開_昨收_pct",
            r."日收開_開盤_pct",
            r."日振幅",
            r."日振幅_昨收_pct",
            r."日振幅_開盤_pct",
            r."日成交量",
            r."日5最大量_日期",
            r."日5最大量",
            r."日10最大量_日期",
            r."日10最大量",
            r."日20最大量_日期",
            r."日20最大量",
            r."日60最大量_日期",
            r."日60最大量",
            r."日5均量",
            r."日10均量",
            r."日20均量",
            r."日60均量",
            r."日量增減_昨日_pct",
            r."日量增減_五日_pct",
            r."日量增減_十日_pct",
            r."日量增減_二十日_pct",
            r."日量增減_六十日_pct",
            r."日當日價量",
            r."日5日價量",
            r."日10日價量",
            r."日20日價量",
            r."日60日價量",
            r."K線型態",
            r."跳空缺口",
            r."跳空缺口_昨收_pct",
            r."實體_pct",
            r."上影_pct",
            r."下影_pct",
            r."日5MA",
            r."日10MA",
            r."日20MA",
            r."日60MA",
            r."日120MA",
            r."日240MA",
            r."日5均上升幅度_點",
            r."日10均上升幅度_點",
            r."日20均上升幅度_點",
            r."日60均上升幅度_點",
            r."日5均上升幅度_pct",
            r."日10均上升幅度_pct",
            r."日20均上升幅度_pct",
            r."日60均上升幅度_pct",
            r."日5_明日扣抵值",
            r."日10_明日扣抵值",
            r."日20_明日扣抵值",
            r."日60_明日扣抵值",
            r."日5_明日扣抵值影響_點",
            r."日10_明日扣抵值影響_點",
            r."日20_明日扣抵值影響_點",
            r."日60_明日扣抵值影響_點",
            r."日5_明日扣抵值影響_pct",
            r."日10_明日扣抵值影響_pct",
            r."日20_明日扣抵值影響_pct",
            r."日60_明日扣抵值影響_pct",
            r."日5MA乖離率",
            r."日10MA乖離率",
            r."日20MA乖離率",
            r."日60MA乖離率",
            r."日布林標準差",
            r."日布林上軌",
            r."日布林下軌",
            r."日法人多倉",
            r."日法人空倉",
            r."未沖銷_全",
            r."未沖銷_近期",
            r."未沖銷_遠期",
            r."主力_月結算狀態",
            r."主力新建多單",
            r."主力新建空單",
            r."主力淨多空和_5日",
            r."主力淨多空和_10日",
            r."主力淨多空和_20日",
            r."主力淨多空和_60日",
            r."夜盤開",
            r."夜盤收",
            r."夜盤高",
            r."夜盤低",
            r."夜5MA",
            r."夜成交量",
            r."夜法人多單量",
            r."夜法人空單量",
            r."全市場OI_小台",
            r."三大法人多單_小台",
            r."三大法人空單_小台",
            r."散戶多單_小台",
            r."散戶空單_小台",
            r."散戶多空比_小台",
            r."全市場OI_微台",
            r."三大法人多單_微台",
            r."三大法人空單_微台",
            r."散戶多單_微台",
            r."散戶空單_微台",
            r."散戶多空比_微台",
            r."法人總買超_億",
            r."call_月選",
            r."put_月選",
            r."買賣權比_月選",
            r."call_週三",
            r."put_週三",
            r."買賣權比_週三",
            r."call_週五",
            r."put_週五",
            r."買賣權比_週五",
            r."日位置",
            r."策略_價量昨日",
            r."策略_價量五日",
            r."日MA方向_5",
            r."日MA方向_10",
            r."日MA方向_20",
            r."策略_線型",
            r."法人籌碼",
            r."散戶籌碼",
            r."加權籌碼",
            r."期貨籌碼",
            r."選權籌碼",
            r."策略_籌碼",
            r."策略_價量昨日_線型",
            r."策略_價量五日_線型",
            r."策略_價量昨日_籌碼",
            r."策略_價量五日_籌碼",
            r."策略_線型_籌碼",
            r."策略_核心",
            r."建立時間",
            lag(r."全日收") OVER w AS "全昨收",
            lag(r."全日開") OVER w AS "全昨开",
            lag(r."全日高") OVER w AS "全昨高",
            lag(r."全日低") OVER w AS "全昨低",
            lag(r."夜成交量") OVER w AS "所属夜成交量"
           FROM report.v_futures_report_daily_core r
          WINDOW w AS (PARTITION BY r.futures_id ORDER BY r."日期")
        ), full_volume AS (
         SELECT b.id,
            b.futures_id,
            b."日期",
            b."年",
            b."月",
            b."星期",
            b."週次",
            b."天別",
            b."全日開",
            b."全日收",
            b."全日高",
            b."全日低",
            b."全5MA",
            b."全10MA",
            b."全20MA",
            b."全60MA",
            b."全120MA",
            b."日盤開",
            b."日盤收",
            b."日盤高",
            b."日盤低",
            b."昨日收盤",
            b."日漲跌",
            b."日價漲跌_pct",
            b."日收減日開",
            b."日收開_昨收_pct",
            b."日收開_開盤_pct",
            b."日振幅",
            b."日振幅_昨收_pct",
            b."日振幅_開盤_pct",
            b."日成交量",
            b."日5最大量_日期",
            b."日5最大量",
            b."日10最大量_日期",
            b."日10最大量",
            b."日20最大量_日期",
            b."日20最大量",
            b."日60最大量_日期",
            b."日60最大量",
            b."日5均量",
            b."日10均量",
            b."日20均量",
            b."日60均量",
            b."日量增減_昨日_pct",
            b."日量增減_五日_pct",
            b."日量增減_十日_pct",
            b."日量增減_二十日_pct",
            b."日量增減_六十日_pct",
            b."日當日價量",
            b."日5日價量",
            b."日10日價量",
            b."日20日價量",
            b."日60日價量",
            b."K線型態",
            b."跳空缺口",
            b."跳空缺口_昨收_pct",
            b."實體_pct",
            b."上影_pct",
            b."下影_pct",
            b."日5MA",
            b."日10MA",
            b."日20MA",
            b."日60MA",
            b."日120MA",
            b."日240MA",
            b."日5均上升幅度_點",
            b."日10均上升幅度_點",
            b."日20均上升幅度_點",
            b."日60均上升幅度_點",
            b."日5均上升幅度_pct",
            b."日10均上升幅度_pct",
            b."日20均上升幅度_pct",
            b."日60均上升幅度_pct",
            b."日5_明日扣抵值",
            b."日10_明日扣抵值",
            b."日20_明日扣抵值",
            b."日60_明日扣抵值",
            b."日5_明日扣抵值影響_點",
            b."日10_明日扣抵值影響_點",
            b."日20_明日扣抵值影響_點",
            b."日60_明日扣抵值影響_點",
            b."日5_明日扣抵值影響_pct",
            b."日10_明日扣抵值影響_pct",
            b."日20_明日扣抵值影響_pct",
            b."日60_明日扣抵值影響_pct",
            b."日5MA乖離率",
            b."日10MA乖離率",
            b."日20MA乖離率",
            b."日60MA乖離率",
            b."日布林標準差",
            b."日布林上軌",
            b."日布林下軌",
            b."日法人多倉",
            b."日法人空倉",
            b."未沖銷_全",
            b."未沖銷_近期",
            b."未沖銷_遠期",
            b."主力_月結算狀態",
            b."主力新建多單",
            b."主力新建空單",
            b."主力淨多空和_5日",
            b."主力淨多空和_10日",
            b."主力淨多空和_20日",
            b."主力淨多空和_60日",
            b."夜盤開",
            b."夜盤收",
            b."夜盤高",
            b."夜盤低",
            b."夜5MA",
            b."夜成交量",
            b."夜法人多單量",
            b."夜法人空單量",
            b."全市場OI_小台",
            b."三大法人多單_小台",
            b."三大法人空單_小台",
            b."散戶多單_小台",
            b."散戶空單_小台",
            b."散戶多空比_小台",
            b."全市場OI_微台",
            b."三大法人多單_微台",
            b."三大法人空單_微台",
            b."散戶多單_微台",
            b."散戶空單_微台",
            b."散戶多空比_微台",
            b."法人總買超_億",
            b."call_月選",
            b."put_月選",
            b."買賣權比_月選",
            b."call_週三",
            b."put_週三",
            b."買賣權比_週三",
            b."call_週五",
            b."put_週五",
            b."買賣權比_週五",
            b."日位置",
            b."策略_價量昨日",
            b."策略_價量五日",
            b."日MA方向_5",
            b."日MA方向_10",
            b."日MA方向_20",
            b."策略_線型",
            b."法人籌碼",
            b."散戶籌碼",
            b."加權籌碼",
            b."期貨籌碼",
            b."選權籌碼",
            b."策略_籌碼",
            b."策略_價量昨日_線型",
            b."策略_價量五日_線型",
            b."策略_價量昨日_籌碼",
            b."策略_價量五日_籌碼",
            b."策略_線型_籌碼",
            b."策略_核心",
            b."建立時間",
            b."全昨收",
            b."全昨开",
            b."全昨高",
            b."全昨低",
            b."所属夜成交量",
                CASE
                    WHEN ((b."日成交量" IS NULL) AND (b."所属夜成交量" IS NULL)) THEN NULL::numeric
                    ELSE (COALESCE(b."日成交量", (0)::numeric) + COALESCE(b."所属夜成交量", (0)::numeric))
                END AS "全成交量_calc"
           FROM full_base b
        ), full_window AS (
         SELECT v.id,
            v.futures_id,
            v."日期",
            v."年",
            v."月",
            v."星期",
            v."週次",
            v."天別",
            v."全日開",
            v."全日收",
            v."全日高",
            v."全日低",
            v."全5MA",
            v."全10MA",
            v."全20MA",
            v."全60MA",
            v."全120MA",
            v."日盤開",
            v."日盤收",
            v."日盤高",
            v."日盤低",
            v."昨日收盤",
            v."日漲跌",
            v."日價漲跌_pct",
            v."日收減日開",
            v."日收開_昨收_pct",
            v."日收開_開盤_pct",
            v."日振幅",
            v."日振幅_昨收_pct",
            v."日振幅_開盤_pct",
            v."日成交量",
            v."日5最大量_日期",
            v."日5最大量",
            v."日10最大量_日期",
            v."日10最大量",
            v."日20最大量_日期",
            v."日20最大量",
            v."日60最大量_日期",
            v."日60最大量",
            v."日5均量",
            v."日10均量",
            v."日20均量",
            v."日60均量",
            v."日量增減_昨日_pct",
            v."日量增減_五日_pct",
            v."日量增減_十日_pct",
            v."日量增減_二十日_pct",
            v."日量增減_六十日_pct",
            v."日當日價量",
            v."日5日價量",
            v."日10日價量",
            v."日20日價量",
            v."日60日價量",
            v."K線型態",
            v."跳空缺口",
            v."跳空缺口_昨收_pct",
            v."實體_pct",
            v."上影_pct",
            v."下影_pct",
            v."日5MA",
            v."日10MA",
            v."日20MA",
            v."日60MA",
            v."日120MA",
            v."日240MA",
            v."日5均上升幅度_點",
            v."日10均上升幅度_點",
            v."日20均上升幅度_點",
            v."日60均上升幅度_點",
            v."日5均上升幅度_pct",
            v."日10均上升幅度_pct",
            v."日20均上升幅度_pct",
            v."日60均上升幅度_pct",
            v."日5_明日扣抵值",
            v."日10_明日扣抵值",
            v."日20_明日扣抵值",
            v."日60_明日扣抵值",
            v."日5_明日扣抵值影響_點",
            v."日10_明日扣抵值影響_點",
            v."日20_明日扣抵值影響_點",
            v."日60_明日扣抵值影響_點",
            v."日5_明日扣抵值影響_pct",
            v."日10_明日扣抵值影響_pct",
            v."日20_明日扣抵值影響_pct",
            v."日60_明日扣抵值影響_pct",
            v."日5MA乖離率",
            v."日10MA乖離率",
            v."日20MA乖離率",
            v."日60MA乖離率",
            v."日布林標準差",
            v."日布林上軌",
            v."日布林下軌",
            v."日法人多倉",
            v."日法人空倉",
            v."未沖銷_全",
            v."未沖銷_近期",
            v."未沖銷_遠期",
            v."主力_月結算狀態",
            v."主力新建多單",
            v."主力新建空單",
            v."主力淨多空和_5日",
            v."主力淨多空和_10日",
            v."主力淨多空和_20日",
            v."主力淨多空和_60日",
            v."夜盤開",
            v."夜盤收",
            v."夜盤高",
            v."夜盤低",
            v."夜5MA",
            v."夜成交量",
            v."夜法人多單量",
            v."夜法人空單量",
            v."全市場OI_小台",
            v."三大法人多單_小台",
            v."三大法人空單_小台",
            v."散戶多單_小台",
            v."散戶空單_小台",
            v."散戶多空比_小台",
            v."全市場OI_微台",
            v."三大法人多單_微台",
            v."三大法人空單_微台",
            v."散戶多單_微台",
            v."散戶空單_微台",
            v."散戶多空比_微台",
            v."法人總買超_億",
            v."call_月選",
            v."put_月選",
            v."買賣權比_月選",
            v."call_週三",
            v."put_週三",
            v."買賣權比_週三",
            v."call_週五",
            v."put_週五",
            v."買賣權比_週五",
            v."日位置",
            v."策略_價量昨日",
            v."策略_價量五日",
            v."日MA方向_5",
            v."日MA方向_10",
            v."日MA方向_20",
            v."策略_線型",
            v."法人籌碼",
            v."散戶籌碼",
            v."加權籌碼",
            v."期貨籌碼",
            v."選權籌碼",
            v."策略_籌碼",
            v."策略_價量昨日_線型",
            v."策略_價量五日_線型",
            v."策略_價量昨日_籌碼",
            v."策略_價量五日_籌碼",
            v."策略_線型_籌碼",
            v."策略_核心",
            v."建立時間",
            v."全昨收",
            v."全昨开",
            v."全昨高",
            v."全昨低",
            v."所属夜成交量",
            v."全成交量_calc",
            lag(v."全成交量_calc") OVER w AS "全昨日成交量",
            avg(v."全成交量_calc") OVER (PARTITION BY v.futures_id ORDER BY v."日期" ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS "全5均量_calc",
            avg(v."全成交量_calc") OVER (PARTITION BY v.futures_id ORDER BY v."日期" ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS "全10均量_calc",
            avg(v."全成交量_calc") OVER (PARTITION BY v.futures_id ORDER BY v."日期" ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS "全20均量_calc",
            avg(v."全成交量_calc") OVER (PARTITION BY v.futures_id ORDER BY v."日期" ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) AS "全60均量_calc",
            max(v."全成交量_calc") OVER (PARTITION BY v.futures_id ORDER BY v."日期" ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS "全5最大量_calc",
            max(v."全成交量_calc") OVER (PARTITION BY v.futures_id ORDER BY v."日期" ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS "全10最大量_calc",
            max(v."全成交量_calc") OVER (PARTITION BY v.futures_id ORDER BY v."日期" ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS "全20最大量_calc",
            max(v."全成交量_calc") OVER (PARTITION BY v.futures_id ORDER BY v."日期" ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) AS "全60最大量_calc"
           FROM full_volume v
          WINDOW w AS (PARTITION BY v.futures_id ORDER BY v."日期")
        ), full_metrics AS (
         SELECT w.id,
            w.futures_id,
            w."日期",
            w."年",
            w."月",
            w."星期",
            w."週次",
            w."天別",
            w."全日開",
            w."全日收",
            w."全日高",
            w."全日低",
            w."全5MA",
            w."全10MA",
            w."全20MA",
            w."全60MA",
            w."全120MA",
            w."日盤開",
            w."日盤收",
            w."日盤高",
            w."日盤低",
            w."昨日收盤",
            w."日漲跌",
            w."日價漲跌_pct",
            w."日收減日開",
            w."日收開_昨收_pct",
            w."日收開_開盤_pct",
            w."日振幅",
            w."日振幅_昨收_pct",
            w."日振幅_開盤_pct",
            w."日成交量",
            w."日5最大量_日期",
            w."日5最大量",
            w."日10最大量_日期",
            w."日10最大量",
            w."日20最大量_日期",
            w."日20最大量",
            w."日60最大量_日期",
            w."日60最大量",
            w."日5均量",
            w."日10均量",
            w."日20均量",
            w."日60均量",
            w."日量增減_昨日_pct",
            w."日量增減_五日_pct",
            w."日量增減_十日_pct",
            w."日量增減_二十日_pct",
            w."日量增減_六十日_pct",
            w."日當日價量",
            w."日5日價量",
            w."日10日價量",
            w."日20日價量",
            w."日60日價量",
            w."K線型態",
            w."跳空缺口",
            w."跳空缺口_昨收_pct",
            w."實體_pct",
            w."上影_pct",
            w."下影_pct",
            w."日5MA",
            w."日10MA",
            w."日20MA",
            w."日60MA",
            w."日120MA",
            w."日240MA",
            w."日5均上升幅度_點",
            w."日10均上升幅度_點",
            w."日20均上升幅度_點",
            w."日60均上升幅度_點",
            w."日5均上升幅度_pct",
            w."日10均上升幅度_pct",
            w."日20均上升幅度_pct",
            w."日60均上升幅度_pct",
            w."日5_明日扣抵值",
            w."日10_明日扣抵值",
            w."日20_明日扣抵值",
            w."日60_明日扣抵值",
            w."日5_明日扣抵值影響_點",
            w."日10_明日扣抵值影響_點",
            w."日20_明日扣抵值影響_點",
            w."日60_明日扣抵值影響_點",
            w."日5_明日扣抵值影響_pct",
            w."日10_明日扣抵值影響_pct",
            w."日20_明日扣抵值影響_pct",
            w."日60_明日扣抵值影響_pct",
            w."日5MA乖離率",
            w."日10MA乖離率",
            w."日20MA乖離率",
            w."日60MA乖離率",
            w."日布林標準差",
            w."日布林上軌",
            w."日布林下軌",
            w."日法人多倉",
            w."日法人空倉",
            w."未沖銷_全",
            w."未沖銷_近期",
            w."未沖銷_遠期",
            w."主力_月結算狀態",
            w."主力新建多單",
            w."主力新建空單",
            w."主力淨多空和_5日",
            w."主力淨多空和_10日",
            w."主力淨多空和_20日",
            w."主力淨多空和_60日",
            w."夜盤開",
            w."夜盤收",
            w."夜盤高",
            w."夜盤低",
            w."夜5MA",
            w."夜成交量",
            w."夜法人多單量",
            w."夜法人空單量",
            w."全市場OI_小台",
            w."三大法人多單_小台",
            w."三大法人空單_小台",
            w."散戶多單_小台",
            w."散戶空單_小台",
            w."散戶多空比_小台",
            w."全市場OI_微台",
            w."三大法人多單_微台",
            w."三大法人空單_微台",
            w."散戶多單_微台",
            w."散戶空單_微台",
            w."散戶多空比_微台",
            w."法人總買超_億",
            w."call_月選",
            w."put_月選",
            w."買賣權比_月選",
            w."call_週三",
            w."put_週三",
            w."買賣權比_週三",
            w."call_週五",
            w."put_週五",
            w."買賣權比_週五",
            w."日位置",
            w."策略_價量昨日",
            w."策略_價量五日",
            w."日MA方向_5",
            w."日MA方向_10",
            w."日MA方向_20",
            w."策略_線型",
            w."法人籌碼",
            w."散戶籌碼",
            w."加權籌碼",
            w."期貨籌碼",
            w."選權籌碼",
            w."策略_籌碼",
            w."策略_價量昨日_線型",
            w."策略_價量五日_線型",
            w."策略_價量昨日_籌碼",
            w."策略_價量五日_籌碼",
            w."策略_線型_籌碼",
            w."策略_核心",
            w."建立時間",
            w."全昨收",
            w."全昨开",
            w."全昨高",
            w."全昨低",
            w."所属夜成交量",
            w."全成交量_calc",
            w."全昨日成交量",
            w."全5均量_calc",
            w."全10均量_calc",
            w."全20均量_calc",
            w."全60均量_calc",
            w."全5最大量_calc",
            w."全10最大量_calc",
            w."全20最大量_calc",
            w."全60最大量_calc",
            (w."全日收" - w."全昨收") AS "全涨跌_calc",
            ((w."全日收" - w."全昨收") / NULLIF(w."全昨收", (0)::numeric)) AS "全价涨跌_pct_calc",
            (w."全日收" - w."全日開") AS "全收减全开_calc",
            ((w."全日收" - w."全日開") / NULLIF(w."全昨收", (0)::numeric)) AS "全收开_昨收_pct_calc",
            ((w."全日收" - w."全日開") / NULLIF(w."全日開", (0)::numeric)) AS "全收开_开盘_pct_calc",
            (w."全日高" - w."全日低") AS "全振幅_calc",
            ((w."全日高" - w."全日低") / NULLIF(w."全昨收", (0)::numeric)) AS "全振幅_昨收_pct_calc",
            ((w."全日高" - w."全日低") / NULLIF(w."全日開", (0)::numeric)) AS "全振幅_开盘_pct_calc",
            ((w."全成交量_calc" - w."全昨日成交量") / NULLIF(w."全昨日成交量", (0)::numeric)) AS "全量增减_昨日_pct_calc",
            ((w."全成交量_calc" - w."全5均量_calc") / NULLIF(w."全5均量_calc", (0)::numeric)) AS "全量增减_五日_pct_calc",
            ((w."全成交量_calc" - w."全10均量_calc") / NULLIF(w."全10均量_calc", (0)::numeric)) AS "全量增减_十日_pct_calc",
            ((w."全成交量_calc" - w."全20均量_calc") / NULLIF(w."全20均量_calc", (0)::numeric)) AS "全量增减_二十日_pct_calc",
            ((w."全成交量_calc" - w."全60均量_calc") / NULLIF(w."全60均量_calc", (0)::numeric)) AS "全量增减_六十日_pct_calc",
            (abs((w."全日收" - w."全日開")) / NULLIF((w."全日高" - w."全日低"), (0)::numeric)) AS "全实体_pct_calc",
            (abs((w."全日高" - GREATEST(w."全日開", w."全日收"))) / NULLIF((w."全日高" - w."全日低"), (0)::numeric)) AS "全上影_pct_calc",
            (abs((LEAST(w."全日開", w."全日收") - w."全日低")) / NULLIF((w."全日高" - w."全日低"), (0)::numeric)) AS "全下影_pct_calc",
                CASE
                    WHEN (w."全日低" > w."全昨高") THEN (LEAST(w."全日開", w."全日收") - GREATEST(w."全昨开", w."全昨收"))
                    WHEN (w."全日高" < w."全昨低") THEN (GREATEST(w."全日開", w."全日收") - LEAST(w."全昨开", w."全昨收"))
                    ELSE NULL::numeric
                END AS "全跳空缺口_calc"
           FROM full_window w
        ), full_labels AS (
         SELECT m.id,
            m.futures_id,
            m."日期",
            m."年",
            m."月",
            m."星期",
            m."週次",
            m."天別",
            m."全日開",
            m."全日收",
            m."全日高",
            m."全日低",
            m."全5MA",
            m."全10MA",
            m."全20MA",
            m."全60MA",
            m."全120MA",
            m."日盤開",
            m."日盤收",
            m."日盤高",
            m."日盤低",
            m."昨日收盤",
            m."日漲跌",
            m."日價漲跌_pct",
            m."日收減日開",
            m."日收開_昨收_pct",
            m."日收開_開盤_pct",
            m."日振幅",
            m."日振幅_昨收_pct",
            m."日振幅_開盤_pct",
            m."日成交量",
            m."日5最大量_日期",
            m."日5最大量",
            m."日10最大量_日期",
            m."日10最大量",
            m."日20最大量_日期",
            m."日20最大量",
            m."日60最大量_日期",
            m."日60最大量",
            m."日5均量",
            m."日10均量",
            m."日20均量",
            m."日60均量",
            m."日量增減_昨日_pct",
            m."日量增減_五日_pct",
            m."日量增減_十日_pct",
            m."日量增減_二十日_pct",
            m."日量增減_六十日_pct",
            m."日當日價量",
            m."日5日價量",
            m."日10日價量",
            m."日20日價量",
            m."日60日價量",
            m."K線型態",
            m."跳空缺口",
            m."跳空缺口_昨收_pct",
            m."實體_pct",
            m."上影_pct",
            m."下影_pct",
            m."日5MA",
            m."日10MA",
            m."日20MA",
            m."日60MA",
            m."日120MA",
            m."日240MA",
            m."日5均上升幅度_點",
            m."日10均上升幅度_點",
            m."日20均上升幅度_點",
            m."日60均上升幅度_點",
            m."日5均上升幅度_pct",
            m."日10均上升幅度_pct",
            m."日20均上升幅度_pct",
            m."日60均上升幅度_pct",
            m."日5_明日扣抵值",
            m."日10_明日扣抵值",
            m."日20_明日扣抵值",
            m."日60_明日扣抵值",
            m."日5_明日扣抵值影響_點",
            m."日10_明日扣抵值影響_點",
            m."日20_明日扣抵值影響_點",
            m."日60_明日扣抵值影響_點",
            m."日5_明日扣抵值影響_pct",
            m."日10_明日扣抵值影響_pct",
            m."日20_明日扣抵值影響_pct",
            m."日60_明日扣抵值影響_pct",
            m."日5MA乖離率",
            m."日10MA乖離率",
            m."日20MA乖離率",
            m."日60MA乖離率",
            m."日布林標準差",
            m."日布林上軌",
            m."日布林下軌",
            m."日法人多倉",
            m."日法人空倉",
            m."未沖銷_全",
            m."未沖銷_近期",
            m."未沖銷_遠期",
            m."主力_月結算狀態",
            m."主力新建多單",
            m."主力新建空單",
            m."主力淨多空和_5日",
            m."主力淨多空和_10日",
            m."主力淨多空和_20日",
            m."主力淨多空和_60日",
            m."夜盤開",
            m."夜盤收",
            m."夜盤高",
            m."夜盤低",
            m."夜5MA",
            m."夜成交量",
            m."夜法人多單量",
            m."夜法人空單量",
            m."全市場OI_小台",
            m."三大法人多單_小台",
            m."三大法人空單_小台",
            m."散戶多單_小台",
            m."散戶空單_小台",
            m."散戶多空比_小台",
            m."全市場OI_微台",
            m."三大法人多單_微台",
            m."三大法人空單_微台",
            m."散戶多單_微台",
            m."散戶空單_微台",
            m."散戶多空比_微台",
            m."法人總買超_億",
            m."call_月選",
            m."put_月選",
            m."買賣權比_月選",
            m."call_週三",
            m."put_週三",
            m."買賣權比_週三",
            m."call_週五",
            m."put_週五",
            m."買賣權比_週五",
            m."日位置",
            m."策略_價量昨日",
            m."策略_價量五日",
            m."日MA方向_5",
            m."日MA方向_10",
            m."日MA方向_20",
            m."策略_線型",
            m."法人籌碼",
            m."散戶籌碼",
            m."加權籌碼",
            m."期貨籌碼",
            m."選權籌碼",
            m."策略_籌碼",
            m."策略_價量昨日_線型",
            m."策略_價量五日_線型",
            m."策略_價量昨日_籌碼",
            m."策略_價量五日_籌碼",
            m."策略_線型_籌碼",
            m."策略_核心",
            m."建立時間",
            m."全昨收",
            m."全昨开",
            m."全昨高",
            m."全昨低",
            m."所属夜成交量",
            m."全成交量_calc",
            m."全昨日成交量",
            m."全5均量_calc",
            m."全10均量_calc",
            m."全20均量_calc",
            m."全60均量_calc",
            m."全5最大量_calc",
            m."全10最大量_calc",
            m."全20最大量_calc",
            m."全60最大量_calc",
            m."全涨跌_calc",
            m."全价涨跌_pct_calc",
            m."全收减全开_calc",
            m."全收开_昨收_pct_calc",
            m."全收开_开盘_pct_calc",
            m."全振幅_calc",
            m."全振幅_昨收_pct_calc",
            m."全振幅_开盘_pct_calc",
            m."全量增减_昨日_pct_calc",
            m."全量增减_五日_pct_calc",
            m."全量增减_十日_pct_calc",
            m."全量增减_二十日_pct_calc",
            m."全量增减_六十日_pct_calc",
            m."全实体_pct_calc",
            m."全上影_pct_calc",
            m."全下影_pct_calc",
            m."全跳空缺口_calc",
                CASE
                    WHEN ((m."全日開" IS NULL) OR (m."全日收" IS NULL) OR (m."全实体_pct_calc" IS NULL) OR (m."全上影_pct_calc" IS NULL) OR (m."全下影_pct_calc" IS NULL)) THEN NULL::text
                    WHEN ((abs((m."全日收" - m."全日開")) < 0.000001) OR (m."全实体_pct_calc" < 0.05)) THEN '⬜ 十字線'::text
                    WHEN ((m."全下影_pct_calc" > 0.5) AND (m."全实体_pct_calc" < 0.3)) THEN
                    CASE
                        WHEN (m."全日收" > m."全日開") THEN '🟥 錘子線'::text
                        ELSE '🟩 錘子線'::text
                    END
                    WHEN ((m."全上影_pct_calc" > 0.5) AND (m."全实体_pct_calc" < 0.3)) THEN
                    CASE
                        WHEN (m."全日收" > m."全日開") THEN '🟥 流星線'::text
                        ELSE '🟩 流星線'::text
                    END
                    WHEN (m."全实体_pct_calc" > 0.6) THEN
                    CASE
                        WHEN (m."全日收" > m."全日開") THEN '🟥 長紅K'::text
                        ELSE '🟩 長黑K'::text
                    END
                    ELSE
                    CASE
                        WHEN (m."全日收" > m."全日開") THEN '🟥 中實體K'::text
                        ELSE '🟩 中實體K'::text
                    END
                END AS "全K线型态_calc",
                CASE
                    WHEN ((m."全涨跌_calc" IS NULL) OR (m."全成交量_calc" IS NULL) OR (m."全昨日成交量" IS NULL)) THEN NULL::text
                    ELSE (
                    CASE
                        WHEN (m."全涨跌_calc" > (0)::numeric) THEN '價漲'::text
                        WHEN (m."全涨跌_calc" < (0)::numeric) THEN '價跌'::text
                        ELSE '價同'::text
                    END ||
                    CASE
                        WHEN (m."全成交量_calc" > m."全昨日成交量") THEN '量增'::text
                        WHEN (m."全成交量_calc" < m."全昨日成交量") THEN '量減'::text
                        ELSE '量同'::text
                    END)
                END AS "全当日价量_calc",
                CASE
                    WHEN ((m."全涨跌_calc" IS NULL) OR (m."全成交量_calc" IS NULL) OR (m."全5均量_calc" IS NULL)) THEN NULL::text
                    ELSE (
                    CASE
                        WHEN (m."全涨跌_calc" > (0)::numeric) THEN '價漲'::text
                        WHEN (m."全涨跌_calc" < (0)::numeric) THEN '價跌'::text
                        ELSE '價同'::text
                    END ||
                    CASE
                        WHEN (m."全成交量_calc" > m."全5均量_calc") THEN '量增'::text
                        WHEN (m."全成交量_calc" < m."全5均量_calc") THEN '量減'::text
                        ELSE '量同'::text
                    END)
                END AS "全5日价量_calc",
                CASE
                    WHEN ((m."全涨跌_calc" IS NULL) OR (m."全成交量_calc" IS NULL) OR (m."全10均量_calc" IS NULL)) THEN NULL::text
                    ELSE (
                    CASE
                        WHEN (m."全涨跌_calc" > (0)::numeric) THEN '價漲'::text
                        WHEN (m."全涨跌_calc" < (0)::numeric) THEN '價跌'::text
                        ELSE '價同'::text
                    END ||
                    CASE
                        WHEN (m."全成交量_calc" > m."全10均量_calc") THEN '量增'::text
                        WHEN (m."全成交量_calc" < m."全10均量_calc") THEN '量減'::text
                        ELSE '量同'::text
                    END)
                END AS "全10日价量_calc",
                CASE
                    WHEN ((m."全涨跌_calc" IS NULL) OR (m."全成交量_calc" IS NULL) OR (m."全20均量_calc" IS NULL)) THEN NULL::text
                    ELSE (
                    CASE
                        WHEN (m."全涨跌_calc" > (0)::numeric) THEN '價漲'::text
                        WHEN (m."全涨跌_calc" < (0)::numeric) THEN '價跌'::text
                        ELSE '價同'::text
                    END ||
                    CASE
                        WHEN (m."全成交量_calc" > m."全20均量_calc") THEN '量增'::text
                        WHEN (m."全成交量_calc" < m."全20均量_calc") THEN '量減'::text
                        ELSE '量同'::text
                    END)
                END AS "全20日价量_calc",
                CASE
                    WHEN ((m."全涨跌_calc" IS NULL) OR (m."全成交量_calc" IS NULL) OR (m."全60均量_calc" IS NULL)) THEN NULL::text
                    ELSE (
                    CASE
                        WHEN (m."全涨跌_calc" > (0)::numeric) THEN '價漲'::text
                        WHEN (m."全涨跌_calc" < (0)::numeric) THEN '價跌'::text
                        ELSE '價同'::text
                    END ||
                    CASE
                        WHEN (m."全成交量_calc" > m."全60均量_calc") THEN '量增'::text
                        WHEN (m."全成交量_calc" < m."全60均量_calc") THEN '量減'::text
                        ELSE '量同'::text
                    END)
                END AS "全60日价量_calc"
           FROM full_metrics m
        )
 SELECT id,
    futures_id,
    "日期",
    "年",
    "月",
    "星期",
    "週次",
    "天別",
    "全日開",
    "全日收",
    "全日高",
    "全日低",
    round("全涨跌_calc", 2) AS "全漲跌",
    round("全价涨跌_pct_calc", 4) AS "全價漲跌_pct",
    round("全收减全开_calc", 2) AS "全收減全開",
    round("全收开_昨收_pct_calc", 4) AS "全收開_昨收_pct",
    round("全收开_开盘_pct_calc", 4) AS "全收開_開盤_pct",
    round("全振幅_calc", 2) AS "全振幅",
    round("全振幅_昨收_pct_calc", 4) AS "全振幅_昨收_pct",
    round("全振幅_开盘_pct_calc", 4) AS "全振幅_開盤_pct",
    round("全成交量_calc", 2) AS "全成交量",
    ( SELECT x."日期"
           FROM ( SELECT q."日期",
                    q."全成交量_calc"
                   FROM full_volume q
                  WHERE ((q.futures_id = f.futures_id) AND (q."日期" < f."日期"))
                  ORDER BY q."日期" DESC
                 LIMIT 5) x
          ORDER BY x."全成交量_calc" DESC NULLS LAST, x."日期" DESC
         LIMIT 1) AS "全5最大量_日期",
    round("全5最大量_calc", 2) AS "全5最大量",
    ( SELECT x."日期"
           FROM ( SELECT q."日期",
                    q."全成交量_calc"
                   FROM full_volume q
                  WHERE ((q.futures_id = f.futures_id) AND (q."日期" < f."日期"))
                  ORDER BY q."日期" DESC
                 LIMIT 10) x
          ORDER BY x."全成交量_calc" DESC NULLS LAST, x."日期" DESC
         LIMIT 1) AS "全10最大量_日期",
    round("全10最大量_calc", 2) AS "全10最大量",
    ( SELECT x."日期"
           FROM ( SELECT q."日期",
                    q."全成交量_calc"
                   FROM full_volume q
                  WHERE ((q.futures_id = f.futures_id) AND (q."日期" < f."日期"))
                  ORDER BY q."日期" DESC
                 LIMIT 20) x
          ORDER BY x."全成交量_calc" DESC NULLS LAST, x."日期" DESC
         LIMIT 1) AS "全20最大量_日期",
    round("全20最大量_calc", 2) AS "全20最大量",
    ( SELECT x."日期"
           FROM ( SELECT q."日期",
                    q."全成交量_calc"
                   FROM full_volume q
                  WHERE ((q.futures_id = f.futures_id) AND (q."日期" < f."日期"))
                  ORDER BY q."日期" DESC
                 LIMIT 60) x
          ORDER BY x."全成交量_calc" DESC NULLS LAST, x."日期" DESC
         LIMIT 1) AS "全60最大量_日期",
    round("全60最大量_calc", 2) AS "全60最大量",
    round("全5均量_calc", 2) AS "全5均量",
    round("全10均量_calc", 2) AS "全10均量",
    round("全20均量_calc", 2) AS "全20均量",
    round("全60均量_calc", 2) AS "全60均量",
    round("全量增减_昨日_pct_calc", 4) AS "全量增減_昨日_pct",
    round("全量增减_五日_pct_calc", 4) AS "全量增減_五日_pct",
    round("全量增减_十日_pct_calc", 4) AS "全量增減_十日_pct",
    round("全量增减_二十日_pct_calc", 4) AS "全量增減_二十日_pct",
    round("全量增减_六十日_pct_calc", 4) AS "全量增減_六十日_pct",
    "全当日价量_calc" AS "全當日價量",
    "全5日价量_calc" AS "全5日價量",
    "全10日价量_calc" AS "全10日價量",
    "全20日价量_calc" AS "全20日價量",
    "全60日价量_calc" AS "全60日價量",
    "全K线型态_calc" AS "全K線型態",
    round("全跳空缺口_calc", 2) AS "全跳空缺口",
    round(("全跳空缺口_calc" / NULLIF("全昨收", (0)::numeric)), 4) AS "全跳空缺口_昨收_pct",
    round("全实体_pct_calc", 4) AS "全實體_pct",
    round("全上影_pct_calc", 4) AS "全上影_pct",
    round("全下影_pct_calc", 4) AS "全下影_pct",
    "全5MA",
    "全10MA",
    "全20MA",
    "全60MA",
    "全120MA",
    "日盤開",
    "日盤收",
    "日盤高",
    "日盤低",
    "昨日收盤",
    "日漲跌",
    "日價漲跌_pct",
    "日收減日開",
    "日收開_昨收_pct",
    "日收開_開盤_pct",
    "日振幅",
    "日振幅_昨收_pct",
    "日振幅_開盤_pct",
    "日成交量",
    "日5最大量_日期",
    "日5最大量",
    "日10最大量_日期",
    "日10最大量",
    "日20最大量_日期",
    "日20最大量",
    "日60最大量_日期",
    "日60最大量",
    "日5均量",
    "日10均量",
    "日20均量",
    "日60均量",
    "日量增減_昨日_pct",
    "日量增減_五日_pct",
    "日量增減_十日_pct",
    "日量增減_二十日_pct",
    "日量增減_六十日_pct",
    "日當日價量",
    "日5日價量",
    "日10日價量",
    "日20日價量",
    "日60日價量",
    "K線型態" AS "日K線型態",
    "跳空缺口" AS "日跳空缺口",
    "跳空缺口_昨收_pct" AS "日跳空缺口_昨收_pct",
    "實體_pct" AS "日實體_pct",
    "上影_pct" AS "日上影_pct",
    "下影_pct" AS "日下影_pct",
    "日5MA",
    "日10MA",
    "日20MA",
    "日60MA",
    "日120MA",
    "日240MA",
    "日5均上升幅度_點",
    "日10均上升幅度_點",
    "日20均上升幅度_點",
    "日60均上升幅度_點",
    "日5均上升幅度_pct",
    "日10均上升幅度_pct",
    "日20均上升幅度_pct",
    "日60均上升幅度_pct",
    "日5_明日扣抵值",
    "日10_明日扣抵值",
    "日20_明日扣抵值",
    "日60_明日扣抵值",
    "日5_明日扣抵值影響_點",
    "日10_明日扣抵值影響_點",
    "日20_明日扣抵值影響_點",
    "日60_明日扣抵值影響_點",
    "日5_明日扣抵值影響_pct",
    "日10_明日扣抵值影響_pct",
    "日20_明日扣抵值影響_pct",
    "日60_明日扣抵值影響_pct",
    "日5MA乖離率",
    "日10MA乖離率",
    "日20MA乖離率",
    "日60MA乖離率",
    "日布林標準差",
    "日布林上軌",
    "日布林下軌",
    "日法人多倉",
    "日法人空倉",
    "未沖銷_全",
    "未沖銷_近期",
    "未沖銷_遠期",
    "主力_月結算狀態",
    "主力新建多單",
    "主力新建空單",
    "主力淨多空和_5日",
    "主力淨多空和_10日",
    "主力淨多空和_20日",
    "主力淨多空和_60日",
    "夜盤開",
    "夜盤收",
    "夜盤高",
    "夜盤低",
    "夜5MA",
    "夜成交量",
    "夜法人多單量",
    "夜法人空單量",
    "全市場OI_小台",
    "三大法人多單_小台",
    "三大法人空單_小台",
    "散戶多單_小台",
    "散戶空單_小台",
    "散戶多空比_小台",
    "全市場OI_微台",
    "三大法人多單_微台",
    "三大法人空單_微台",
    "散戶多單_微台",
    "散戶空單_微台",
    "散戶多空比_微台",
    "法人總買超_億",
    "call_月選",
    "put_月選",
    "買賣權比_月選",
    "call_週三",
    "put_週三",
    "買賣權比_週三",
    "call_週五",
    "put_週五",
    "買賣權比_週五",
    "日位置",
    "策略_價量昨日",
    "策略_價量五日",
    "日MA方向_5",
    "日MA方向_10",
    "日MA方向_20",
    "策略_線型",
    "法人籌碼",
    "散戶籌碼",
    "加權籌碼",
    "期貨籌碼",
    "選權籌碼",
    "策略_籌碼",
    "策略_價量昨日_線型",
    "策略_價量五日_線型",
    "策略_價量昨日_籌碼",
    "策略_價量五日_籌碼",
    "策略_線型_籌碼",
    "策略_核心",
    "建立時間"
   FROM full_labels f;


--
-- Name: VIEW v_futures_report_daily_before_full_technical; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON VIEW report.v_futures_report_daily_before_full_technical IS '台指期報表：保留日盤衍生欄位，並於全日低後加入前夜盤＋當日日盤的全日衍生欄位';


--
-- Name: v_futures_report_daily_full_technical; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_report_daily_full_technical AS
 WITH technical_base AS (
         SELECT s.id,
            s.futures_id,
            s."日期",
            s."全日收",
            s."全5MA",
            s."全10MA",
            s."全20MA",
            s."全60MA",
            s."全120MA",
                CASE
                    WHEN (count(s."全日收") OVER (PARTITION BY s.futures_id ORDER BY s."日期" ROWS BETWEEN 239 PRECEDING AND CURRENT ROW) = 240) THEN avg(s."全日收") OVER (PARTITION BY s.futures_id ORDER BY s."日期" ROWS BETWEEN 239 PRECEDING AND CURRENT ROW)
                    ELSE NULL::numeric
                END AS full_ma_240,
            lag(s."全5MA") OVER report_window AS previous_full_ma_5,
            lag(s."全10MA") OVER report_window AS previous_full_ma_10,
            lag(s."全20MA") OVER report_window AS previous_full_ma_20,
            lag(s."全60MA") OVER report_window AS previous_full_ma_60,
            lag(s."全日收", 5) OVER report_window AS deduction_value_5,
            lag(s."全日收", 10) OVER report_window AS deduction_value_10,
            lag(s."全日收", 20) OVER report_window AS deduction_value_20,
            lag(s."全日收", 60) OVER report_window AS deduction_value_60,
                CASE
                    WHEN (count(s."全日收") OVER (PARTITION BY s.futures_id ORDER BY s."日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) = 20) THEN stddev_samp(s."全日收") OVER (PARTITION BY s.futures_id ORDER BY s."日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                    ELSE NULL::numeric
                END AS bollinger_std
           FROM report.v_futures_report_daily_before_full_technical s
          WINDOW report_window AS (PARTITION BY s.futures_id ORDER BY s."日期")
        ), technical_metrics AS (
         SELECT b.id,
            b.futures_id,
            b."日期",
            b."全日收",
            b."全5MA",
            b."全10MA",
            b."全20MA",
            b."全60MA",
            b."全120MA",
            b.full_ma_240,
            b.previous_full_ma_5,
            b.previous_full_ma_10,
            b.previous_full_ma_20,
            b.previous_full_ma_60,
            b.deduction_value_5,
            b.deduction_value_10,
            b.deduction_value_20,
            b.deduction_value_60,
            b.bollinger_std,
            (b."全5MA" - b.previous_full_ma_5) AS ma_change_5_points,
            (b."全10MA" - b.previous_full_ma_10) AS ma_change_10_points,
            (b."全20MA" - b.previous_full_ma_20) AS ma_change_20_points,
            (b."全60MA" - b.previous_full_ma_60) AS ma_change_60_points,
            ((b."全5MA" - b.previous_full_ma_5) / NULLIF(b.previous_full_ma_5, (0)::numeric)) AS ma_change_5_pct,
            ((b."全10MA" - b.previous_full_ma_10) / NULLIF(b.previous_full_ma_10, (0)::numeric)) AS ma_change_10_pct,
            ((b."全20MA" - b.previous_full_ma_20) / NULLIF(b.previous_full_ma_20, (0)::numeric)) AS ma_change_20_pct,
            ((b."全60MA" - b.previous_full_ma_60) / NULLIF(b.previous_full_ma_60, (0)::numeric)) AS ma_change_60_pct,
            (b."全日收" - b.deduction_value_5) AS deduction_effect_5_points,
            (b."全日收" - b.deduction_value_10) AS deduction_effect_10_points,
            (b."全日收" - b.deduction_value_20) AS deduction_effect_20_points,
            (b."全日收" - b.deduction_value_60) AS deduction_effect_60_points,
            ((b."全日收" - b.deduction_value_5) / NULLIF(b."全日收", (0)::numeric)) AS deduction_effect_5_pct,
            ((b."全日收" - b.deduction_value_10) / NULLIF(b."全日收", (0)::numeric)) AS deduction_effect_10_pct,
            ((b."全日收" - b.deduction_value_20) / NULLIF(b."全日收", (0)::numeric)) AS deduction_effect_20_pct,
            ((b."全日收" - b.deduction_value_60) / NULLIF(b."全日收", (0)::numeric)) AS deduction_effect_60_pct,
            ((b."全日收" - b."全5MA") / NULLIF(b."全5MA", (0)::numeric)) AS deviation_5,
            ((b."全日收" - b."全10MA") / NULLIF(b."全10MA", (0)::numeric)) AS deviation_10,
            ((b."全日收" - b."全20MA") / NULLIF(b."全20MA", (0)::numeric)) AS deviation_20,
            ((b."全日收" - b."全60MA") / NULLIF(b."全60MA", (0)::numeric)) AS deviation_60
           FROM technical_base b
        )
 SELECT id,
    futures_id,
    "日期",
    round(full_ma_240, 2) AS "全240MA",
    round(ma_change_5_points, 2) AS "全5均上升幅度_點",
    round(ma_change_10_points, 2) AS "全10均上升幅度_點",
    round(ma_change_20_points, 2) AS "全20均上升幅度_點",
    round(ma_change_60_points, 2) AS "全60均上升幅度_點",
    round(ma_change_5_pct, 4) AS "全5均上升幅度_pct",
    round(ma_change_10_pct, 4) AS "全10均上升幅度_pct",
    round(ma_change_20_pct, 4) AS "全20均上升幅度_pct",
    round(ma_change_60_pct, 4) AS "全60均上升幅度_pct",
    round(deduction_value_5, 2) AS "全5_明日扣抵值",
    round(deduction_value_10, 2) AS "全10_明日扣抵值",
    round(deduction_value_20, 2) AS "全20_明日扣抵值",
    round(deduction_value_60, 2) AS "全60_明日扣抵值",
    round(deduction_effect_5_points, 2) AS "全5_明日扣抵值影響_點",
    round(deduction_effect_10_points, 2) AS "全10_明日扣抵值影響_點",
    round(deduction_effect_20_points, 2) AS "全20_明日扣抵值影響_點",
    round(deduction_effect_60_points, 2) AS "全60_明日扣抵值影響_點",
    round(deduction_effect_5_pct, 4) AS "全5_明日扣抵值影響_pct",
    round(deduction_effect_10_pct, 4) AS "全10_明日扣抵值影響_pct",
    round(deduction_effect_20_pct, 4) AS "全20_明日扣抵值影響_pct",
    round(deduction_effect_60_pct, 4) AS "全60_明日扣抵值影響_pct",
    round(deviation_5, 4) AS "全5MA乖離率",
    round(deviation_10, 4) AS "全10MA乖離率",
    round(deviation_20, 4) AS "全20MA乖離率",
    round(deviation_60, 4) AS "全60MA乖離率",
    round(bollinger_std, 2) AS "全布林標準差",
    round(("全20MA" + ((2)::numeric * bollinger_std)), 2) AS "全布林上軌",
    round(("全20MA" - ((2)::numeric * bollinger_std)), 2) AS "全布林下軌"
   FROM technical_metrics m;


--
-- Name: v_futures_report_daily_base; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_report_daily_base AS
 WITH original AS (
         SELECT s.id,
            s.futures_id,
            s."日期",
            s."年",
            s."月",
            s."星期",
            s."週次",
            s."天別",
            s."全日開",
            s."全日收",
            s."全日高",
            s."全日低",
            s."全漲跌",
            s."全價漲跌_pct",
            s."全收減全開",
            s."全收開_昨收_pct",
            s."全收開_開盤_pct",
            s."全振幅",
            s."全振幅_昨收_pct",
            s."全振幅_開盤_pct",
            s."全成交量",
            s."全5最大量_日期",
            s."全5最大量",
            s."全10最大量_日期",
            s."全10最大量",
            s."全20最大量_日期",
            s."全20最大量",
            s."全60最大量_日期",
            s."全60最大量",
            s."全5均量",
            s."全10均量",
            s."全20均量",
            s."全60均量",
            s."全量增減_昨日_pct",
            s."全量增減_五日_pct",
            s."全量增減_十日_pct",
            s."全量增減_二十日_pct",
            s."全量增減_六十日_pct",
            s."全當日價量",
            s."全5日價量",
            s."全10日價量",
            s."全20日價量",
            s."全60日價量",
            s."全K線型態",
            s."全跳空缺口",
            s."全跳空缺口_昨收_pct",
            s."全實體_pct",
            s."全上影_pct",
            s."全下影_pct",
            s."全5MA",
            s."全10MA",
            s."全20MA",
            s."全60MA",
            s."全120MA",
            t."全240MA",
            t."全5均上升幅度_點",
            t."全10均上升幅度_點",
            t."全20均上升幅度_點",
            t."全60均上升幅度_點",
            t."全5均上升幅度_pct",
            t."全10均上升幅度_pct",
            t."全20均上升幅度_pct",
            t."全60均上升幅度_pct",
            t."全5_明日扣抵值",
            t."全10_明日扣抵值",
            t."全20_明日扣抵值",
            t."全60_明日扣抵值",
            t."全5_明日扣抵值影響_點",
            t."全10_明日扣抵值影響_點",
            t."全20_明日扣抵值影響_點",
            t."全60_明日扣抵值影響_點",
            t."全5_明日扣抵值影響_pct",
            t."全10_明日扣抵值影響_pct",
            t."全20_明日扣抵值影響_pct",
            t."全60_明日扣抵值影響_pct",
            t."全5MA乖離率",
            t."全10MA乖離率",
            t."全20MA乖離率",
            t."全60MA乖離率",
            t."全布林標準差",
            t."全布林上軌",
            t."全布林下軌",
            s."日盤開",
            s."日盤收",
            s."日盤高",
            s."日盤低",
            s."日漲跌",
            s."日價漲跌_pct",
            s."日收減日開",
            s."日收開_昨收_pct",
            s."日收開_開盤_pct",
            s."日振幅",
            s."日振幅_昨收_pct",
            s."日振幅_開盤_pct",
            s."日成交量",
            s."日5最大量_日期",
            s."日5最大量",
            s."日10最大量_日期",
            s."日10最大量",
            s."日20最大量_日期",
            s."日20最大量",
            s."日60最大量_日期",
            s."日60最大量",
            s."日5均量",
            s."日10均量",
            s."日20均量",
            s."日60均量",
            s."日量增減_昨日_pct",
            s."日量增減_五日_pct",
            s."日量增減_十日_pct",
            s."日量增減_二十日_pct",
            s."日量增減_六十日_pct",
            s."日當日價量",
            s."日5日價量",
            s."日10日價量",
            s."日20日價量",
            s."日60日價量",
            s."日K線型態",
            s."日跳空缺口",
            s."日跳空缺口_昨收_pct",
            s."日實體_pct",
            s."日上影_pct",
            s."日下影_pct",
            s."日5MA",
            s."日10MA",
            s."日20MA",
            s."日60MA",
            s."日120MA",
            s."日240MA",
            s."日5均上升幅度_點",
            s."日10均上升幅度_點",
            s."日20均上升幅度_點",
            s."日60均上升幅度_點",
            s."日5均上升幅度_pct",
            s."日10均上升幅度_pct",
            s."日20均上升幅度_pct",
            s."日60均上升幅度_pct",
            s."日5_明日扣抵值",
            s."日10_明日扣抵值",
            s."日20_明日扣抵值",
            s."日60_明日扣抵值",
            s."日5_明日扣抵值影響_點",
            s."日10_明日扣抵值影響_點",
            s."日20_明日扣抵值影響_點",
            s."日60_明日扣抵值影響_點",
            s."日5_明日扣抵值影響_pct",
            s."日10_明日扣抵值影響_pct",
            s."日20_明日扣抵值影響_pct",
            s."日60_明日扣抵值影響_pct",
            s."日5MA乖離率",
            s."日10MA乖離率",
            s."日20MA乖離率",
            s."日60MA乖離率",
            s."日布林標準差",
            s."日布林上軌",
            s."日布林下軌",
            s."日法人多倉",
            s."日法人空倉",
            s."未沖銷_全",
            s."未沖銷_近期",
            s."未沖銷_遠期",
            s."主力_月結算狀態",
            s."主力新建多單",
            s."主力新建空單",
            s."主力淨多空和_5日",
            s."主力淨多空和_10日",
            s."主力淨多空和_20日",
            s."主力淨多空和_60日",
            s."夜盤開",
            s."夜盤收",
            s."夜盤高",
            s."夜盤低",
            s."夜5MA",
            s."夜成交量",
            s."夜法人多單量",
            s."夜法人空單量",
            s."全市場OI_小台",
            s."三大法人多單_小台",
            s."三大法人空單_小台",
            s."散戶多單_小台",
            s."散戶空單_小台",
            s."散戶多空比_小台",
            s."全市場OI_微台",
            s."三大法人多單_微台",
            s."三大法人空單_微台",
            s."散戶多單_微台",
            s."散戶空單_微台",
            s."散戶多空比_微台",
            s."法人總買超_億",
            s."call_月選",
            s."put_月選",
            s."買賣權比_月選",
            s."call_週三",
            s."put_週三",
            s."買賣權比_週三",
            s."call_週五",
            s."put_週五",
            s."買賣權比_週五",
            s."日位置",
            s."策略_價量昨日",
            s."策略_價量五日",
            s."日MA方向_5",
            s."日MA方向_10",
            s."日MA方向_20",
            s."策略_線型",
            s."法人籌碼",
            s."散戶籌碼",
            s."加權籌碼",
            s."期貨籌碼",
            s."選權籌碼",
            s."策略_籌碼",
            s."策略_價量昨日_線型",
            s."策略_價量五日_線型",
            s."策略_價量昨日_籌碼",
            s."策略_價量五日_籌碼",
            s."策略_線型_籌碼",
            s."策略_核心",
            s."建立時間"
           FROM (report.v_futures_report_daily_before_full_technical s
             LEFT JOIN report.v_futures_report_daily_full_technical t ON (((t.id = s.id) AND (t.futures_id = s.futures_id) AND (t."日期" = s."日期"))))
        )
 SELECT o.id,
    o.futures_id,
    o."日期",
    o."年",
    o."月",
    o."星期",
    o."週次",
    o."天別",
    o."全日開",
    o."全日收",
    o."全日高",
    o."全日低",
    o."全漲跌",
    o."全價漲跌_pct",
    o."全收減全開",
    o."全收開_昨收_pct",
    o."全收開_開盤_pct",
    o."全振幅",
    o."全振幅_昨收_pct",
    o."全振幅_開盤_pct",
    o."全成交量",
    o."全5最大量_日期",
    o."全5最大量",
    o."全10最大量_日期",
    o."全10最大量",
    o."全20最大量_日期",
    o."全20最大量",
    o."全60最大量_日期",
    o."全60最大量",
    o."全5均量",
    o."全10均量",
    o."全20均量",
    o."全60均量",
    o."全量增減_昨日_pct",
    o."全量增減_五日_pct",
    o."全量增減_十日_pct",
    o."全量增減_二十日_pct",
    o."全量增減_六十日_pct",
    o."全當日價量",
    o."全5日價量",
    o."全10日價量",
    o."全20日價量",
    o."全60日價量",
    o."全K線型態",
    o."全跳空缺口",
    o."全跳空缺口_昨收_pct",
    o."全實體_pct",
    o."全上影_pct",
    o."全下影_pct",
    o."全5MA",
    o."全10MA",
    o."全20MA",
    o."全60MA",
    o."全120MA",
    o."全240MA",
    o."全5均上升幅度_點",
    o."全10均上升幅度_點",
    o."全20均上升幅度_點",
    o."全60均上升幅度_點",
    o."全5均上升幅度_pct",
    o."全10均上升幅度_pct",
    o."全20均上升幅度_pct",
    o."全60均上升幅度_pct",
    o."全5_明日扣抵值",
    o."全10_明日扣抵值",
    o."全20_明日扣抵值",
    o."全60_明日扣抵值",
    o."全5_明日扣抵值影響_點",
    o."全10_明日扣抵值影響_點",
    o."全20_明日扣抵值影響_點",
    o."全60_明日扣抵值影響_點",
    o."全5_明日扣抵值影響_pct",
    o."全10_明日扣抵值影響_pct",
    o."全20_明日扣抵值影響_pct",
    o."全60_明日扣抵值影響_pct",
    o."全5MA乖離率",
    o."全10MA乖離率",
    o."全20MA乖離率",
    o."全60MA乖離率",
    o."全布林標準差",
    o."全布林上軌",
    o."全布林下軌",
    o."日盤開",
    o."日盤收",
    o."日盤高",
    o."日盤低",
    o."日漲跌",
    o."日價漲跌_pct",
    o."日收減日開",
    o."日收開_昨收_pct",
    o."日收開_開盤_pct",
    o."日振幅",
    o."日振幅_昨收_pct",
    o."日振幅_開盤_pct",
    o."日成交量",
    o."日5最大量_日期",
    o."日5最大量",
    o."日10最大量_日期",
    o."日10最大量",
    o."日20最大量_日期",
    o."日20最大量",
    o."日60最大量_日期",
    o."日60最大量",
    o."日5均量",
    o."日10均量",
    o."日20均量",
    o."日60均量",
    o."日量增減_昨日_pct",
    o."日量增減_五日_pct",
    o."日量增減_十日_pct",
    o."日量增減_二十日_pct",
    o."日量增減_六十日_pct",
    o."日當日價量",
    o."日5日價量",
    o."日10日價量",
    o."日20日價量",
    o."日60日價量",
    o."日K線型態",
    o."日跳空缺口",
    o."日跳空缺口_昨收_pct",
    o."日實體_pct",
    o."日上影_pct",
    o."日下影_pct",
    o."日5MA",
    o."日10MA",
    o."日20MA",
    o."日60MA",
    o."日120MA",
    o."日240MA",
    o."日5均上升幅度_點",
    o."日10均上升幅度_點",
    o."日20均上升幅度_點",
    o."日60均上升幅度_點",
    o."日5均上升幅度_pct",
    o."日10均上升幅度_pct",
    o."日20均上升幅度_pct",
    o."日60均上升幅度_pct",
    o."日5_明日扣抵值",
    o."日10_明日扣抵值",
    o."日20_明日扣抵值",
    o."日60_明日扣抵值",
    o."日5_明日扣抵值影響_點",
    o."日10_明日扣抵值影響_點",
    o."日20_明日扣抵值影響_點",
    o."日60_明日扣抵值影響_點",
    o."日5_明日扣抵值影響_pct",
    o."日10_明日扣抵值影響_pct",
    o."日20_明日扣抵值影響_pct",
    o."日60_明日扣抵值影響_pct",
    o."日5MA乖離率",
    o."日10MA乖離率",
    o."日20MA乖離率",
    o."日60MA乖離率",
    o."日布林標準差",
    o."日布林上軌",
    o."日布林下軌",
    o."日法人多倉",
    o."日法人空倉",
    o."未沖銷_全",
    o."未沖銷_近期",
    o."未沖銷_遠期",
    o."主力_月結算狀態",
    o."主力新建多單",
    o."主力新建空單",
    o."主力淨多空和_5日",
    o."主力淨多空和_10日",
    o."主力淨多空和_20日",
    o."主力淨多空和_60日",
    o."夜盤開",
    o."夜盤收",
    o."夜盤高",
    o."夜盤低",
    r."夜漲跌",
    r."夜價漲跌_pct",
    o."夜成交量",
    r."夜價量結構",
    r."夜K線型態",
    r."夜5MA乖離率",
    r."夜10MA乖離率",
    r."夜20MA乖離率",
    r."夜60MA乖離率",
    r."夜布林標準差",
    r."夜布林上軌",
    r."夜布林下軌",
    o."夜法人多單量",
    o."夜法人空單量",
    o."全市場OI_小台",
    o."三大法人多單_小台",
    o."三大法人空單_小台",
    o."散戶多單_小台",
    o."散戶空單_小台",
    o."散戶多空比_小台",
    o."全市場OI_微台",
    o."三大法人多單_微台",
    o."三大法人空單_微台",
    o."散戶多單_微台",
    o."散戶空單_微台",
    o."散戶多空比_微台",
    o."法人總買超_億",
    o."call_月選",
    o."put_月選",
    o."買賣權比_月選",
    o."call_週三",
    o."put_週三",
    o."買賣權比_週三",
    o."call_週五",
    o."put_週五",
    o."買賣權比_週五",
    o."日位置" AS "全位置",
    o."策略_價量昨日" AS "策略_全價量昨日",
    o."策略_價量五日" AS "策略_全價量五日",
    o."日MA方向_5" AS "全MA方向_5",
    o."日MA方向_10" AS "全MA方向_10",
    o."日MA方向_20" AS "全MA方向_20",
    o."策略_線型",
    o."法人籌碼",
    o."散戶籌碼",
    o."加權籌碼",
    o."期貨籌碼",
    o."選權籌碼",
    o."策略_籌碼",
    o."策略_價量昨日_線型",
    o."策略_價量五日_線型",
    o."策略_價量昨日_籌碼",
    o."策略_價量五日_籌碼",
    o."策略_線型_籌碼",
    o."策略_核心",
    o."建立時間"
   FROM (original o
     LEFT JOIN core.futures_report_daily_raw r ON ((r.id = o.id)));


--
-- Name: VIEW v_futures_report_daily_base; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON VIEW report.v_futures_report_daily_base IS '期貨指數報表：夜盤衍生欄位由 Python 寫入 raw；View 僅以主鍵 JOIN 讀取，避免即時歷史視窗重算';


--
-- Name: mv_futures_report_daily_base; Type: MATERIALIZED VIEW; Schema: report; Owner: -
--

CREATE MATERIALIZED VIEW report.mv_futures_report_daily_base AS
 SELECT id,
    futures_id,
    "日期",
    "年",
    "月",
    "星期",
    "週次",
    "天別",
    "全日開",
    "全日收",
    "全日高",
    "全日低",
    "全漲跌",
    "全價漲跌_pct",
    "全收減全開",
    "全收開_昨收_pct",
    "全收開_開盤_pct",
    "全振幅",
    "全振幅_昨收_pct",
    "全振幅_開盤_pct",
    "全成交量",
    "全5最大量_日期",
    "全5最大量",
    "全10最大量_日期",
    "全10最大量",
    "全20最大量_日期",
    "全20最大量",
    "全60最大量_日期",
    "全60最大量",
    "全5均量",
    "全10均量",
    "全20均量",
    "全60均量",
    "全量增減_昨日_pct",
    "全量增減_五日_pct",
    "全量增減_十日_pct",
    "全量增減_二十日_pct",
    "全量增減_六十日_pct",
    "全當日價量",
    "全5日價量",
    "全10日價量",
    "全20日價量",
    "全60日價量",
    "全K線型態",
    "全跳空缺口",
    "全跳空缺口_昨收_pct",
    "全實體_pct",
    "全上影_pct",
    "全下影_pct",
    "全5MA",
    "全10MA",
    "全20MA",
    "全60MA",
    "全120MA",
    "全240MA",
    "全5均上升幅度_點",
    "全10均上升幅度_點",
    "全20均上升幅度_點",
    "全60均上升幅度_點",
    "全5均上升幅度_pct",
    "全10均上升幅度_pct",
    "全20均上升幅度_pct",
    "全60均上升幅度_pct",
    "全5_明日扣抵值",
    "全10_明日扣抵值",
    "全20_明日扣抵值",
    "全60_明日扣抵值",
    "全5_明日扣抵值影響_點",
    "全10_明日扣抵值影響_點",
    "全20_明日扣抵值影響_點",
    "全60_明日扣抵值影響_點",
    "全5_明日扣抵值影響_pct",
    "全10_明日扣抵值影響_pct",
    "全20_明日扣抵值影響_pct",
    "全60_明日扣抵值影響_pct",
    "全5MA乖離率",
    "全10MA乖離率",
    "全20MA乖離率",
    "全60MA乖離率",
    "全布林標準差",
    "全布林上軌",
    "全布林下軌",
    "日盤開",
    "日盤收",
    "日盤高",
    "日盤低",
    "日漲跌",
    "日價漲跌_pct",
    "日收減日開",
    "日收開_昨收_pct",
    "日收開_開盤_pct",
    "日振幅",
    "日振幅_昨收_pct",
    "日振幅_開盤_pct",
    "日成交量",
    "日5最大量_日期",
    "日5最大量",
    "日10最大量_日期",
    "日10最大量",
    "日20最大量_日期",
    "日20最大量",
    "日60最大量_日期",
    "日60最大量",
    "日5均量",
    "日10均量",
    "日20均量",
    "日60均量",
    "日量增減_昨日_pct",
    "日量增減_五日_pct",
    "日量增減_十日_pct",
    "日量增減_二十日_pct",
    "日量增減_六十日_pct",
    "日當日價量",
    "日5日價量",
    "日10日價量",
    "日20日價量",
    "日60日價量",
    "日K線型態",
    "日跳空缺口",
    "日跳空缺口_昨收_pct",
    "日實體_pct",
    "日上影_pct",
    "日下影_pct",
    "日5MA",
    "日10MA",
    "日20MA",
    "日60MA",
    "日120MA",
    "日240MA",
    "日5均上升幅度_點",
    "日10均上升幅度_點",
    "日20均上升幅度_點",
    "日60均上升幅度_點",
    "日5均上升幅度_pct",
    "日10均上升幅度_pct",
    "日20均上升幅度_pct",
    "日60均上升幅度_pct",
    "日5_明日扣抵值",
    "日10_明日扣抵值",
    "日20_明日扣抵值",
    "日60_明日扣抵值",
    "日5_明日扣抵值影響_點",
    "日10_明日扣抵值影響_點",
    "日20_明日扣抵值影響_點",
    "日60_明日扣抵值影響_點",
    "日5_明日扣抵值影響_pct",
    "日10_明日扣抵值影響_pct",
    "日20_明日扣抵值影響_pct",
    "日60_明日扣抵值影響_pct",
    "日5MA乖離率",
    "日10MA乖離率",
    "日20MA乖離率",
    "日60MA乖離率",
    "日布林標準差",
    "日布林上軌",
    "日布林下軌",
    "日法人多倉",
    "日法人空倉",
    "未沖銷_全",
    "未沖銷_近期",
    "未沖銷_遠期",
    "主力_月結算狀態",
    "主力新建多單",
    "主力新建空單",
    "主力淨多空和_5日",
    "主力淨多空和_10日",
    "主力淨多空和_20日",
    "主力淨多空和_60日",
    "夜盤開",
    "夜盤收",
    "夜盤高",
    "夜盤低",
    "夜漲跌",
    "夜價漲跌_pct",
    "夜成交量",
    "夜價量結構",
    "夜K線型態",
    "夜5MA乖離率",
    "夜10MA乖離率",
    "夜20MA乖離率",
    "夜60MA乖離率",
    "夜布林標準差",
    "夜布林上軌",
    "夜布林下軌",
    "夜法人多單量",
    "夜法人空單量",
    "全市場OI_小台",
    "三大法人多單_小台",
    "三大法人空單_小台",
    "散戶多單_小台",
    "散戶空單_小台",
    "散戶多空比_小台",
    "全市場OI_微台",
    "三大法人多單_微台",
    "三大法人空單_微台",
    "散戶多單_微台",
    "散戶空單_微台",
    "散戶多空比_微台",
    "法人總買超_億",
    "call_月選",
    "put_月選",
    "買賣權比_月選",
    "call_週三",
    "put_週三",
    "買賣權比_週三",
    "call_週五",
    "put_週五",
    "買賣權比_週五",
    "全位置",
    "策略_全價量昨日",
    "策略_全價量五日",
    "全MA方向_5",
    "全MA方向_10",
    "全MA方向_20",
    "策略_線型",
    "法人籌碼",
    "散戶籌碼",
    "加權籌碼",
    "期貨籌碼",
    "選權籌碼",
    "策略_籌碼",
    "策略_價量昨日_線型",
    "策略_價量五日_線型",
    "策略_價量昨日_籌碼",
    "策略_價量五日_籌碼",
    "策略_線型_籌碼",
    "策略_核心",
    "建立時間"
   FROM report.v_futures_report_daily_base
  WITH NO DATA;


--
-- Name: MATERIALIZED VIEW mv_futures_report_daily_base; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON MATERIALIZED VIEW report.mv_futures_report_daily_base IS '期貨完整報表物化快取；更新流程刷新一次，Dashboard 查詢不再重算多層技術指標 View。';


--
-- Name: v_attention_notice; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_attention_notice AS
 SELECT 'twse'::text AS "來源",
    to_char((to_timestamp((twse_announcement_notice."日期_ts")::double precision) AT TIME ZONE 'UTC'::text), 'YYYY/MM/DD'::text) AS "公告日期",
    twse_announcement_notice."證券代號",
    twse_announcement_notice."證券名稱",
    twse_announcement_notice.law_src,
    twse_announcement_notice."累計次數" AS "累計",
    twse_announcement_notice."收盤價",
    twse_announcement_notice."本益比",
    twse_announcement_notice."注意交易資訊"
   FROM raw_exchange.twse_announcement_notice
UNION ALL
 SELECT 'tpex'::text AS "來源",
    to_char((to_timestamp((tpex_bulletin_attention."公告日期_ts")::double precision) AT TIME ZONE 'UTC'::text), 'YYYY/MM/DD'::text) AS "公告日期",
    tpex_bulletin_attention."證券代號",
    tpex_bulletin_attention."證券名稱",
    tpex_bulletin_attention.law_src,
    tpex_bulletin_attention."累計",
    tpex_bulletin_attention."收盤價",
    tpex_bulletin_attention."本益比",
    tpex_bulletin_attention."注意交易資訊"
   FROM raw_exchange.tpex_bulletin_attention;


--
-- Name: v_data_source; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_data_source AS
 SELECT name,
    storage_table,
    api_method,
    (((src_link || api_path) || '?'::text) || api_params) AS api_sample,
    (src_link || web_path) AS web_page,
    api_res_sample,
    memo
   FROM core.data_source ds;


--
-- Name: v_futures_dashboard_turning; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_dashboard_turning AS
 SELECT b.id,
    b.futures_id,
    b."日期",
    b."年",
    b."月",
    b."星期",
    b."週次",
    b."天別",
    b."全日開",
    b."全日收",
    b."全日高",
    b."全日低",
    b."全漲跌",
    b."全價漲跌_pct",
    b."全收減全開",
    b."全收開_昨收_pct",
    b."全收開_開盤_pct",
    b."全振幅",
    b."全振幅_昨收_pct",
    b."全振幅_開盤_pct",
    b."全成交量",
    b."全5最大量_日期",
    b."全5最大量",
    b."全10最大量_日期",
    b."全10最大量",
    b."全20最大量_日期",
    b."全20最大量",
    b."全60最大量_日期",
    b."全60最大量",
    b."全5均量",
    b."全10均量",
    b."全20均量",
    b."全60均量",
    b."全量增減_昨日_pct",
    b."全量增減_五日_pct",
    b."全量增減_十日_pct",
    b."全量增減_二十日_pct",
    b."全量增減_六十日_pct",
    b."全當日價量",
    b."全5日價量",
    b."全10日價量",
    b."全20日價量",
    b."全60日價量",
    b."全K線型態",
    b."全跳空缺口",
    b."全跳空缺口_昨收_pct",
    b."全實體_pct",
    b."全上影_pct",
    b."全下影_pct",
    b."全5MA",
    b."全10MA",
    b."全20MA",
    b."全60MA",
    b."全120MA",
    b."全240MA",
    b."全5均上升幅度_點",
    b."全10均上升幅度_點",
    b."全20均上升幅度_點",
    b."全60均上升幅度_點",
    b."全5均上升幅度_pct",
    b."全10均上升幅度_pct",
    b."全20均上升幅度_pct",
    b."全60均上升幅度_pct",
    b."全5_明日扣抵值",
    b."全10_明日扣抵值",
    b."全20_明日扣抵值",
    b."全60_明日扣抵值",
    b."全5_明日扣抵值影響_點",
    b."全10_明日扣抵值影響_點",
    b."全20_明日扣抵值影響_點",
    b."全60_明日扣抵值影響_點",
    b."全5_明日扣抵值影響_pct",
    b."全10_明日扣抵值影響_pct",
    b."全20_明日扣抵值影響_pct",
    b."全60_明日扣抵值影響_pct",
    b."全5MA乖離率",
    b."全10MA乖離率",
    b."全20MA乖離率",
    b."全60MA乖離率",
    b."全布林標準差",
    b."全布林上軌",
    b."全布林下軌",
    b."日盤開",
    b."日盤收",
    b."日盤高",
    b."日盤低",
    b."日漲跌",
    b."日價漲跌_pct",
    b."日收減日開",
    b."日收開_昨收_pct",
    b."日收開_開盤_pct",
    b."日振幅",
    b."日振幅_昨收_pct",
    b."日振幅_開盤_pct",
    b."日成交量",
    b."日5最大量_日期",
    b."日5最大量",
    b."日10最大量_日期",
    b."日10最大量",
    b."日20最大量_日期",
    b."日20最大量",
    b."日60最大量_日期",
    b."日60最大量",
    b."日5均量",
    b."日10均量",
    b."日20均量",
    b."日60均量",
    b."日量增減_昨日_pct",
    b."日量增減_五日_pct",
    b."日量增減_十日_pct",
    b."日量增減_二十日_pct",
    b."日量增減_六十日_pct",
    b."日當日價量",
    b."日5日價量",
    b."日10日價量",
    b."日20日價量",
    b."日60日價量",
    b."日K線型態",
    b."日跳空缺口",
    b."日跳空缺口_昨收_pct",
    b."日實體_pct",
    b."日上影_pct",
    b."日下影_pct",
    b."日5MA",
    b."日10MA",
    b."日20MA",
    b."日60MA",
    b."日120MA",
    b."日240MA",
    b."日5均上升幅度_點",
    b."日10均上升幅度_點",
    b."日20均上升幅度_點",
    b."日60均上升幅度_點",
    b."日5均上升幅度_pct",
    b."日10均上升幅度_pct",
    b."日20均上升幅度_pct",
    b."日60均上升幅度_pct",
    b."日5_明日扣抵值",
    b."日10_明日扣抵值",
    b."日20_明日扣抵值",
    b."日60_明日扣抵值",
    b."日5_明日扣抵值影響_點",
    b."日10_明日扣抵值影響_點",
    b."日20_明日扣抵值影響_點",
    b."日60_明日扣抵值影響_點",
    b."日5_明日扣抵值影響_pct",
    b."日10_明日扣抵值影響_pct",
    b."日20_明日扣抵值影響_pct",
    b."日60_明日扣抵值影響_pct",
    b."日5MA乖離率",
    b."日10MA乖離率",
    b."日20MA乖離率",
    b."日60MA乖離率",
    b."日布林標準差",
    b."日布林上軌",
    b."日布林下軌",
    b."日法人多倉",
    b."日法人空倉",
    b."未沖銷_全",
    b."未沖銷_近期",
    b."未沖銷_遠期",
    b."主力_月結算狀態",
    b."主力新建多單",
    b."主力新建空單",
    b."主力淨多空和_5日",
    b."主力淨多空和_10日",
    b."主力淨多空和_20日",
    b."主力淨多空和_60日",
    b."夜盤開",
    b."夜盤收",
    b."夜盤高",
    b."夜盤低",
    b."夜漲跌",
    b."夜價漲跌_pct",
    b."夜成交量",
    b."夜價量結構",
    b."夜K線型態",
    b."夜5MA乖離率",
    b."夜10MA乖離率",
    b."夜20MA乖離率",
    b."夜60MA乖離率",
    b."夜布林標準差",
    b."夜布林上軌",
    b."夜布林下軌",
    b."夜法人多單量",
    b."夜法人空單量",
    b."全市場OI_小台",
    b."三大法人多單_小台",
    b."三大法人空單_小台",
    b."散戶多單_小台",
    b."散戶空單_小台",
    b."散戶多空比_小台",
    b."全市場OI_微台",
    b."三大法人多單_微台",
    b."三大法人空單_微台",
    b."散戶多單_微台",
    b."散戶空單_微台",
    b."散戶多空比_微台",
    b."法人總買超_億",
    b."call_月選",
    b."put_月選",
    b."買賣權比_月選",
    b."call_週三",
    b."put_週三",
    b."買賣權比_週三",
    b."call_週五",
    b."put_週五",
    b."買賣權比_週五",
        CASE
            WHEN ((b."日盤高" IS NULL) OR (b."日盤低" IS NULL) OR (b."日盤收" IS NULL)) THEN NULL::text
            WHEN (abs((b."日盤高" - b."日盤收")) < abs((b."日盤收" - b."日盤低"))) THEN '漲勢盤'::text
            ELSE '跌勢盤'::text
        END AS "日盤走勢",
    t."日盤轉折支撐",
    t."日盤轉折壓力",
        CASE
            WHEN ((b."夜盤高" IS NULL) OR (b."夜盤低" IS NULL) OR (b."夜盤收" IS NULL)) THEN NULL::text
            WHEN (abs((b."夜盤高" - b."夜盤收")) < abs((b."夜盤收" - b."夜盤低"))) THEN '漲勢盤'::text
            ELSE '跌勢盤'::text
        END AS "夜盤走勢",
    t."夜盤轉折支撐",
    t."夜盤轉折壓力",
    b."全位置",
    b."策略_全價量昨日",
    b."策略_全價量五日",
    b."全MA方向_5",
    b."全MA方向_10",
    b."全MA方向_20",
    b."策略_線型",
    b."法人籌碼",
    b."散戶籌碼",
    b."加權籌碼",
    b."期貨籌碼",
    b."選權籌碼",
    b."策略_籌碼",
    b."策略_價量昨日_線型",
    b."策略_價量五日_線型",
    b."策略_價量昨日_籌碼",
    b."策略_價量五日_籌碼",
    b."策略_線型_籌碼",
    b."策略_核心_昨價量",
    b."策略_核心_五日價量",
    b."建立時間",
    b."夜盤資料來源"
   FROM (report.futures_report_daily_dashboard_cache b
     LEFT JOIN report.futures_turning_levels t ON (((t.futures_id = b.futures_id) AND (t."日期" = b."日期"))));


--
-- Name: v_futures_dashboard_turning_without_session_trend; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_dashboard_turning_without_session_trend AS
 SELECT b.id,
    b.futures_id,
    b."日期",
    b."年",
    b."月",
    b."星期",
    b."週次",
    b."天別",
    b."全日開",
    b."全日收",
    b."全日高",
    b."全日低",
    b."全漲跌",
    b."全價漲跌_pct",
    b."全收減全開",
    b."全收開_昨收_pct",
    b."全收開_開盤_pct",
    b."全振幅",
    b."全振幅_昨收_pct",
    b."全振幅_開盤_pct",
    b."全成交量",
    b."全5最大量_日期",
    b."全5最大量",
    b."全10最大量_日期",
    b."全10最大量",
    b."全20最大量_日期",
    b."全20最大量",
    b."全60最大量_日期",
    b."全60最大量",
    b."全5均量",
    b."全10均量",
    b."全20均量",
    b."全60均量",
    b."全量增減_昨日_pct",
    b."全量增減_五日_pct",
    b."全量增減_十日_pct",
    b."全量增減_二十日_pct",
    b."全量增減_六十日_pct",
    b."全當日價量",
    b."全5日價量",
    b."全10日價量",
    b."全20日價量",
    b."全60日價量",
    b."全K線型態",
    b."全跳空缺口",
    b."全跳空缺口_昨收_pct",
    b."全實體_pct",
    b."全上影_pct",
    b."全下影_pct",
    b."全5MA",
    b."全10MA",
    b."全20MA",
    b."全60MA",
    b."全120MA",
    b."全240MA",
    b."全5均上升幅度_點",
    b."全10均上升幅度_點",
    b."全20均上升幅度_點",
    b."全60均上升幅度_點",
    b."全5均上升幅度_pct",
    b."全10均上升幅度_pct",
    b."全20均上升幅度_pct",
    b."全60均上升幅度_pct",
    b."全5_明日扣抵值",
    b."全10_明日扣抵值",
    b."全20_明日扣抵值",
    b."全60_明日扣抵值",
    b."全5_明日扣抵值影響_點",
    b."全10_明日扣抵值影響_點",
    b."全20_明日扣抵值影響_點",
    b."全60_明日扣抵值影響_點",
    b."全5_明日扣抵值影響_pct",
    b."全10_明日扣抵值影響_pct",
    b."全20_明日扣抵值影響_pct",
    b."全60_明日扣抵值影響_pct",
    b."全5MA乖離率",
    b."全10MA乖離率",
    b."全20MA乖離率",
    b."全60MA乖離率",
    b."全布林標準差",
    b."全布林上軌",
    b."全布林下軌",
    b."日盤開",
    b."日盤收",
    b."日盤高",
    b."日盤低",
    b."日漲跌",
    b."日價漲跌_pct",
    b."日收減日開",
    b."日收開_昨收_pct",
    b."日收開_開盤_pct",
    b."日振幅",
    b."日振幅_昨收_pct",
    b."日振幅_開盤_pct",
    b."日成交量",
    b."日5最大量_日期",
    b."日5最大量",
    b."日10最大量_日期",
    b."日10最大量",
    b."日20最大量_日期",
    b."日20最大量",
    b."日60最大量_日期",
    b."日60最大量",
    b."日5均量",
    b."日10均量",
    b."日20均量",
    b."日60均量",
    b."日量增減_昨日_pct",
    b."日量增減_五日_pct",
    b."日量增減_十日_pct",
    b."日量增減_二十日_pct",
    b."日量增減_六十日_pct",
    b."日當日價量",
    b."日5日價量",
    b."日10日價量",
    b."日20日價量",
    b."日60日價量",
    b."日K線型態",
    b."日跳空缺口",
    b."日跳空缺口_昨收_pct",
    b."日實體_pct",
    b."日上影_pct",
    b."日下影_pct",
    b."日5MA",
    b."日10MA",
    b."日20MA",
    b."日60MA",
    b."日120MA",
    b."日240MA",
    b."日5均上升幅度_點",
    b."日10均上升幅度_點",
    b."日20均上升幅度_點",
    b."日60均上升幅度_點",
    b."日5均上升幅度_pct",
    b."日10均上升幅度_pct",
    b."日20均上升幅度_pct",
    b."日60均上升幅度_pct",
    b."日5_明日扣抵值",
    b."日10_明日扣抵值",
    b."日20_明日扣抵值",
    b."日60_明日扣抵值",
    b."日5_明日扣抵值影響_點",
    b."日10_明日扣抵值影響_點",
    b."日20_明日扣抵值影響_點",
    b."日60_明日扣抵值影響_點",
    b."日5_明日扣抵值影響_pct",
    b."日10_明日扣抵值影響_pct",
    b."日20_明日扣抵值影響_pct",
    b."日60_明日扣抵值影響_pct",
    b."日5MA乖離率",
    b."日10MA乖離率",
    b."日20MA乖離率",
    b."日60MA乖離率",
    b."日布林標準差",
    b."日布林上軌",
    b."日布林下軌",
    b."日法人多倉",
    b."日法人空倉",
    b."未沖銷_全",
    b."未沖銷_近期",
    b."未沖銷_遠期",
    b."主力_月結算狀態",
    b."主力新建多單",
    b."主力新建空單",
    b."主力淨多空和_5日",
    b."主力淨多空和_10日",
    b."主力淨多空和_20日",
    b."主力淨多空和_60日",
    b."夜盤開",
    b."夜盤收",
    b."夜盤高",
    b."夜盤低",
    b."夜漲跌",
    b."夜價漲跌_pct",
    b."夜成交量",
    b."夜價量結構",
    b."夜K線型態",
    b."夜5MA乖離率",
    b."夜10MA乖離率",
    b."夜20MA乖離率",
    b."夜60MA乖離率",
    b."夜布林標準差",
    b."夜布林上軌",
    b."夜布林下軌",
    b."夜法人多單量",
    b."夜法人空單量",
    b."全市場OI_小台",
    b."三大法人多單_小台",
    b."三大法人空單_小台",
    b."散戶多單_小台",
    b."散戶空單_小台",
    b."散戶多空比_小台",
    b."全市場OI_微台",
    b."三大法人多單_微台",
    b."三大法人空單_微台",
    b."散戶多單_微台",
    b."散戶空單_微台",
    b."散戶多空比_微台",
    b."法人總買超_億",
    b."call_月選",
    b."put_月選",
    b."買賣權比_月選",
    b."call_週三",
    b."put_週三",
    b."買賣權比_週三",
    b."call_週五",
    b."put_週五",
    b."買賣權比_週五",
    t."日盤轉折支撐",
    t."日盤轉折壓力",
    t."夜盤轉折支撐",
    t."夜盤轉折壓力",
    b."全位置",
    b."策略_全價量昨日",
    b."策略_全價量五日",
    b."全MA方向_5",
    b."全MA方向_10",
    b."全MA方向_20",
    b."策略_線型",
    b."法人籌碼",
    b."散戶籌碼",
    b."加權籌碼",
    b."期貨籌碼",
    b."選權籌碼",
    b."策略_籌碼",
    b."策略_價量昨日_線型",
    b."策略_價量五日_線型",
    b."策略_價量昨日_籌碼",
    b."策略_價量五日_籌碼",
    b."策略_線型_籌碼",
    b."策略_核心_昨價量",
    b."策略_核心_五日價量",
    b."建立時間",
    b."夜盤資料來源"
   FROM (report.futures_report_daily_dashboard_cache b
     LEFT JOIN report.futures_turning_levels t ON (((t.futures_id = b.futures_id) AND (t."日期" = b."日期"))));


--
-- Name: v_taifex_tx_night_fallback; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_taifex_tx_night_fallback AS
 WITH official_front AS (
         SELECT d.id,
            d.trade_date,
            d.futures_id,
            d.contract_month,
            d.trading_session,
            d.open,
            d.high,
            d.low,
            d.close,
            d.volume,
            d.settlement_price,
            d.open_interest,
            d.created_at,
            d.updated_at,
            row_number() OVER (PARTITION BY d.trade_date, d.futures_id ORDER BY d.volume DESC NULLS LAST, d.contract_month) AS contract_rank
           FROM raw_exchange.taifex_futures_daily d
          WHERE ((d.trading_session = 'after_market'::text) AND (d.contract_month ~ '^[0-9]{6}$'::text))
        ), mapped AS (
         SELECT previous_day.report_date,
            o.trade_date AS official_trade_date,
            o.futures_id,
            o.contract_month,
            o.open,
            o.close,
            o.high,
            o.low,
            o.volume
           FROM (official_front o
             JOIN LATERAL ( SELECT max(f.date) AS report_date
                   FROM raw_finmind.fm_taiwan_futures_daily f
                  WHERE ((f.futures_id = o.futures_id) AND (f.trading_session = 'position'::text) AND (f.date < o.trade_date))) previous_day ON ((previous_day.report_date IS NOT NULL)))
          WHERE (o.contract_rank = 1)
        )
 SELECT r.futures_id,
    r."日期" AS report_date,
    m.official_trade_date,
    m.contract_month,
    COALESCE(r."夜盤開", m.open) AS open,
    COALESCE(r."夜盤收", m.close) AS close,
    COALESCE(r."夜盤高", m.high) AS high,
    COALESCE(r."夜盤低", m.low) AS low,
    COALESCE(r."夜成交量", (m.volume)::double precision) AS volume,
        CASE
            WHEN (EXISTS ( SELECT 1
               FROM raw_finmind.fm_taiwan_futures_daily fm
              WHERE ((fm.futures_id = r.futures_id) AND (fm.date = m.official_trade_date) AND (fm.trading_session = 'after_market'::text) AND (fm.open IS NOT NULL) AND (fm.close IS NOT NULL) AND (fm.max IS NOT NULL) AND (fm.min IS NOT NULL)))) THEN 'FinMind'::text
            WHEN ((m.open IS NOT NULL) AND (m.close IS NOT NULL) AND (m.high IS NOT NULL) AND (m.low IS NOT NULL)) THEN 'TAIFEX'::text
            WHEN ((r."夜盤開" IS NOT NULL) AND (r."夜盤收" IS NOT NULL) AND (r."夜盤高" IS NOT NULL) AND (r."夜盤低" IS NOT NULL)) THEN '既有資料'::text
            ELSE NULL::text
        END AS data_source
   FROM (core.futures_report_daily_raw r
     LEFT JOIN mapped m ON (((m.futures_id = r.futures_id) AND (m.report_date = r."日期"))));


--
-- Name: VIEW v_taifex_tx_night_fallback; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON VIEW report.v_taifex_tx_night_fallback IS '期貨夜盤行情優先採 FinMind，缺少開收高低時改採期交所官方資料。';


--
-- Name: v_futures_report_daily_without_turning; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_report_daily_without_turning AS
 SELECT cache.id,
    cache.futures_id,
    cache."日期",
    cache."年",
    cache."月",
    cache."星期",
    cache."週次",
    cache."天別",
    cache."全日開",
    cache."全日收",
    cache."全日高",
    cache."全日低",
    cache."全漲跌",
    cache."全價漲跌_pct",
    cache."全收減全開",
    cache."全收開_昨收_pct",
    cache."全收開_開盤_pct",
    cache."全振幅",
    cache."全振幅_昨收_pct",
    cache."全振幅_開盤_pct",
    cache."全成交量",
    cache."全5最大量_日期",
    cache."全5最大量",
    cache."全10最大量_日期",
    cache."全10最大量",
    cache."全20最大量_日期",
    cache."全20最大量",
    cache."全60最大量_日期",
    cache."全60最大量",
    cache."全5均量",
    cache."全10均量",
    cache."全20均量",
    cache."全60均量",
    cache."全量增減_昨日_pct",
    cache."全量增減_五日_pct",
    cache."全量增減_十日_pct",
    cache."全量增減_二十日_pct",
    cache."全量增減_六十日_pct",
    cache."全當日價量",
    cache."全5日價量",
    cache."全10日價量",
    cache."全20日價量",
    cache."全60日價量",
    cache."全K線型態",
    cache."全跳空缺口",
    cache."全跳空缺口_昨收_pct",
    cache."全實體_pct",
    cache."全上影_pct",
    cache."全下影_pct",
    cache."全5MA",
    cache."全10MA",
    cache."全20MA",
    cache."全60MA",
    cache."全120MA",
    cache."全240MA",
    cache."全5均上升幅度_點",
    cache."全10均上升幅度_點",
    cache."全20均上升幅度_點",
    cache."全60均上升幅度_點",
    cache."全5均上升幅度_pct",
    cache."全10均上升幅度_pct",
    cache."全20均上升幅度_pct",
    cache."全60均上升幅度_pct",
    cache."全5_明日扣抵值",
    cache."全10_明日扣抵值",
    cache."全20_明日扣抵值",
    cache."全60_明日扣抵值",
    cache."全5_明日扣抵值影響_點",
    cache."全10_明日扣抵值影響_點",
    cache."全20_明日扣抵值影響_點",
    cache."全60_明日扣抵值影響_點",
    cache."全5_明日扣抵值影響_pct",
    cache."全10_明日扣抵值影響_pct",
    cache."全20_明日扣抵值影響_pct",
    cache."全60_明日扣抵值影響_pct",
    cache."全5MA乖離率",
    cache."全10MA乖離率",
    cache."全20MA乖離率",
    cache."全60MA乖離率",
    cache."全布林標準差",
    cache."全布林上軌",
    cache."全布林下軌",
    cache."日盤開",
    cache."日盤收",
    cache."日盤高",
    cache."日盤低",
    cache."日漲跌",
    cache."日價漲跌_pct",
    cache."日收減日開",
    cache."日收開_昨收_pct",
    cache."日收開_開盤_pct",
    cache."日振幅",
    cache."日振幅_昨收_pct",
    cache."日振幅_開盤_pct",
    cache."日成交量",
    cache."日5最大量_日期",
    cache."日5最大量",
    cache."日10最大量_日期",
    cache."日10最大量",
    cache."日20最大量_日期",
    cache."日20最大量",
    cache."日60最大量_日期",
    cache."日60最大量",
    cache."日5均量",
    cache."日10均量",
    cache."日20均量",
    cache."日60均量",
    cache."日量增減_昨日_pct",
    cache."日量增減_五日_pct",
    cache."日量增減_十日_pct",
    cache."日量增減_二十日_pct",
    cache."日量增減_六十日_pct",
    cache."日當日價量",
    cache."日5日價量",
    cache."日10日價量",
    cache."日20日價量",
    cache."日60日價量",
    cache."日K線型態",
    cache."日跳空缺口",
    cache."日跳空缺口_昨收_pct",
    cache."日實體_pct",
    cache."日上影_pct",
    cache."日下影_pct",
    cache."日5MA",
    cache."日10MA",
    cache."日20MA",
    cache."日60MA",
    cache."日120MA",
    cache."日240MA",
    cache."日5均上升幅度_點",
    cache."日10均上升幅度_點",
    cache."日20均上升幅度_點",
    cache."日60均上升幅度_點",
    cache."日5均上升幅度_pct",
    cache."日10均上升幅度_pct",
    cache."日20均上升幅度_pct",
    cache."日60均上升幅度_pct",
    cache."日5_明日扣抵值",
    cache."日10_明日扣抵值",
    cache."日20_明日扣抵值",
    cache."日60_明日扣抵值",
    cache."日5_明日扣抵值影響_點",
    cache."日10_明日扣抵值影響_點",
    cache."日20_明日扣抵值影響_點",
    cache."日60_明日扣抵值影響_點",
    cache."日5_明日扣抵值影響_pct",
    cache."日10_明日扣抵值影響_pct",
    cache."日20_明日扣抵值影響_pct",
    cache."日60_明日扣抵值影響_pct",
    cache."日5MA乖離率",
    cache."日10MA乖離率",
    cache."日20MA乖離率",
    cache."日60MA乖離率",
    cache."日布林標準差",
    cache."日布林上軌",
    cache."日布林下軌",
    cache."日法人多倉",
    cache."日法人空倉",
    cache."未沖銷_全",
    cache."未沖銷_近期",
    cache."未沖銷_遠期",
    cache."主力_月結算狀態",
    cache."主力新建多單",
    cache."主力新建空單",
    cache."主力淨多空和_5日",
    cache."主力淨多空和_10日",
    cache."主力淨多空和_20日",
    cache."主力淨多空和_60日",
    COALESCE((cache."夜盤開")::double precision, fallback.open) AS "夜盤開",
    COALESCE((cache."夜盤收")::double precision, fallback.close) AS "夜盤收",
    COALESCE((cache."夜盤高")::double precision, fallback.high) AS "夜盤高",
    COALESCE((cache."夜盤低")::double precision, fallback.low) AS "夜盤低",
    cache."夜漲跌",
    cache."夜價漲跌_pct",
    COALESCE((cache."夜成交量")::double precision, fallback.volume) AS "夜成交量",
    cache."夜價量結構",
    cache."夜K線型態",
    cache."夜5MA乖離率",
    cache."夜10MA乖離率",
    cache."夜20MA乖離率",
    cache."夜60MA乖離率",
    cache."夜布林標準差",
    cache."夜布林上軌",
    cache."夜布林下軌",
    cache."夜法人多單量",
    cache."夜法人空單量",
    cache."全市場OI_小台",
    cache."三大法人多單_小台",
    cache."三大法人空單_小台",
    cache."散戶多單_小台",
    cache."散戶空單_小台",
    cache."散戶多空比_小台",
    cache."全市場OI_微台",
    cache."三大法人多單_微台",
    cache."三大法人空單_微台",
    cache."散戶多單_微台",
    cache."散戶空單_微台",
    cache."散戶多空比_微台",
    cache."法人總買超_億",
    cache."call_月選",
    cache."put_月選",
    cache."買賣權比_月選",
    cache."call_週三",
    cache."put_週三",
    cache."買賣權比_週三",
    cache."call_週五",
    cache."put_週五",
    cache."買賣權比_週五",
    cache."全位置",
    cache."策略_全價量昨日",
    cache."策略_全價量五日",
    cache."全MA方向_5",
    cache."全MA方向_10",
    cache."全MA方向_20",
    cache."策略_線型",
    cache."法人籌碼",
    cache."散戶籌碼",
    cache."加權籌碼",
    cache."期貨籌碼",
    cache."選權籌碼",
    cache."策略_籌碼",
    cache."策略_價量昨日_線型",
    cache."策略_價量五日_線型",
    cache."策略_價量昨日_籌碼",
    cache."策略_價量五日_籌碼",
    cache."策略_線型_籌碼",
    cache."策略_核心",
    cache."建立時間",
    fallback.data_source AS "夜盤資料來源"
   FROM (( SELECT history.id,
            history.futures_id,
            history."日期",
            history."年",
            history."月",
            history."星期",
            history."週次",
            history."天別",
            history."全日開",
            history."全日收",
            history."全日高",
            history."全日低",
            history."全漲跌",
            history."全價漲跌_pct",
            history."全收減全開",
            history."全收開_昨收_pct",
            history."全收開_開盤_pct",
            history."全振幅",
            history."全振幅_昨收_pct",
            history."全振幅_開盤_pct",
            history."全成交量",
            history."全5最大量_日期",
            history."全5最大量",
            history."全10最大量_日期",
            history."全10最大量",
            history."全20最大量_日期",
            history."全20最大量",
            history."全60最大量_日期",
            history."全60最大量",
            history."全5均量",
            history."全10均量",
            history."全20均量",
            history."全60均量",
            history."全量增減_昨日_pct",
            history."全量增減_五日_pct",
            history."全量增減_十日_pct",
            history."全量增減_二十日_pct",
            history."全量增減_六十日_pct",
            history."全當日價量",
            history."全5日價量",
            history."全10日價量",
            history."全20日價量",
            history."全60日價量",
            history."全K線型態",
            history."全跳空缺口",
            history."全跳空缺口_昨收_pct",
            history."全實體_pct",
            history."全上影_pct",
            history."全下影_pct",
            history."全5MA",
            history."全10MA",
            history."全20MA",
            history."全60MA",
            history."全120MA",
            history."全240MA",
            history."全5均上升幅度_點",
            history."全10均上升幅度_點",
            history."全20均上升幅度_點",
            history."全60均上升幅度_點",
            history."全5均上升幅度_pct",
            history."全10均上升幅度_pct",
            history."全20均上升幅度_pct",
            history."全60均上升幅度_pct",
            history."全5_明日扣抵值",
            history."全10_明日扣抵值",
            history."全20_明日扣抵值",
            history."全60_明日扣抵值",
            history."全5_明日扣抵值影響_點",
            history."全10_明日扣抵值影響_點",
            history."全20_明日扣抵值影響_點",
            history."全60_明日扣抵值影響_點",
            history."全5_明日扣抵值影響_pct",
            history."全10_明日扣抵值影響_pct",
            history."全20_明日扣抵值影響_pct",
            history."全60_明日扣抵值影響_pct",
            history."全5MA乖離率",
            history."全10MA乖離率",
            history."全20MA乖離率",
            history."全60MA乖離率",
            history."全布林標準差",
            history."全布林上軌",
            history."全布林下軌",
            history."日盤開",
            history."日盤收",
            history."日盤高",
            history."日盤低",
            history."日漲跌",
            history."日價漲跌_pct",
            history."日收減日開",
            history."日收開_昨收_pct",
            history."日收開_開盤_pct",
            history."日振幅",
            history."日振幅_昨收_pct",
            history."日振幅_開盤_pct",
            history."日成交量",
            history."日5最大量_日期",
            history."日5最大量",
            history."日10最大量_日期",
            history."日10最大量",
            history."日20最大量_日期",
            history."日20最大量",
            history."日60最大量_日期",
            history."日60最大量",
            history."日5均量",
            history."日10均量",
            history."日20均量",
            history."日60均量",
            history."日量增減_昨日_pct",
            history."日量增減_五日_pct",
            history."日量增減_十日_pct",
            history."日量增減_二十日_pct",
            history."日量增減_六十日_pct",
            history."日當日價量",
            history."日5日價量",
            history."日10日價量",
            history."日20日價量",
            history."日60日價量",
            history."日K線型態",
            history."日跳空缺口",
            history."日跳空缺口_昨收_pct",
            history."日實體_pct",
            history."日上影_pct",
            history."日下影_pct",
            history."日5MA",
            history."日10MA",
            history."日20MA",
            history."日60MA",
            history."日120MA",
            history."日240MA",
            history."日5均上升幅度_點",
            history."日10均上升幅度_點",
            history."日20均上升幅度_點",
            history."日60均上升幅度_點",
            history."日5均上升幅度_pct",
            history."日10均上升幅度_pct",
            history."日20均上升幅度_pct",
            history."日60均上升幅度_pct",
            history."日5_明日扣抵值",
            history."日10_明日扣抵值",
            history."日20_明日扣抵值",
            history."日60_明日扣抵值",
            history."日5_明日扣抵值影響_點",
            history."日10_明日扣抵值影響_點",
            history."日20_明日扣抵值影響_點",
            history."日60_明日扣抵值影響_點",
            history."日5_明日扣抵值影響_pct",
            history."日10_明日扣抵值影響_pct",
            history."日20_明日扣抵值影響_pct",
            history."日60_明日扣抵值影響_pct",
            history."日5MA乖離率",
            history."日10MA乖離率",
            history."日20MA乖離率",
            history."日60MA乖離率",
            history."日布林標準差",
            history."日布林上軌",
            history."日布林下軌",
            history."日法人多倉",
            history."日法人空倉",
            history."未沖銷_全",
            history."未沖銷_近期",
            history."未沖銷_遠期",
            history."主力_月結算狀態",
            history."主力新建多單",
            history."主力新建空單",
            history."主力淨多空和_5日",
            history."主力淨多空和_10日",
            history."主力淨多空和_20日",
            history."主力淨多空和_60日",
            history."夜盤開",
            history."夜盤收",
            history."夜盤高",
            history."夜盤低",
            history."夜漲跌",
            history."夜價漲跌_pct",
            history."夜成交量",
            history."夜價量結構",
            history."夜K線型態",
            history."夜5MA乖離率",
            history."夜10MA乖離率",
            history."夜20MA乖離率",
            history."夜60MA乖離率",
            history."夜布林標準差",
            history."夜布林上軌",
            history."夜布林下軌",
            history."夜法人多單量",
            history."夜法人空單量",
            history."全市場OI_小台",
            history."三大法人多單_小台",
            history."三大法人空單_小台",
            history."散戶多單_小台",
            history."散戶空單_小台",
            history."散戶多空比_小台",
            history."全市場OI_微台",
            history."三大法人多單_微台",
            history."三大法人空單_微台",
            history."散戶多單_微台",
            history."散戶空單_微台",
            history."散戶多空比_微台",
            history."法人總買超_億",
            history."call_月選",
            history."put_月選",
            history."買賣權比_月選",
            history."call_週三",
            history."put_週三",
            history."買賣權比_週三",
            history."call_週五",
            history."put_週五",
            history."買賣權比_週五",
            history."全位置",
            history."策略_全價量昨日",
            history."策略_全價量五日",
            history."全MA方向_5",
            history."全MA方向_10",
            history."全MA方向_20",
            history."策略_線型",
            history."法人籌碼",
            history."散戶籌碼",
            history."加權籌碼",
            history."期貨籌碼",
            history."選權籌碼",
            history."策略_籌碼",
            history."策略_價量昨日_線型",
            history."策略_價量五日_線型",
            history."策略_價量昨日_籌碼",
            history."策略_價量五日_籌碼",
            history."策略_線型_籌碼",
            history."策略_核心",
            history."建立時間"
           FROM report.mv_futures_report_daily_base history
        UNION ALL
         SELECT live_tail.id,
            live_tail.futures_id,
            live_tail."日期",
            live_tail."年",
            live_tail."月",
            live_tail."星期",
            live_tail."週次",
            live_tail."天別",
            live_tail."全日開",
            live_tail."全日收",
            live_tail."全日高",
            live_tail."全日低",
            live_tail."全漲跌",
            live_tail."全價漲跌_pct",
            live_tail."全收減全開",
            live_tail."全收開_昨收_pct",
            live_tail."全收開_開盤_pct",
            live_tail."全振幅",
            live_tail."全振幅_昨收_pct",
            live_tail."全振幅_開盤_pct",
            live_tail."全成交量",
            live_tail."全5最大量_日期",
            live_tail."全5最大量",
            live_tail."全10最大量_日期",
            live_tail."全10最大量",
            live_tail."全20最大量_日期",
            live_tail."全20最大量",
            live_tail."全60最大量_日期",
            live_tail."全60最大量",
            live_tail."全5均量",
            live_tail."全10均量",
            live_tail."全20均量",
            live_tail."全60均量",
            live_tail."全量增減_昨日_pct",
            live_tail."全量增減_五日_pct",
            live_tail."全量增減_十日_pct",
            live_tail."全量增減_二十日_pct",
            live_tail."全量增減_六十日_pct",
            live_tail."全當日價量",
            live_tail."全5日價量",
            live_tail."全10日價量",
            live_tail."全20日價量",
            live_tail."全60日價量",
            live_tail."全K線型態",
            live_tail."全跳空缺口",
            live_tail."全跳空缺口_昨收_pct",
            live_tail."全實體_pct",
            live_tail."全上影_pct",
            live_tail."全下影_pct",
            live_tail."全5MA",
            live_tail."全10MA",
            live_tail."全20MA",
            live_tail."全60MA",
            live_tail."全120MA",
            live_tail."全240MA",
            live_tail."全5均上升幅度_點",
            live_tail."全10均上升幅度_點",
            live_tail."全20均上升幅度_點",
            live_tail."全60均上升幅度_點",
            live_tail."全5均上升幅度_pct",
            live_tail."全10均上升幅度_pct",
            live_tail."全20均上升幅度_pct",
            live_tail."全60均上升幅度_pct",
            live_tail."全5_明日扣抵值",
            live_tail."全10_明日扣抵值",
            live_tail."全20_明日扣抵值",
            live_tail."全60_明日扣抵值",
            live_tail."全5_明日扣抵值影響_點",
            live_tail."全10_明日扣抵值影響_點",
            live_tail."全20_明日扣抵值影響_點",
            live_tail."全60_明日扣抵值影響_點",
            live_tail."全5_明日扣抵值影響_pct",
            live_tail."全10_明日扣抵值影響_pct",
            live_tail."全20_明日扣抵值影響_pct",
            live_tail."全60_明日扣抵值影響_pct",
            live_tail."全5MA乖離率",
            live_tail."全10MA乖離率",
            live_tail."全20MA乖離率",
            live_tail."全60MA乖離率",
            live_tail."全布林標準差",
            live_tail."全布林上軌",
            live_tail."全布林下軌",
            live_tail."日盤開",
            live_tail."日盤收",
            live_tail."日盤高",
            live_tail."日盤低",
            live_tail."日漲跌",
            live_tail."日價漲跌_pct",
            live_tail."日收減日開",
            live_tail."日收開_昨收_pct",
            live_tail."日收開_開盤_pct",
            live_tail."日振幅",
            live_tail."日振幅_昨收_pct",
            live_tail."日振幅_開盤_pct",
            live_tail."日成交量",
            live_tail."日5最大量_日期",
            live_tail."日5最大量",
            live_tail."日10最大量_日期",
            live_tail."日10最大量",
            live_tail."日20最大量_日期",
            live_tail."日20最大量",
            live_tail."日60最大量_日期",
            live_tail."日60最大量",
            live_tail."日5均量",
            live_tail."日10均量",
            live_tail."日20均量",
            live_tail."日60均量",
            live_tail."日量增減_昨日_pct",
            live_tail."日量增減_五日_pct",
            live_tail."日量增減_十日_pct",
            live_tail."日量增減_二十日_pct",
            live_tail."日量增減_六十日_pct",
            live_tail."日當日價量",
            live_tail."日5日價量",
            live_tail."日10日價量",
            live_tail."日20日價量",
            live_tail."日60日價量",
            live_tail."日K線型態",
            live_tail."日跳空缺口",
            live_tail."日跳空缺口_昨收_pct",
            live_tail."日實體_pct",
            live_tail."日上影_pct",
            live_tail."日下影_pct",
            live_tail."日5MA",
            live_tail."日10MA",
            live_tail."日20MA",
            live_tail."日60MA",
            live_tail."日120MA",
            live_tail."日240MA",
            live_tail."日5均上升幅度_點",
            live_tail."日10均上升幅度_點",
            live_tail."日20均上升幅度_點",
            live_tail."日60均上升幅度_點",
            live_tail."日5均上升幅度_pct",
            live_tail."日10均上升幅度_pct",
            live_tail."日20均上升幅度_pct",
            live_tail."日60均上升幅度_pct",
            live_tail."日5_明日扣抵值",
            live_tail."日10_明日扣抵值",
            live_tail."日20_明日扣抵值",
            live_tail."日60_明日扣抵值",
            live_tail."日5_明日扣抵值影響_點",
            live_tail."日10_明日扣抵值影響_點",
            live_tail."日20_明日扣抵值影響_點",
            live_tail."日60_明日扣抵值影響_點",
            live_tail."日5_明日扣抵值影響_pct",
            live_tail."日10_明日扣抵值影響_pct",
            live_tail."日20_明日扣抵值影響_pct",
            live_tail."日60_明日扣抵值影響_pct",
            live_tail."日5MA乖離率",
            live_tail."日10MA乖離率",
            live_tail."日20MA乖離率",
            live_tail."日60MA乖離率",
            live_tail."日布林標準差",
            live_tail."日布林上軌",
            live_tail."日布林下軌",
            live_tail."日法人多倉",
            live_tail."日法人空倉",
            live_tail."未沖銷_全",
            live_tail."未沖銷_近期",
            live_tail."未沖銷_遠期",
            live_tail."主力_月結算狀態",
            live_tail."主力新建多單",
            live_tail."主力新建空單",
            live_tail."主力淨多空和_5日",
            live_tail."主力淨多空和_10日",
            live_tail."主力淨多空和_20日",
            live_tail."主力淨多空和_60日",
            live_tail."夜盤開",
            live_tail."夜盤收",
            live_tail."夜盤高",
            live_tail."夜盤低",
            live_tail."夜漲跌",
            live_tail."夜價漲跌_pct",
            live_tail."夜成交量",
            live_tail."夜價量結構",
            live_tail."夜K線型態",
            live_tail."夜5MA乖離率",
            live_tail."夜10MA乖離率",
            live_tail."夜20MA乖離率",
            live_tail."夜60MA乖離率",
            live_tail."夜布林標準差",
            live_tail."夜布林上軌",
            live_tail."夜布林下軌",
            live_tail."夜法人多單量",
            live_tail."夜法人空單量",
            live_tail."全市場OI_小台",
            live_tail."三大法人多單_小台",
            live_tail."三大法人空單_小台",
            live_tail."散戶多單_小台",
            live_tail."散戶空單_小台",
            live_tail."散戶多空比_小台",
            live_tail."全市場OI_微台",
            live_tail."三大法人多單_微台",
            live_tail."三大法人空單_微台",
            live_tail."散戶多單_微台",
            live_tail."散戶空單_微台",
            live_tail."散戶多空比_微台",
            live_tail."法人總買超_億",
            live_tail."call_月選",
            live_tail."put_月選",
            live_tail."買賣權比_月選",
            live_tail."call_週三",
            live_tail."put_週三",
            live_tail."買賣權比_週三",
            live_tail."call_週五",
            live_tail."put_週五",
            live_tail."買賣權比_週五",
            live_tail."全位置",
            live_tail."策略_全價量昨日",
            live_tail."策略_全價量五日",
            live_tail."全MA方向_5",
            live_tail."全MA方向_10",
            live_tail."全MA方向_20",
            live_tail."策略_線型",
            live_tail."法人籌碼",
            live_tail."散戶籌碼",
            live_tail."加權籌碼",
            live_tail."期貨籌碼",
            live_tail."選權籌碼",
            live_tail."策略_籌碼",
            live_tail."策略_價量昨日_線型",
            live_tail."策略_價量五日_線型",
            live_tail."策略_價量昨日_籌碼",
            live_tail."策略_價量五日_籌碼",
            live_tail."策略_線型_籌碼",
            live_tail."策略_核心",
            live_tail."建立時間"
           FROM report.futures_report_daily_tail_cache live_tail
          WHERE (live_tail."日期" > COALESCE(( SELECT max(history_max."日期") AS max
                   FROM report.mv_futures_report_daily_base history_max), '1900-01-01'::date))) cache
     LEFT JOIN report.v_taifex_tx_night_fallback fallback ON (((fallback.futures_id = cache.futures_id) AND (fallback.report_date = cache."日期"))));


--
-- Name: VIEW v_futures_report_daily_without_turning; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON VIEW report.v_futures_report_daily_without_turning IS '期貨指數混合快取報表；物化歷史加小型尾端快取，頁面載入不重算 live base。';


--
-- Name: v_futures_report_daily; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_report_daily AS
 SELECT b.id,
    b.futures_id,
    b."日期",
    b."年",
    b."月",
    b."星期",
    b."週次",
    b."天別",
    b."全日開",
    b."全日收",
    b."全日高",
    b."全日低",
    b."全漲跌",
    b."全價漲跌_pct",
    b."全收減全開",
    b."全收開_昨收_pct",
    b."全收開_開盤_pct",
    b."全振幅",
    b."全振幅_昨收_pct",
    b."全振幅_開盤_pct",
    b."全成交量",
    b."全5最大量_日期",
    b."全5最大量",
    b."全10最大量_日期",
    b."全10最大量",
    b."全20最大量_日期",
    b."全20最大量",
    b."全60最大量_日期",
    b."全60最大量",
    b."全5均量",
    b."全10均量",
    b."全20均量",
    b."全60均量",
    b."全量增減_昨日_pct",
    b."全量增減_五日_pct",
    b."全量增減_十日_pct",
    b."全量增減_二十日_pct",
    b."全量增減_六十日_pct",
    b."全當日價量",
    b."全5日價量",
    b."全10日價量",
    b."全20日價量",
    b."全60日價量",
    b."全K線型態",
    b."全跳空缺口",
    b."全跳空缺口_昨收_pct",
    b."全實體_pct",
    b."全上影_pct",
    b."全下影_pct",
    b."全5MA",
    b."全10MA",
    b."全20MA",
    b."全60MA",
    b."全120MA",
    b."全240MA",
    b."全5均上升幅度_點",
    b."全10均上升幅度_點",
    b."全20均上升幅度_點",
    b."全60均上升幅度_點",
    b."全5均上升幅度_pct",
    b."全10均上升幅度_pct",
    b."全20均上升幅度_pct",
    b."全60均上升幅度_pct",
    b."全5_明日扣抵值",
    b."全10_明日扣抵值",
    b."全20_明日扣抵值",
    b."全60_明日扣抵值",
    b."全5_明日扣抵值影響_點",
    b."全10_明日扣抵值影響_點",
    b."全20_明日扣抵值影響_點",
    b."全60_明日扣抵值影響_點",
    b."全5_明日扣抵值影響_pct",
    b."全10_明日扣抵值影響_pct",
    b."全20_明日扣抵值影響_pct",
    b."全60_明日扣抵值影響_pct",
    b."全5MA乖離率",
    b."全10MA乖離率",
    b."全20MA乖離率",
    b."全60MA乖離率",
    b."全布林標準差",
    b."全布林上軌",
    b."全布林下軌",
    b."日盤開",
    b."日盤收",
    b."日盤高",
    b."日盤低",
    b."日漲跌",
    b."日價漲跌_pct",
    b."日收減日開",
    b."日收開_昨收_pct",
    b."日收開_開盤_pct",
    b."日振幅",
    b."日振幅_昨收_pct",
    b."日振幅_開盤_pct",
    b."日成交量",
    b."日5最大量_日期",
    b."日5最大量",
    b."日10最大量_日期",
    b."日10最大量",
    b."日20最大量_日期",
    b."日20最大量",
    b."日60最大量_日期",
    b."日60最大量",
    b."日5均量",
    b."日10均量",
    b."日20均量",
    b."日60均量",
    b."日量增減_昨日_pct",
    b."日量增減_五日_pct",
    b."日量增減_十日_pct",
    b."日量增減_二十日_pct",
    b."日量增減_六十日_pct",
    b."日當日價量",
    b."日5日價量",
    b."日10日價量",
    b."日20日價量",
    b."日60日價量",
    b."日K線型態",
    b."日跳空缺口",
    b."日跳空缺口_昨收_pct",
    b."日實體_pct",
    b."日上影_pct",
    b."日下影_pct",
    b."日5MA",
    b."日10MA",
    b."日20MA",
    b."日60MA",
    b."日120MA",
    b."日240MA",
    b."日5均上升幅度_點",
    b."日10均上升幅度_點",
    b."日20均上升幅度_點",
    b."日60均上升幅度_點",
    b."日5均上升幅度_pct",
    b."日10均上升幅度_pct",
    b."日20均上升幅度_pct",
    b."日60均上升幅度_pct",
    b."日5_明日扣抵值",
    b."日10_明日扣抵值",
    b."日20_明日扣抵值",
    b."日60_明日扣抵值",
    b."日5_明日扣抵值影響_點",
    b."日10_明日扣抵值影響_點",
    b."日20_明日扣抵值影響_點",
    b."日60_明日扣抵值影響_點",
    b."日5_明日扣抵值影響_pct",
    b."日10_明日扣抵值影響_pct",
    b."日20_明日扣抵值影響_pct",
    b."日60_明日扣抵值影響_pct",
    b."日5MA乖離率",
    b."日10MA乖離率",
    b."日20MA乖離率",
    b."日60MA乖離率",
    b."日布林標準差",
    b."日布林上軌",
    b."日布林下軌",
    b."日法人多倉",
    b."日法人空倉",
    b."未沖銷_全",
    b."未沖銷_近期",
    b."未沖銷_遠期",
    b."主力_月結算狀態",
    b."主力新建多單",
    b."主力新建空單",
    b."主力淨多空和_5日",
    b."主力淨多空和_10日",
    b."主力淨多空和_20日",
    b."主力淨多空和_60日",
    b."夜盤開",
    b."夜盤收",
    b."夜盤高",
    b."夜盤低",
    b."夜漲跌",
    b."夜價漲跌_pct",
    b."夜成交量",
    b."夜價量結構",
    b."夜K線型態",
    b."夜5MA乖離率",
    b."夜10MA乖離率",
    b."夜20MA乖離率",
    b."夜60MA乖離率",
    b."夜布林標準差",
    b."夜布林上軌",
    b."夜布林下軌",
    b."夜法人多單量",
    b."夜法人空單量",
    b."全市場OI_小台",
    b."三大法人多單_小台",
    b."三大法人空單_小台",
    b."散戶多單_小台",
    b."散戶空單_小台",
    b."散戶多空比_小台",
    b."全市場OI_微台",
    b."三大法人多單_微台",
    b."三大法人空單_微台",
    b."散戶多單_微台",
    b."散戶空單_微台",
    b."散戶多空比_微台",
    b."法人總買超_億",
    b."call_月選",
    b."put_月選",
    b."買賣權比_月選",
    b."call_週三",
    b."put_週三",
    b."買賣權比_週三",
    b."call_週五",
    b."put_週五",
    b."買賣權比_週五",
        CASE
            WHEN ((b."日盤高" IS NULL) OR (b."日盤低" IS NULL) OR (b."日盤收" IS NULL)) THEN NULL::text
            WHEN (abs((b."日盤高" - b."日盤收")) < abs((b."日盤收" - b."日盤低"))) THEN '漲勢盤'::text
            ELSE '跌勢盤'::text
        END AS "日盤走勢",
    t."日盤轉折支撐",
    t."日盤轉折壓力",
        CASE
            WHEN ((b."夜盤高" IS NULL) OR (b."夜盤低" IS NULL) OR (b."夜盤收" IS NULL)) THEN NULL::text
            WHEN (abs((b."夜盤高" - b."夜盤收")) < abs((b."夜盤收" - b."夜盤低"))) THEN '漲勢盤'::text
            ELSE '跌勢盤'::text
        END AS "夜盤走勢",
    t."夜盤轉折支撐",
    t."夜盤轉折壓力",
    b."全位置",
    b."策略_全價量昨日",
    b."策略_全價量五日",
    b."全MA方向_5",
    b."全MA方向_10",
    b."全MA方向_20",
    b."策略_線型",
    b."法人籌碼",
    b."散戶籌碼",
    b."加權籌碼",
    b."期貨籌碼",
    b."選權籌碼",
    b."策略_籌碼",
    b."策略_價量昨日_線型",
    b."策略_價量五日_線型",
    b."策略_價量昨日_籌碼",
    b."策略_價量五日_籌碼",
    b."策略_線型_籌碼",
    b."策略_核心",
    b."建立時間",
    b."夜盤資料來源"
   FROM (report.v_futures_report_daily_without_turning b
     LEFT JOIN report.futures_turning_levels t ON (((t.futures_id = b.futures_id) AND (t."日期" = b."日期"))));


--
-- Name: v_futures_report_daily_strategy_calc; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_report_daily_strategy_calc AS
 WITH lagged AS (
         SELECT src.id,
            src.futures_id,
            src."日期",
            src."全日收",
            src."全成交量",
            src."全5均量",
            src."全5MA",
            src."全10MA",
            src."全20MA",
            src."全布林上軌",
            src."全布林下軌",
            src."策略_籌碼",
            src."法人籌碼",
            lag(src."全日收") OVER w AS prev_close,
            lag(src."全成交量") OVER w AS prev_volume,
            lag(src."全5MA") OVER w AS prev_ma5,
            lag(src."全10MA") OVER w AS prev_ma10,
            lag(src."全20MA") OVER w AS prev_ma20
           FROM report.v_futures_report_daily_without_turning src
          WINDOW w AS (PARTITION BY src.futures_id ORDER BY src."日期")
        ), classified AS (
         SELECT l.id,
            l.futures_id,
            l."日期",
            l."全日收",
            l."全成交量",
            l."全5均量",
            l."全5MA",
            l."全10MA",
            l."全20MA",
            l."全布林上軌",
            l."全布林下軌",
            l."策略_籌碼",
            l."法人籌碼",
            l.prev_close,
            l.prev_volume,
            l.prev_ma5,
            l.prev_ma10,
            l.prev_ma20,
                CASE
                    WHEN (l."全日收" > l.prev_close) THEN '價漲'::text
                    WHEN (l."全日收" < l.prev_close) THEN '價跌'::text
                    WHEN (l."全日收" = l.prev_close) THEN '價同'::text
                    ELSE NULL::text
                END AS full_price,
                CASE
                    WHEN (l."全成交量" > l.prev_volume) THEN '量增'::text
                    WHEN (l."全成交量" < l.prev_volume) THEN '量縮'::text
                    WHEN (l."全成交量" = l.prev_volume) THEN '量同'::text
                    ELSE NULL::text
                END AS volume_yesterday,
                CASE
                    WHEN (l."全成交量" > l."全5均量") THEN '量增'::text
                    WHEN (l."全成交量" < l."全5均量") THEN '量縮'::text
                    WHEN (l."全成交量" = l."全5均量") THEN '量同'::text
                    ELSE NULL::text
                END AS volume_5,
                CASE
                    WHEN (l."全5MA" > l.prev_ma5) THEN '上'::text
                    WHEN (l."全5MA" < l.prev_ma5) THEN '下'::text
                    ELSE NULL::text
                END AS direction_5,
                CASE
                    WHEN (l."全10MA" > l.prev_ma10) THEN '上'::text
                    WHEN (l."全10MA" < l.prev_ma10) THEN '下'::text
                    ELSE NULL::text
                END AS direction_10,
                CASE
                    WHEN (l."全20MA" > l.prev_ma20) THEN '上'::text
                    WHEN (l."全20MA" < l.prev_ma20) THEN '下'::text
                    ELSE NULL::text
                END AS direction_20
           FROM lagged l
        ), trended AS (
         SELECT c.id,
            c.futures_id,
            c."日期",
            c."全日收",
            c."全成交量",
            c."全5均量",
            c."全5MA",
            c."全10MA",
            c."全20MA",
            c."全布林上軌",
            c."全布林下軌",
            c."策略_籌碼",
            c."法人籌碼",
            c.prev_close,
            c.prev_volume,
            c.prev_ma5,
            c.prev_ma10,
            c.prev_ma20,
            c.full_price,
            c.volume_yesterday,
            c.volume_5,
            c.direction_5,
            c.direction_10,
            c.direction_20,
            public.fn_futures_full_trend(c.direction_5, c.direction_10, c.direction_20) AS full_trend
           FROM classified c
        ), positioned AS (
         SELECT t.id,
            t.futures_id,
            t."日期",
            t."全日收",
            t."全成交量",
            t."全5均量",
            t."全5MA",
            t."全10MA",
            t."全20MA",
            t."全布林上軌",
            t."全布林下軌",
            t."策略_籌碼",
            t."法人籌碼",
            t.prev_close,
            t.prev_volume,
            t.prev_ma5,
            t.prev_ma10,
            t.prev_ma20,
            t.full_price,
            t.volume_yesterday,
            t.volume_5,
            t.direction_5,
            t.direction_10,
            t.direction_20,
            t.full_trend,
            public.fn_futures_full_position(t.full_trend, t."全日收", t."全5MA", t."全布林上軌", t."全布林下軌") AS full_position
           FROM trended t
        ), strategies AS (
         SELECT p.id,
            p.futures_id,
            p."日期",
            p."全日收",
            p."全成交量",
            p."全5均量",
            p."全5MA",
            p."全10MA",
            p."全20MA",
            p."全布林上軌",
            p."全布林下軌",
            p."策略_籌碼",
            p."法人籌碼",
            p.prev_close,
            p.prev_volume,
            p.prev_ma5,
            p.prev_ma10,
            p.prev_ma20,
            p.full_price,
            p.volume_yesterday,
            p.volume_5,
            p.direction_5,
            p.direction_10,
            p.direction_20,
            p.full_trend,
            p.full_position,
            public.fn_futures_price_volume_strategy(p.full_price, p.volume_yesterday, p.full_trend, p.full_position) AS strategy_yesterday,
            public.fn_futures_price_volume_strategy(p.full_price, p.volume_5, p.full_trend, p.full_position) AS strategy_5
           FROM positioned p
        )
 SELECT id,
    futures_id,
    "日期",
    full_position AS "全位置",
    strategy_yesterday AS "策略_全價量昨日",
    strategy_5 AS "策略_全價量五日",
    direction_5 AS "全MA方向_5",
    direction_10 AS "全MA方向_10",
    direction_20 AS "全MA方向_20",
    full_trend AS "策略_線型",
    public.fn_futures_price_line_strategy(strategy_yesterday, full_trend, full_position) AS "策略_價量昨日_線型",
    public.fn_futures_price_line_strategy(strategy_5, full_trend, full_position) AS "策略_價量五日_線型",
    public.fn_futures_price_chip_strategy(strategy_yesterday, "策略_籌碼", "法人籌碼") AS "策略_價量昨日_籌碼",
    public.fn_futures_price_chip_strategy(strategy_5, "策略_籌碼", "法人籌碼") AS "策略_價量五日_籌碼",
    public.fn_report_core_strategy(strategy_5, full_trend, full_position, "策略_籌碼", "法人籌碼") AS "策略_核心",
    public.fn_report_line_chip_strategy(full_trend, full_position, "策略_籌碼", "法人籌碼") AS "策略_線型_籌碼",
    public.fn_report_core_strategy(strategy_yesterday, full_trend, full_position, "策略_籌碼", "法人籌碼") AS "策略_核心_昨價量",
    public.fn_report_core_strategy(strategy_5, full_trend, full_position, "策略_籌碼", "法人籌碼") AS "策略_核心_五日價量"
   FROM strategies;


--
-- Name: VIEW v_futures_report_daily_strategy_calc; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON VIEW report.v_futures_report_daily_strategy_calc IS '期貨全日策略窄表；僅保留昨價量／五日價量雙核心，不再提供舊策略_核心。';


--
-- Name: v_futures_report_daily_corrected; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_report_daily_corrected AS
 SELECT src.id,
    src.futures_id,
    src."日期",
    src."年",
    src."月",
    src."星期",
    src."週次",
    src."天別",
    src."全日開",
    src."全日收",
    src."全日高",
    src."全日低",
    src."全漲跌",
    src."全價漲跌_pct",
    src."全收減全開",
    src."全收開_昨收_pct",
    src."全收開_開盤_pct",
    src."全振幅",
    src."全振幅_昨收_pct",
    src."全振幅_開盤_pct",
    src."全成交量",
    src."全5最大量_日期",
    src."全5最大量",
    src."全10最大量_日期",
    src."全10最大量",
    src."全20最大量_日期",
    src."全20最大量",
    src."全60最大量_日期",
    src."全60最大量",
    src."全5均量",
    src."全10均量",
    src."全20均量",
    src."全60均量",
    src."全量增減_昨日_pct",
    src."全量增減_五日_pct",
    src."全量增減_十日_pct",
    src."全量增減_二十日_pct",
    src."全量增減_六十日_pct",
    src."全當日價量",
    src."全5日價量",
    src."全10日價量",
    src."全20日價量",
    src."全60日價量",
    src."全K線型態",
    src."全跳空缺口",
    src."全跳空缺口_昨收_pct",
    src."全實體_pct",
    src."全上影_pct",
    src."全下影_pct",
    src."全5MA",
    src."全10MA",
    src."全20MA",
    src."全60MA",
    src."全120MA",
    src."全240MA",
    src."全5均上升幅度_點",
    src."全10均上升幅度_點",
    src."全20均上升幅度_點",
    src."全60均上升幅度_點",
    src."全5均上升幅度_pct",
    src."全10均上升幅度_pct",
    src."全20均上升幅度_pct",
    src."全60均上升幅度_pct",
    src."全5_明日扣抵值",
    src."全10_明日扣抵值",
    src."全20_明日扣抵值",
    src."全60_明日扣抵值",
    src."全5_明日扣抵值影響_點",
    src."全10_明日扣抵值影響_點",
    src."全20_明日扣抵值影響_點",
    src."全60_明日扣抵值影響_點",
    src."全5_明日扣抵值影響_pct",
    src."全10_明日扣抵值影響_pct",
    src."全20_明日扣抵值影響_pct",
    src."全60_明日扣抵值影響_pct",
    src."全5MA乖離率",
    src."全10MA乖離率",
    src."全20MA乖離率",
    src."全60MA乖離率",
    src."全布林標準差",
    src."全布林上軌",
    src."全布林下軌",
    src."日盤開",
    src."日盤收",
    src."日盤高",
    src."日盤低",
    src."日漲跌",
    src."日價漲跌_pct",
    src."日收減日開",
    src."日收開_昨收_pct",
    src."日收開_開盤_pct",
    src."日振幅",
    src."日振幅_昨收_pct",
    src."日振幅_開盤_pct",
    src."日成交量",
    src."日5最大量_日期",
    src."日5最大量",
    src."日10最大量_日期",
    src."日10最大量",
    src."日20最大量_日期",
    src."日20最大量",
    src."日60最大量_日期",
    src."日60最大量",
    src."日5均量",
    src."日10均量",
    src."日20均量",
    src."日60均量",
    src."日量增減_昨日_pct",
    src."日量增減_五日_pct",
    src."日量增減_十日_pct",
    src."日量增減_二十日_pct",
    src."日量增減_六十日_pct",
    src."日當日價量",
    src."日5日價量",
    src."日10日價量",
    src."日20日價量",
    src."日60日價量",
    src."日K線型態",
    src."日跳空缺口",
    src."日跳空缺口_昨收_pct",
    src."日實體_pct",
    src."日上影_pct",
    src."日下影_pct",
    src."日5MA",
    src."日10MA",
    src."日20MA",
    src."日60MA",
    src."日120MA",
    src."日240MA",
    src."日5均上升幅度_點",
    src."日10均上升幅度_點",
    src."日20均上升幅度_點",
    src."日60均上升幅度_點",
    src."日5均上升幅度_pct",
    src."日10均上升幅度_pct",
    src."日20均上升幅度_pct",
    src."日60均上升幅度_pct",
    src."日5_明日扣抵值",
    src."日10_明日扣抵值",
    src."日20_明日扣抵值",
    src."日60_明日扣抵值",
    src."日5_明日扣抵值影響_點",
    src."日10_明日扣抵值影響_點",
    src."日20_明日扣抵值影響_點",
    src."日60_明日扣抵值影響_點",
    src."日5_明日扣抵值影響_pct",
    src."日10_明日扣抵值影響_pct",
    src."日20_明日扣抵值影響_pct",
    src."日60_明日扣抵值影響_pct",
    src."日5MA乖離率",
    src."日10MA乖離率",
    src."日20MA乖離率",
    src."日60MA乖離率",
    src."日布林標準差",
    src."日布林上軌",
    src."日布林下軌",
    src."日法人多倉",
    src."日法人空倉",
    src."未沖銷_全",
    src."未沖銷_近期",
    src."未沖銷_遠期",
    src."主力_月結算狀態",
    src."主力新建多單",
    src."主力新建空單",
    src."主力淨多空和_5日",
    src."主力淨多空和_10日",
    src."主力淨多空和_20日",
    src."主力淨多空和_60日",
    src."夜盤開",
    src."夜盤收",
    src."夜盤高",
    src."夜盤低",
    src."夜漲跌",
    src."夜價漲跌_pct",
    src."夜成交量",
    src."夜價量結構",
    src."夜K線型態",
    src."夜5MA乖離率",
    src."夜10MA乖離率",
    src."夜20MA乖離率",
    src."夜60MA乖離率",
    src."夜布林標準差",
    src."夜布林上軌",
    src."夜布林下軌",
    src."夜法人多單量",
    src."夜法人空單量",
    src."全市場OI_小台",
    src."三大法人多單_小台",
    src."三大法人空單_小台",
    src."散戶多單_小台",
    src."散戶空單_小台",
    src."散戶多空比_小台",
    src."全市場OI_微台",
    src."三大法人多單_微台",
    src."三大法人空單_微台",
    src."散戶多單_微台",
    src."散戶空單_微台",
    src."散戶多空比_微台",
    src."法人總買超_億",
    src."call_月選",
    src."put_月選",
    src."買賣權比_月選",
    src."call_週三",
    src."put_週三",
    src."買賣權比_週三",
    src."call_週五",
    src."put_週五",
    src."買賣權比_週五",
    calc."全位置",
    calc."策略_全價量昨日",
    calc."策略_全價量五日",
    calc."全MA方向_5",
    calc."全MA方向_10",
    calc."全MA方向_20",
    calc."策略_線型",
    src."法人籌碼",
    src."散戶籌碼",
    src."加權籌碼",
    src."期貨籌碼",
    src."選權籌碼",
    src."策略_籌碼",
    calc."策略_價量昨日_線型",
    calc."策略_價量五日_線型",
    calc."策略_價量昨日_籌碼",
    calc."策略_價量五日_籌碼",
    calc."策略_線型_籌碼",
    calc."策略_核心_昨價量",
    calc."策略_核心_五日價量",
    src."建立時間",
    src."夜盤資料來源"
   FROM (report.v_futures_report_daily_without_turning src
     JOIN report.v_futures_report_daily_strategy_calc calc ON (((calc.id = src.id) AND (calc.futures_id = src.futures_id) AND (calc."日期" = src."日期"))));


--
-- Name: VIEW v_futures_report_daily_corrected; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON VIEW report.v_futures_report_daily_corrected IS '期貨正式報表；舊策略_核心已移除，雙核心位於建立時間之前。';


--
-- Name: v_futures_report_daily_corrected_turning; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_report_daily_corrected_turning AS
 SELECT b.id,
    b.futures_id,
    b."日期",
    b."年",
    b."月",
    b."星期",
    b."週次",
    b."天別",
    b."全日開",
    b."全日收",
    b."全日高",
    b."全日低",
    b."全漲跌",
    b."全價漲跌_pct",
    b."全收減全開",
    b."全收開_昨收_pct",
    b."全收開_開盤_pct",
    b."全振幅",
    b."全振幅_昨收_pct",
    b."全振幅_開盤_pct",
    b."全成交量",
    b."全5最大量_日期",
    b."全5最大量",
    b."全10最大量_日期",
    b."全10最大量",
    b."全20最大量_日期",
    b."全20最大量",
    b."全60最大量_日期",
    b."全60最大量",
    b."全5均量",
    b."全10均量",
    b."全20均量",
    b."全60均量",
    b."全量增減_昨日_pct",
    b."全量增減_五日_pct",
    b."全量增減_十日_pct",
    b."全量增減_二十日_pct",
    b."全量增減_六十日_pct",
    b."全當日價量",
    b."全5日價量",
    b."全10日價量",
    b."全20日價量",
    b."全60日價量",
    b."全K線型態",
    b."全跳空缺口",
    b."全跳空缺口_昨收_pct",
    b."全實體_pct",
    b."全上影_pct",
    b."全下影_pct",
    b."全5MA",
    b."全10MA",
    b."全20MA",
    b."全60MA",
    b."全120MA",
    b."全240MA",
    b."全5均上升幅度_點",
    b."全10均上升幅度_點",
    b."全20均上升幅度_點",
    b."全60均上升幅度_點",
    b."全5均上升幅度_pct",
    b."全10均上升幅度_pct",
    b."全20均上升幅度_pct",
    b."全60均上升幅度_pct",
    b."全5_明日扣抵值",
    b."全10_明日扣抵值",
    b."全20_明日扣抵值",
    b."全60_明日扣抵值",
    b."全5_明日扣抵值影響_點",
    b."全10_明日扣抵值影響_點",
    b."全20_明日扣抵值影響_點",
    b."全60_明日扣抵值影響_點",
    b."全5_明日扣抵值影響_pct",
    b."全10_明日扣抵值影響_pct",
    b."全20_明日扣抵值影響_pct",
    b."全60_明日扣抵值影響_pct",
    b."全5MA乖離率",
    b."全10MA乖離率",
    b."全20MA乖離率",
    b."全60MA乖離率",
    b."全布林標準差",
    b."全布林上軌",
    b."全布林下軌",
    b."日盤開",
    b."日盤收",
    b."日盤高",
    b."日盤低",
    b."日漲跌",
    b."日價漲跌_pct",
    b."日收減日開",
    b."日收開_昨收_pct",
    b."日收開_開盤_pct",
    b."日振幅",
    b."日振幅_昨收_pct",
    b."日振幅_開盤_pct",
    b."日成交量",
    b."日5最大量_日期",
    b."日5最大量",
    b."日10最大量_日期",
    b."日10最大量",
    b."日20最大量_日期",
    b."日20最大量",
    b."日60最大量_日期",
    b."日60最大量",
    b."日5均量",
    b."日10均量",
    b."日20均量",
    b."日60均量",
    b."日量增減_昨日_pct",
    b."日量增減_五日_pct",
    b."日量增減_十日_pct",
    b."日量增減_二十日_pct",
    b."日量增減_六十日_pct",
    b."日當日價量",
    b."日5日價量",
    b."日10日價量",
    b."日20日價量",
    b."日60日價量",
    b."日K線型態",
    b."日跳空缺口",
    b."日跳空缺口_昨收_pct",
    b."日實體_pct",
    b."日上影_pct",
    b."日下影_pct",
    b."日5MA",
    b."日10MA",
    b."日20MA",
    b."日60MA",
    b."日120MA",
    b."日240MA",
    b."日5均上升幅度_點",
    b."日10均上升幅度_點",
    b."日20均上升幅度_點",
    b."日60均上升幅度_點",
    b."日5均上升幅度_pct",
    b."日10均上升幅度_pct",
    b."日20均上升幅度_pct",
    b."日60均上升幅度_pct",
    b."日5_明日扣抵值",
    b."日10_明日扣抵值",
    b."日20_明日扣抵值",
    b."日60_明日扣抵值",
    b."日5_明日扣抵值影響_點",
    b."日10_明日扣抵值影響_點",
    b."日20_明日扣抵值影響_點",
    b."日60_明日扣抵值影響_點",
    b."日5_明日扣抵值影響_pct",
    b."日10_明日扣抵值影響_pct",
    b."日20_明日扣抵值影響_pct",
    b."日60_明日扣抵值影響_pct",
    b."日5MA乖離率",
    b."日10MA乖離率",
    b."日20MA乖離率",
    b."日60MA乖離率",
    b."日布林標準差",
    b."日布林上軌",
    b."日布林下軌",
    b."日法人多倉",
    b."日法人空倉",
    b."未沖銷_全",
    b."未沖銷_近期",
    b."未沖銷_遠期",
    b."主力_月結算狀態",
    b."主力新建多單",
    b."主力新建空單",
    b."主力淨多空和_5日",
    b."主力淨多空和_10日",
    b."主力淨多空和_20日",
    b."主力淨多空和_60日",
    b."夜盤開",
    b."夜盤收",
    b."夜盤高",
    b."夜盤低",
    b."夜漲跌",
    b."夜價漲跌_pct",
    b."夜成交量",
    b."夜價量結構",
    b."夜K線型態",
    b."夜5MA乖離率",
    b."夜10MA乖離率",
    b."夜20MA乖離率",
    b."夜60MA乖離率",
    b."夜布林標準差",
    b."夜布林上軌",
    b."夜布林下軌",
    b."夜法人多單量",
    b."夜法人空單量",
    b."全市場OI_小台",
    b."三大法人多單_小台",
    b."三大法人空單_小台",
    b."散戶多單_小台",
    b."散戶空單_小台",
    b."散戶多空比_小台",
    b."全市場OI_微台",
    b."三大法人多單_微台",
    b."三大法人空單_微台",
    b."散戶多單_微台",
    b."散戶空單_微台",
    b."散戶多空比_微台",
    b."法人總買超_億",
    b."call_月選",
    b."put_月選",
    b."買賣權比_月選",
    b."call_週三",
    b."put_週三",
    b."買賣權比_週三",
    b."call_週五",
    b."put_週五",
    b."買賣權比_週五",
        CASE
            WHEN ((b."日盤高" IS NULL) OR (b."日盤低" IS NULL) OR (b."日盤收" IS NULL)) THEN NULL::text
            WHEN (abs((b."日盤高" - b."日盤收")) < abs((b."日盤收" - b."日盤低"))) THEN '漲勢盤'::text
            ELSE '跌勢盤'::text
        END AS "日盤走勢",
    t."日盤轉折支撐",
    t."日盤轉折壓力",
        CASE
            WHEN ((b."夜盤高" IS NULL) OR (b."夜盤低" IS NULL) OR (b."夜盤收" IS NULL)) THEN NULL::text
            WHEN (abs((b."夜盤高" - b."夜盤收")) < abs((b."夜盤收" - b."夜盤低"))) THEN '漲勢盤'::text
            ELSE '跌勢盤'::text
        END AS "夜盤走勢",
    t."夜盤轉折支撐",
    t."夜盤轉折壓力",
    b."全位置",
    b."策略_全價量昨日",
    b."策略_全價量五日",
    b."全MA方向_5",
    b."全MA方向_10",
    b."全MA方向_20",
    b."策略_線型",
    b."法人籌碼",
    b."散戶籌碼",
    b."加權籌碼",
    b."期貨籌碼",
    b."選權籌碼",
    b."策略_籌碼",
    b."策略_價量昨日_線型",
    b."策略_價量五日_線型",
    b."策略_價量昨日_籌碼",
    b."策略_價量五日_籌碼",
    b."策略_線型_籌碼",
    b."策略_核心_昨價量",
    b."策略_核心_五日價量",
    b."建立時間",
    b."夜盤資料來源"
   FROM (report.v_futures_report_daily_corrected b
     LEFT JOIN report.futures_turning_levels t ON (((t.futures_id = b.futures_id) AND (t."日期" = b."日期"))));


--
-- Name: v_futures_report_daily_corrected_turning_without_session_trend; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_report_daily_corrected_turning_without_session_trend AS
 SELECT b.id,
    b.futures_id,
    b."日期",
    b."年",
    b."月",
    b."星期",
    b."週次",
    b."天別",
    b."全日開",
    b."全日收",
    b."全日高",
    b."全日低",
    b."全漲跌",
    b."全價漲跌_pct",
    b."全收減全開",
    b."全收開_昨收_pct",
    b."全收開_開盤_pct",
    b."全振幅",
    b."全振幅_昨收_pct",
    b."全振幅_開盤_pct",
    b."全成交量",
    b."全5最大量_日期",
    b."全5最大量",
    b."全10最大量_日期",
    b."全10最大量",
    b."全20最大量_日期",
    b."全20最大量",
    b."全60最大量_日期",
    b."全60最大量",
    b."全5均量",
    b."全10均量",
    b."全20均量",
    b."全60均量",
    b."全量增減_昨日_pct",
    b."全量增減_五日_pct",
    b."全量增減_十日_pct",
    b."全量增減_二十日_pct",
    b."全量增減_六十日_pct",
    b."全當日價量",
    b."全5日價量",
    b."全10日價量",
    b."全20日價量",
    b."全60日價量",
    b."全K線型態",
    b."全跳空缺口",
    b."全跳空缺口_昨收_pct",
    b."全實體_pct",
    b."全上影_pct",
    b."全下影_pct",
    b."全5MA",
    b."全10MA",
    b."全20MA",
    b."全60MA",
    b."全120MA",
    b."全240MA",
    b."全5均上升幅度_點",
    b."全10均上升幅度_點",
    b."全20均上升幅度_點",
    b."全60均上升幅度_點",
    b."全5均上升幅度_pct",
    b."全10均上升幅度_pct",
    b."全20均上升幅度_pct",
    b."全60均上升幅度_pct",
    b."全5_明日扣抵值",
    b."全10_明日扣抵值",
    b."全20_明日扣抵值",
    b."全60_明日扣抵值",
    b."全5_明日扣抵值影響_點",
    b."全10_明日扣抵值影響_點",
    b."全20_明日扣抵值影響_點",
    b."全60_明日扣抵值影響_點",
    b."全5_明日扣抵值影響_pct",
    b."全10_明日扣抵值影響_pct",
    b."全20_明日扣抵值影響_pct",
    b."全60_明日扣抵值影響_pct",
    b."全5MA乖離率",
    b."全10MA乖離率",
    b."全20MA乖離率",
    b."全60MA乖離率",
    b."全布林標準差",
    b."全布林上軌",
    b."全布林下軌",
    b."日盤開",
    b."日盤收",
    b."日盤高",
    b."日盤低",
    b."日漲跌",
    b."日價漲跌_pct",
    b."日收減日開",
    b."日收開_昨收_pct",
    b."日收開_開盤_pct",
    b."日振幅",
    b."日振幅_昨收_pct",
    b."日振幅_開盤_pct",
    b."日成交量",
    b."日5最大量_日期",
    b."日5最大量",
    b."日10最大量_日期",
    b."日10最大量",
    b."日20最大量_日期",
    b."日20最大量",
    b."日60最大量_日期",
    b."日60最大量",
    b."日5均量",
    b."日10均量",
    b."日20均量",
    b."日60均量",
    b."日量增減_昨日_pct",
    b."日量增減_五日_pct",
    b."日量增減_十日_pct",
    b."日量增減_二十日_pct",
    b."日量增減_六十日_pct",
    b."日當日價量",
    b."日5日價量",
    b."日10日價量",
    b."日20日價量",
    b."日60日價量",
    b."日K線型態",
    b."日跳空缺口",
    b."日跳空缺口_昨收_pct",
    b."日實體_pct",
    b."日上影_pct",
    b."日下影_pct",
    b."日5MA",
    b."日10MA",
    b."日20MA",
    b."日60MA",
    b."日120MA",
    b."日240MA",
    b."日5均上升幅度_點",
    b."日10均上升幅度_點",
    b."日20均上升幅度_點",
    b."日60均上升幅度_點",
    b."日5均上升幅度_pct",
    b."日10均上升幅度_pct",
    b."日20均上升幅度_pct",
    b."日60均上升幅度_pct",
    b."日5_明日扣抵值",
    b."日10_明日扣抵值",
    b."日20_明日扣抵值",
    b."日60_明日扣抵值",
    b."日5_明日扣抵值影響_點",
    b."日10_明日扣抵值影響_點",
    b."日20_明日扣抵值影響_點",
    b."日60_明日扣抵值影響_點",
    b."日5_明日扣抵值影響_pct",
    b."日10_明日扣抵值影響_pct",
    b."日20_明日扣抵值影響_pct",
    b."日60_明日扣抵值影響_pct",
    b."日5MA乖離率",
    b."日10MA乖離率",
    b."日20MA乖離率",
    b."日60MA乖離率",
    b."日布林標準差",
    b."日布林上軌",
    b."日布林下軌",
    b."日法人多倉",
    b."日法人空倉",
    b."未沖銷_全",
    b."未沖銷_近期",
    b."未沖銷_遠期",
    b."主力_月結算狀態",
    b."主力新建多單",
    b."主力新建空單",
    b."主力淨多空和_5日",
    b."主力淨多空和_10日",
    b."主力淨多空和_20日",
    b."主力淨多空和_60日",
    b."夜盤開",
    b."夜盤收",
    b."夜盤高",
    b."夜盤低",
    b."夜漲跌",
    b."夜價漲跌_pct",
    b."夜成交量",
    b."夜價量結構",
    b."夜K線型態",
    b."夜5MA乖離率",
    b."夜10MA乖離率",
    b."夜20MA乖離率",
    b."夜60MA乖離率",
    b."夜布林標準差",
    b."夜布林上軌",
    b."夜布林下軌",
    b."夜法人多單量",
    b."夜法人空單量",
    b."全市場OI_小台",
    b."三大法人多單_小台",
    b."三大法人空單_小台",
    b."散戶多單_小台",
    b."散戶空單_小台",
    b."散戶多空比_小台",
    b."全市場OI_微台",
    b."三大法人多單_微台",
    b."三大法人空單_微台",
    b."散戶多單_微台",
    b."散戶空單_微台",
    b."散戶多空比_微台",
    b."法人總買超_億",
    b."call_月選",
    b."put_月選",
    b."買賣權比_月選",
    b."call_週三",
    b."put_週三",
    b."買賣權比_週三",
    b."call_週五",
    b."put_週五",
    b."買賣權比_週五",
    t."日盤轉折支撐",
    t."日盤轉折壓力",
    t."夜盤轉折支撐",
    t."夜盤轉折壓力",
    b."全位置",
    b."策略_全價量昨日",
    b."策略_全價量五日",
    b."全MA方向_5",
    b."全MA方向_10",
    b."全MA方向_20",
    b."策略_線型",
    b."法人籌碼",
    b."散戶籌碼",
    b."加權籌碼",
    b."期貨籌碼",
    b."選權籌碼",
    b."策略_籌碼",
    b."策略_價量昨日_線型",
    b."策略_價量五日_線型",
    b."策略_價量昨日_籌碼",
    b."策略_價量五日_籌碼",
    b."策略_線型_籌碼",
    b."策略_核心_昨價量",
    b."策略_核心_五日價量",
    b."建立時間",
    b."夜盤資料來源"
   FROM (report.v_futures_report_daily_corrected b
     LEFT JOIN report.futures_turning_levels t ON (((t.futures_id = b.futures_id) AND (t."日期" = b."日期"))));


--
-- Name: v_futures_report_daily_without_session_trend; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_futures_report_daily_without_session_trend AS
 SELECT b.id,
    b.futures_id,
    b."日期",
    b."年",
    b."月",
    b."星期",
    b."週次",
    b."天別",
    b."全日開",
    b."全日收",
    b."全日高",
    b."全日低",
    b."全漲跌",
    b."全價漲跌_pct",
    b."全收減全開",
    b."全收開_昨收_pct",
    b."全收開_開盤_pct",
    b."全振幅",
    b."全振幅_昨收_pct",
    b."全振幅_開盤_pct",
    b."全成交量",
    b."全5最大量_日期",
    b."全5最大量",
    b."全10最大量_日期",
    b."全10最大量",
    b."全20最大量_日期",
    b."全20最大量",
    b."全60最大量_日期",
    b."全60最大量",
    b."全5均量",
    b."全10均量",
    b."全20均量",
    b."全60均量",
    b."全量增減_昨日_pct",
    b."全量增減_五日_pct",
    b."全量增減_十日_pct",
    b."全量增減_二十日_pct",
    b."全量增減_六十日_pct",
    b."全當日價量",
    b."全5日價量",
    b."全10日價量",
    b."全20日價量",
    b."全60日價量",
    b."全K線型態",
    b."全跳空缺口",
    b."全跳空缺口_昨收_pct",
    b."全實體_pct",
    b."全上影_pct",
    b."全下影_pct",
    b."全5MA",
    b."全10MA",
    b."全20MA",
    b."全60MA",
    b."全120MA",
    b."全240MA",
    b."全5均上升幅度_點",
    b."全10均上升幅度_點",
    b."全20均上升幅度_點",
    b."全60均上升幅度_點",
    b."全5均上升幅度_pct",
    b."全10均上升幅度_pct",
    b."全20均上升幅度_pct",
    b."全60均上升幅度_pct",
    b."全5_明日扣抵值",
    b."全10_明日扣抵值",
    b."全20_明日扣抵值",
    b."全60_明日扣抵值",
    b."全5_明日扣抵值影響_點",
    b."全10_明日扣抵值影響_點",
    b."全20_明日扣抵值影響_點",
    b."全60_明日扣抵值影響_點",
    b."全5_明日扣抵值影響_pct",
    b."全10_明日扣抵值影響_pct",
    b."全20_明日扣抵值影響_pct",
    b."全60_明日扣抵值影響_pct",
    b."全5MA乖離率",
    b."全10MA乖離率",
    b."全20MA乖離率",
    b."全60MA乖離率",
    b."全布林標準差",
    b."全布林上軌",
    b."全布林下軌",
    b."日盤開",
    b."日盤收",
    b."日盤高",
    b."日盤低",
    b."日漲跌",
    b."日價漲跌_pct",
    b."日收減日開",
    b."日收開_昨收_pct",
    b."日收開_開盤_pct",
    b."日振幅",
    b."日振幅_昨收_pct",
    b."日振幅_開盤_pct",
    b."日成交量",
    b."日5最大量_日期",
    b."日5最大量",
    b."日10最大量_日期",
    b."日10最大量",
    b."日20最大量_日期",
    b."日20最大量",
    b."日60最大量_日期",
    b."日60最大量",
    b."日5均量",
    b."日10均量",
    b."日20均量",
    b."日60均量",
    b."日量增減_昨日_pct",
    b."日量增減_五日_pct",
    b."日量增減_十日_pct",
    b."日量增減_二十日_pct",
    b."日量增減_六十日_pct",
    b."日當日價量",
    b."日5日價量",
    b."日10日價量",
    b."日20日價量",
    b."日60日價量",
    b."日K線型態",
    b."日跳空缺口",
    b."日跳空缺口_昨收_pct",
    b."日實體_pct",
    b."日上影_pct",
    b."日下影_pct",
    b."日5MA",
    b."日10MA",
    b."日20MA",
    b."日60MA",
    b."日120MA",
    b."日240MA",
    b."日5均上升幅度_點",
    b."日10均上升幅度_點",
    b."日20均上升幅度_點",
    b."日60均上升幅度_點",
    b."日5均上升幅度_pct",
    b."日10均上升幅度_pct",
    b."日20均上升幅度_pct",
    b."日60均上升幅度_pct",
    b."日5_明日扣抵值",
    b."日10_明日扣抵值",
    b."日20_明日扣抵值",
    b."日60_明日扣抵值",
    b."日5_明日扣抵值影響_點",
    b."日10_明日扣抵值影響_點",
    b."日20_明日扣抵值影響_點",
    b."日60_明日扣抵值影響_點",
    b."日5_明日扣抵值影響_pct",
    b."日10_明日扣抵值影響_pct",
    b."日20_明日扣抵值影響_pct",
    b."日60_明日扣抵值影響_pct",
    b."日5MA乖離率",
    b."日10MA乖離率",
    b."日20MA乖離率",
    b."日60MA乖離率",
    b."日布林標準差",
    b."日布林上軌",
    b."日布林下軌",
    b."日法人多倉",
    b."日法人空倉",
    b."未沖銷_全",
    b."未沖銷_近期",
    b."未沖銷_遠期",
    b."主力_月結算狀態",
    b."主力新建多單",
    b."主力新建空單",
    b."主力淨多空和_5日",
    b."主力淨多空和_10日",
    b."主力淨多空和_20日",
    b."主力淨多空和_60日",
    b."夜盤開",
    b."夜盤收",
    b."夜盤高",
    b."夜盤低",
    b."夜漲跌",
    b."夜價漲跌_pct",
    b."夜成交量",
    b."夜價量結構",
    b."夜K線型態",
    b."夜5MA乖離率",
    b."夜10MA乖離率",
    b."夜20MA乖離率",
    b."夜60MA乖離率",
    b."夜布林標準差",
    b."夜布林上軌",
    b."夜布林下軌",
    b."夜法人多單量",
    b."夜法人空單量",
    b."全市場OI_小台",
    b."三大法人多單_小台",
    b."三大法人空單_小台",
    b."散戶多單_小台",
    b."散戶空單_小台",
    b."散戶多空比_小台",
    b."全市場OI_微台",
    b."三大法人多單_微台",
    b."三大法人空單_微台",
    b."散戶多單_微台",
    b."散戶空單_微台",
    b."散戶多空比_微台",
    b."法人總買超_億",
    b."call_月選",
    b."put_月選",
    b."買賣權比_月選",
    b."call_週三",
    b."put_週三",
    b."買賣權比_週三",
    b."call_週五",
    b."put_週五",
    b."買賣權比_週五",
    t."日盤轉折支撐",
    t."日盤轉折壓力",
    t."夜盤轉折支撐",
    t."夜盤轉折壓力",
    b."全位置",
    b."策略_全價量昨日",
    b."策略_全價量五日",
    b."全MA方向_5",
    b."全MA方向_10",
    b."全MA方向_20",
    b."策略_線型",
    b."法人籌碼",
    b."散戶籌碼",
    b."加權籌碼",
    b."期貨籌碼",
    b."選權籌碼",
    b."策略_籌碼",
    b."策略_價量昨日_線型",
    b."策略_價量五日_線型",
    b."策略_價量昨日_籌碼",
    b."策略_價量五日_籌碼",
    b."策略_線型_籌碼",
    b."策略_核心",
    b."建立時間",
    b."夜盤資料來源"
   FROM (report.v_futures_report_daily_without_turning b
     LEFT JOIN report.futures_turning_levels t ON (((t.futures_id = b.futures_id) AND (t."日期" = b."日期"))));


--
-- Name: v_ml_model_performance; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_ml_model_performance AS
 WITH latest_prediction AS (
         SELECT DISTINCT ON (ml_prediction.model_version_id, ml_prediction.target_date) ml_prediction.prediction_id,
            ml_prediction.run_id,
            ml_prediction.model_version_id,
            ml_prediction.target_date,
            ml_prediction.feature_date,
            ml_prediction.trained_through_date,
            ml_prediction.predicted_class,
            ml_prediction.predicted_label,
            ml_prediction.probability_down,
            ml_prediction.probability_flat,
            ml_prediction.probability_up,
            ml_prediction.confidence,
            ml_prediction.feature_values,
            ml_prediction.actual_class,
            ml_prediction.actual_label,
            ml_prediction.actual_return_pct,
            ml_prediction.is_correct,
            ml_prediction.predicted_at,
            ml_prediction.actual_updated_at
           FROM ml.ml_prediction
          ORDER BY ml_prediction.model_version_id, ml_prediction.target_date, ml_prediction.predicted_at DESC, ml_prediction.prediction_id DESC
        )
 SELECT m.model_key,
    m.model_name,
    v.version_no,
    v.algorithm,
    count(p.prediction_id) AS prediction_count,
    count(p.actual_class) AS realized_count,
    count(*) FILTER (WHERE p.is_correct) AS correct_count,
    round(((100.0 * (count(*) FILTER (WHERE p.is_correct))::numeric) / (NULLIF(count(p.actual_class), 0))::numeric), 2) AS accuracy_pct,
    min(p.target_date) AS first_prediction_date,
    max(p.target_date) AS last_prediction_date
   FROM ((ml.ml_model m
     JOIN ml.ml_model_version v ON ((v.model_id = m.model_id)))
     LEFT JOIN latest_prediction p ON ((p.model_version_id = v.model_version_id)))
  GROUP BY m.model_key, m.model_name, v.version_no, v.algorithm;


--
-- Name: v_taiex_report_base; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_taiex_report_base AS
 WITH base AS (
         SELECT stock_report_daily.id,
            stock_report_daily."日期",
            stock_report_daily."股票代號",
            stock_report_daily."年",
            stock_report_daily."月",
            stock_report_daily."星期",
            stock_report_daily."週次",
            stock_report_daily."天別",
            stock_report_daily."開盤價",
            stock_report_daily."收盤價",
            stock_report_daily."收盤_開盤",
            stock_report_daily."最高價",
            stock_report_daily."最低價",
            stock_report_daily."漲跌",
            stock_report_daily."日振幅",
            stock_report_daily."漲跌幅_pct",
            stock_report_daily."日振幅_昨收_pct",
            stock_report_daily."日振福_開盤_pct",
            stock_report_daily."成交量",
            stock_report_daily."實體_pct",
            stock_report_daily."上影_pct",
            stock_report_daily."下影_pct",
            stock_report_daily."K線型態",
            stock_report_daily."跳空缺口",
            stock_report_daily."跳空缺口_昨收_pct",
            stock_report_daily."當日價量",
            stock_report_daily."5日價量",
            stock_report_daily."10日價量",
            stock_report_daily."20日價量",
            stock_report_daily."60日價量",
            stock_report_daily."總成交金額_億",
            stock_report_daily."法人總買超_億",
            stock_report_daily."買超_外資_億",
            stock_report_daily."買超_投信_億",
            stock_report_daily."買超_自營商_億",
            stock_report_daily."買超_融資_億",
            stock_report_daily."法人淨買賣超_扣融資_億",
            stock_report_daily."法人買賣超_總成交_pct",
            stock_report_daily."漲跌幅百分位數",
            stock_report_daily."漲跌幅級距",
            stock_report_daily."三大法人買賣超百分位數",
            stock_report_daily."三大法人買賣超級距",
            stock_report_daily."資金走向判定",
            stock_report_daily.is_complete,
            stock_report_daily.updated_at
           FROM core.stock_report_daily
          WHERE (stock_report_daily."股票代號" = 'TAIEX'::text)
        ), base_idx AS (
         SELECT b.id,
            b."日期",
            b."股票代號",
            b."年",
            b."月",
            b."星期",
            b."週次",
            b."天別",
            b."開盤價",
            b."收盤價",
            b."收盤_開盤",
            b."最高價",
            b."最低價",
            b."漲跌",
            b."日振幅",
            b."漲跌幅_pct",
            b."日振幅_昨收_pct",
            b."日振福_開盤_pct",
            b."成交量",
            b."實體_pct",
            b."上影_pct",
            b."下影_pct",
            b."K線型態",
            b."跳空缺口",
            b."跳空缺口_昨收_pct",
            b."當日價量",
            b."5日價量",
            b."10日價量",
            b."20日價量",
            b."60日價量",
            b."總成交金額_億",
            b."法人總買超_億",
            b."買超_外資_億",
            b."買超_投信_億",
            b."買超_自營商_億",
            b."買超_融資_億",
            b."法人淨買賣超_扣融資_億",
            b."法人買賣超_總成交_pct",
            b."漲跌幅百分位數",
            b."漲跌幅級距",
            b."三大法人買賣超百分位數",
            b."三大法人買賣超級距",
            b."資金走向判定",
            b.is_complete,
            b.updated_at,
            row_number() OVER (ORDER BY b."日期") AS rn
           FROM base b
        ), calc1 AS (
         SELECT b.id,
            b."日期",
            b."股票代號",
            b."年",
            b."月",
            b."星期",
            b."週次",
            b."天別",
            b."開盤價",
            b."收盤價",
            b."收盤_開盤",
            b."最高價",
            b."最低價",
            b."漲跌",
            b."日振幅",
            b."漲跌幅_pct",
            b."日振幅_昨收_pct",
            b."日振福_開盤_pct",
            b."成交量",
            b."實體_pct",
            b."上影_pct",
            b."下影_pct",
            b."K線型態",
            b."跳空缺口",
            b."跳空缺口_昨收_pct",
            b."當日價量",
            b."5日價量",
            b."10日價量",
            b."20日價量",
            b."60日價量",
            b."總成交金額_億",
            b."法人總買超_億",
            b."買超_外資_億",
            b."買超_投信_億",
            b."買超_自營商_億",
            b."買超_融資_億",
            b."法人淨買賣超_扣融資_億",
            b."法人買賣超_總成交_pct",
            b."漲跌幅百分位數",
            b."漲跌幅級距",
            b."三大法人買賣超百分位數",
            b."三大法人買賣超級距",
            b."資金走向判定",
            b.is_complete,
            b.updated_at,
            b.rn,
            lag(b."收盤價", 1) OVER (ORDER BY b.rn) AS "昨收",
            lag(b."成交量", 1) OVER (ORDER BY b.rn) AS "昨量",
                CASE
                    WHEN (count(b."收盤價") OVER (ORDER BY b.rn ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) = 5) THEN avg(b."收盤價") OVER (ORDER BY b.rn ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "MA5_calc",
                CASE
                    WHEN (count(b."收盤價") OVER (ORDER BY b.rn ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) = 10) THEN avg(b."收盤價") OVER (ORDER BY b.rn ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "MA10_calc",
                CASE
                    WHEN (count(b."收盤價") OVER (ORDER BY b.rn ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) = 20) THEN avg(b."收盤價") OVER (ORDER BY b.rn ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "MA20_calc",
                CASE
                    WHEN (count(b."收盤價") OVER (ORDER BY b.rn ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) = 60) THEN avg(b."收盤價") OVER (ORDER BY b.rn ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "MA60_calc",
                CASE
                    WHEN (count(b."成交量") OVER (ORDER BY b.rn ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) = 5) THEN avg(b."成交量") OVER (ORDER BY b.rn ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "VOLMA5_calc",
                CASE
                    WHEN (count(b."成交量") OVER (ORDER BY b.rn ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) = 10) THEN avg(b."成交量") OVER (ORDER BY b.rn ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "VOLMA10_calc",
                CASE
                    WHEN (count(b."成交量") OVER (ORDER BY b.rn ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) = 20) THEN avg(b."成交量") OVER (ORDER BY b.rn ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "VOLMA20_calc",
                CASE
                    WHEN (count(b."成交量") OVER (ORDER BY b.rn ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) = 60) THEN avg(b."成交量") OVER (ORDER BY b.rn ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
                    ELSE NULL::double precision
                END AS "VOLMA60_calc",
            lag(b."收盤價", 4) OVER (ORDER BY b.rn) AS "抵5_calc",
            lag(b."收盤價", 9) OVER (ORDER BY b.rn) AS "抵10_calc",
            lag(b."收盤價", 19) OVER (ORDER BY b.rn) AS "抵20_calc",
            lag(b."收盤價", 59) OVER (ORDER BY b.rn) AS "抵60_calc",
                CASE
                    WHEN (b.rn >= 5) THEN ( SELECT max(x."成交量") AS max
                       FROM base_idx x
                      WHERE ((x.rn >= (b.rn - 4)) AND (x.rn <= b.rn)))
                    ELSE NULL::double precision
                END AS "MAXVOL5_calc",
                CASE
                    WHEN (b.rn >= 10) THEN ( SELECT max(x."成交量") AS max
                       FROM base_idx x
                      WHERE ((x.rn >= (b.rn - 9)) AND (x.rn <= b.rn)))
                    ELSE NULL::double precision
                END AS "MAXVOL10_calc",
                CASE
                    WHEN (b.rn >= 20) THEN ( SELECT max(x."成交量") AS max
                       FROM base_idx x
                      WHERE ((x.rn >= (b.rn - 19)) AND (x.rn <= b.rn)))
                    ELSE NULL::double precision
                END AS "MAXVOL20_calc",
                CASE
                    WHEN (b.rn >= 60) THEN ( SELECT max(x."成交量") AS max
                       FROM base_idx x
                      WHERE ((x.rn >= (b.rn - 59)) AND (x.rn <= b.rn)))
                    ELSE NULL::double precision
                END AS "MAXVOL60_calc",
                CASE
                    WHEN (b.rn >= 5) THEN ( SELECT x."日期"
                       FROM base_idx x
                      WHERE ((x.rn >= (b.rn - 4)) AND (x.rn <= b.rn))
                      ORDER BY x."成交量" DESC, x."日期" DESC
                     LIMIT 1)
                    ELSE NULL::date
                END AS "MAXVOL5_DATE_calc",
                CASE
                    WHEN (b.rn >= 10) THEN ( SELECT x."日期"
                       FROM base_idx x
                      WHERE ((x.rn >= (b.rn - 9)) AND (x.rn <= b.rn))
                      ORDER BY x."成交量" DESC, x."日期" DESC
                     LIMIT 1)
                    ELSE NULL::date
                END AS "MAXVOL10_DATE_calc",
                CASE
                    WHEN (b.rn >= 20) THEN ( SELECT x."日期"
                       FROM base_idx x
                      WHERE ((x.rn >= (b.rn - 19)) AND (x.rn <= b.rn))
                      ORDER BY x."成交量" DESC, x."日期" DESC
                     LIMIT 1)
                    ELSE NULL::date
                END AS "MAXVOL20_DATE_calc",
                CASE
                    WHEN (b.rn >= 60) THEN ( SELECT x."日期"
                       FROM base_idx x
                      WHERE ((x.rn >= (b.rn - 59)) AND (x.rn <= b.rn))
                      ORDER BY x."成交量" DESC, x."日期" DESC
                     LIMIT 1)
                    ELSE NULL::date
                END AS "MAXVOL60_DATE_calc"
           FROM base_idx b
        ), calc2 AS (
         SELECT c.id,
            c."日期",
            c."股票代號",
            c."年",
            c."月",
            c."星期",
            c."週次",
            c."天別",
            c."開盤價",
            c."收盤價",
            c."收盤_開盤",
            c."最高價",
            c."最低價",
            c."漲跌",
            c."日振幅",
            c."漲跌幅_pct",
            c."日振幅_昨收_pct",
            c."日振福_開盤_pct",
            c."成交量",
            c."實體_pct",
            c."上影_pct",
            c."下影_pct",
            c."K線型態",
            c."跳空缺口",
            c."跳空缺口_昨收_pct",
            c."當日價量",
            c."5日價量",
            c."10日價量",
            c."20日價量",
            c."60日價量",
            c."總成交金額_億",
            c."法人總買超_億",
            c."買超_外資_億",
            c."買超_投信_億",
            c."買超_自營商_億",
            c."買超_融資_億",
            c."法人淨買賣超_扣融資_億",
            c."法人買賣超_總成交_pct",
            c."漲跌幅百分位數",
            c."漲跌幅級距",
            c."三大法人買賣超百分位數",
            c."三大法人買賣超級距",
            c."資金走向判定",
            c.is_complete,
            c.updated_at,
            c.rn,
            c."昨收",
            c."昨量",
            c."MA5_calc",
            c."MA10_calc",
            c."MA20_calc",
            c."MA60_calc",
            c."VOLMA5_calc",
            c."VOLMA10_calc",
            c."VOLMA20_calc",
            c."VOLMA60_calc",
            c."抵5_calc",
            c."抵10_calc",
            c."抵20_calc",
            c."抵60_calc",
            c."MAXVOL5_calc",
            c."MAXVOL10_calc",
            c."MAXVOL20_calc",
            c."MAXVOL60_calc",
            c."MAXVOL5_DATE_calc",
            c."MAXVOL10_DATE_calc",
            c."MAXVOL20_DATE_calc",
            c."MAXVOL60_DATE_calc",
            (c."收盤價" - c."開盤價") AS "收盤開盤_calc",
            (c."最高價" - c."最低價") AS "日振幅_calc",
            (c."收盤價" - c."昨收") AS "漲跌_calc",
                CASE
                    WHEN ((c."昨收" IS NULL) OR (c."昨收" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."收盤價" - c."昨收") / c."昨收")
                END AS "漲跌幅_pct_calc",
                CASE
                    WHEN ((c."昨收" IS NULL) OR (c."昨收" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."最高價" - c."最低價") / c."昨收")
                END AS "日振幅_昨收_pct_calc",
                CASE
                    WHEN ((c."開盤價" IS NULL) OR (c."開盤價" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."最高價" - c."最低價") / c."開盤價")
                END AS "日振福_開盤_pct_calc",
                CASE
                    WHEN ((c."昨量" IS NULL) OR (c."昨量" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."成交量" - c."昨量") / c."昨量")
                END AS "量增率_pct_calc",
                CASE
                    WHEN ((c."VOLMA5_calc" IS NULL) OR (c."VOLMA5_calc" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."成交量" - c."VOLMA5_calc") / c."VOLMA5_calc")
                END AS "量增率5_pct_calc",
                CASE
                    WHEN ((c."VOLMA10_calc" IS NULL) OR (c."VOLMA10_calc" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."成交量" - c."VOLMA10_calc") / c."VOLMA10_calc")
                END AS "量增率10_pct_calc",
                CASE
                    WHEN ((c."VOLMA20_calc" IS NULL) OR (c."VOLMA20_calc" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."成交量" - c."VOLMA20_calc") / c."VOLMA20_calc")
                END AS "量增率20_pct_calc",
                CASE
                    WHEN ((c."VOLMA60_calc" IS NULL) OR (c."VOLMA60_calc" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."成交量" - c."VOLMA60_calc") / c."VOLMA60_calc")
                END AS "量增率60_pct_calc",
                CASE
                    WHEN ((c."收盤價" IS NULL) OR (c."收盤價" = (0)::double precision) OR (c."抵5_calc" IS NULL)) THEN NULL::double precision
                    ELSE ((c."收盤價" - c."抵5_calc") / c."收盤價")
                END AS "抵5影響_pct_calc",
                CASE
                    WHEN ((c."收盤價" IS NULL) OR (c."收盤價" = (0)::double precision) OR (c."抵10_calc" IS NULL)) THEN NULL::double precision
                    ELSE ((c."收盤價" - c."抵10_calc") / c."收盤價")
                END AS "抵10影響_pct_calc",
                CASE
                    WHEN ((c."收盤價" IS NULL) OR (c."收盤價" = (0)::double precision) OR (c."抵20_calc" IS NULL)) THEN NULL::double precision
                    ELSE ((c."收盤價" - c."抵20_calc") / c."收盤價")
                END AS "抵20影響_pct_calc",
                CASE
                    WHEN ((c."收盤價" IS NULL) OR (c."收盤價" = (0)::double precision) OR (c."抵60_calc" IS NULL)) THEN NULL::double precision
                    ELSE ((c."收盤價" - c."抵60_calc") / c."收盤價")
                END AS "抵60影響_pct_calc",
            (c."收盤價" - c."抵5_calc") AS "抵5影響點_calc",
            (c."收盤價" - c."抵10_calc") AS "抵10影響點_calc",
            (c."收盤價" - c."抵20_calc") AS "抵20影響點_calc",
            (c."收盤價" - c."抵60_calc") AS "抵60影響點_calc",
                CASE
                    WHEN ((c."MA5_calc" IS NULL) OR (c."MA5_calc" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."收盤價" - c."MA5_calc") / c."MA5_calc")
                END AS "乖離5_calc",
                CASE
                    WHEN ((c."MA10_calc" IS NULL) OR (c."MA10_calc" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."收盤價" - c."MA10_calc") / c."MA10_calc")
                END AS "乖離10_calc",
                CASE
                    WHEN ((c."MA20_calc" IS NULL) OR (c."MA20_calc" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."收盤價" - c."MA20_calc") / c."MA20_calc")
                END AS "乖離20_calc",
                CASE
                    WHEN ((c."MA60_calc" IS NULL) OR (c."MA60_calc" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."收盤價" - c."MA60_calc") / c."MA60_calc")
                END AS "乖離60_calc",
                CASE
                    WHEN ((c."昨收" IS NULL) OR (c."昨收" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."收盤價" - c."開盤價") / c."昨收")
                END AS "實體_昨收_pct_calc",
                CASE
                    WHEN ((c."開盤價" IS NULL) OR (c."開盤價" = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."收盤價" - c."開盤價") / c."開盤價")
                END AS "實體_開盤_pct_calc",
            (c."MA5_calc" - lag(c."MA5_calc", 1) OVER (ORDER BY c.rn)) AS "MA5上升幅度_calc",
            (c."MA10_calc" - lag(c."MA10_calc", 1) OVER (ORDER BY c.rn)) AS "MA10上升幅度_calc",
            (c."MA20_calc" - lag(c."MA20_calc", 1) OVER (ORDER BY c.rn)) AS "MA20上升幅度_calc",
            (c."MA60_calc" - lag(c."MA60_calc", 1) OVER (ORDER BY c.rn)) AS "MA60上升幅度_calc",
                CASE
                    WHEN ((lag(c."MA5_calc", 1) OVER (ORDER BY c.rn) IS NULL) OR (lag(c."MA5_calc", 1) OVER (ORDER BY c.rn) = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."MA5_calc" - lag(c."MA5_calc", 1) OVER (ORDER BY c.rn)) / lag(c."MA5_calc", 1) OVER (ORDER BY c.rn))
                END AS "MA5上升幅度_pct_calc",
                CASE
                    WHEN ((lag(c."MA10_calc", 1) OVER (ORDER BY c.rn) IS NULL) OR (lag(c."MA10_calc", 1) OVER (ORDER BY c.rn) = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."MA10_calc" - lag(c."MA10_calc", 1) OVER (ORDER BY c.rn)) / lag(c."MA10_calc", 1) OVER (ORDER BY c.rn))
                END AS "MA10上升幅度_pct_calc",
                CASE
                    WHEN ((lag(c."MA20_calc", 1) OVER (ORDER BY c.rn) IS NULL) OR (lag(c."MA20_calc", 1) OVER (ORDER BY c.rn) = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."MA20_calc" - lag(c."MA20_calc", 1) OVER (ORDER BY c.rn)) / lag(c."MA20_calc", 1) OVER (ORDER BY c.rn))
                END AS "MA20上升幅度_pct_calc",
                CASE
                    WHEN ((lag(c."MA60_calc", 1) OVER (ORDER BY c.rn) IS NULL) OR (lag(c."MA60_calc", 1) OVER (ORDER BY c.rn) = (0)::double precision)) THEN NULL::double precision
                    ELSE ((c."MA60_calc" - lag(c."MA60_calc", 1) OVER (ORDER BY c.rn)) / lag(c."MA60_calc", 1) OVER (ORDER BY c.rn))
                END AS "MA60上升幅度_pct_calc"
           FROM calc1 c
        ), calc3 AS (
         SELECT c.id,
            c."日期",
            c."股票代號",
            c."年",
            c."月",
            c."星期",
            c."週次",
            c."天別",
            c."開盤價",
            c."收盤價",
            c."收盤_開盤",
            c."最高價",
            c."最低價",
            c."漲跌",
            c."日振幅",
            c."漲跌幅_pct",
            c."日振幅_昨收_pct",
            c."日振福_開盤_pct",
            c."成交量",
            c."實體_pct",
            c."上影_pct",
            c."下影_pct",
            c."K線型態",
            c."跳空缺口",
            c."跳空缺口_昨收_pct",
            c."當日價量",
            c."5日價量",
            c."10日價量",
            c."20日價量",
            c."60日價量",
            c."總成交金額_億",
            c."法人總買超_億",
            c."買超_外資_億",
            c."買超_投信_億",
            c."買超_自營商_億",
            c."買超_融資_億",
            c."法人淨買賣超_扣融資_億",
            c."法人買賣超_總成交_pct",
            c."漲跌幅百分位數",
            c."漲跌幅級距",
            c."三大法人買賣超百分位數",
            c."三大法人買賣超級距",
            c."資金走向判定",
            c.is_complete,
            c.updated_at,
            c.rn,
            c."昨收",
            c."昨量",
            c."MA5_calc",
            c."MA10_calc",
            c."MA20_calc",
            c."MA60_calc",
            c."VOLMA5_calc",
            c."VOLMA10_calc",
            c."VOLMA20_calc",
            c."VOLMA60_calc",
            c."抵5_calc",
            c."抵10_calc",
            c."抵20_calc",
            c."抵60_calc",
            c."MAXVOL5_calc",
            c."MAXVOL10_calc",
            c."MAXVOL20_calc",
            c."MAXVOL60_calc",
            c."MAXVOL5_DATE_calc",
            c."MAXVOL10_DATE_calc",
            c."MAXVOL20_DATE_calc",
            c."MAXVOL60_DATE_calc",
            c."收盤開盤_calc",
            c."日振幅_calc",
            c."漲跌_calc",
            c."漲跌幅_pct_calc",
            c."日振幅_昨收_pct_calc",
            c."日振福_開盤_pct_calc",
            c."量增率_pct_calc",
            c."量增率5_pct_calc",
            c."量增率10_pct_calc",
            c."量增率20_pct_calc",
            c."量增率60_pct_calc",
            c."抵5影響_pct_calc",
            c."抵10影響_pct_calc",
            c."抵20影響_pct_calc",
            c."抵60影響_pct_calc",
            c."抵5影響點_calc",
            c."抵10影響點_calc",
            c."抵20影響點_calc",
            c."抵60影響點_calc",
            c."乖離5_calc",
            c."乖離10_calc",
            c."乖離20_calc",
            c."乖離60_calc",
            c."實體_昨收_pct_calc",
            c."實體_開盤_pct_calc",
            c."MA5上升幅度_calc",
            c."MA10上升幅度_calc",
            c."MA20上升幅度_calc",
            c."MA60上升幅度_calc",
            c."MA5上升幅度_pct_calc",
            c."MA10上升幅度_pct_calc",
            c."MA20上升幅度_pct_calc",
            c."MA60上升幅度_pct_calc",
                CASE
                    WHEN (c."漲跌_calc" > (0)::double precision) THEN '價漲'::text
                    WHEN (c."漲跌_calc" < (0)::double precision) THEN '價跌'::text
                    WHEN (c."漲跌_calc" = (0)::double precision) THEN '價同'::text
                    ELSE NULL::text
                END AS "價漲跌_昨日",
                CASE
                    WHEN (c."量增率_pct_calc" > (0)::double precision) THEN '量增'::text
                    WHEN (c."量增率_pct_calc" < (0)::double precision) THEN '量縮'::text
                    WHEN (c."量增率_pct_calc" = (0)::double precision) THEN '量同'::text
                    ELSE NULL::text
                END AS "量增減_昨日",
                CASE
                    WHEN (c."量增率5_pct_calc" > (0)::double precision) THEN '量增'::text
                    WHEN (c."量增率5_pct_calc" < (0)::double precision) THEN '量縮'::text
                    WHEN (c."量增率5_pct_calc" = (0)::double precision) THEN '量同'::text
                    ELSE NULL::text
                END AS "量增減_五日",
                CASE
                    WHEN ((c."MA5_calc" > lag(c."MA5_calc") OVER (ORDER BY c.rn)) AND (c."MA10_calc" > lag(c."MA10_calc") OVER (ORDER BY c.rn))) THEN '多'::text
                    WHEN ((c."MA5_calc" <= lag(c."MA5_calc") OVER (ORDER BY c.rn)) AND (c."MA10_calc" <= lag(c."MA10_calc") OVER (ORDER BY c.rn))) THEN '空'::text
                    ELSE '盤整'::text
                END AS "趨勢",
                CASE
                    WHEN ((
                    CASE
                        WHEN ((c."MA5_calc" > lag(c."MA5_calc") OVER (ORDER BY c.rn)) AND (c."MA10_calc" > lag(c."MA10_calc") OVER (ORDER BY c.rn))) THEN '多'::text
                        WHEN ((c."MA5_calc" <= lag(c."MA5_calc") OVER (ORDER BY c.rn)) AND (c."MA10_calc" <= lag(c."MA10_calc") OVER (ORDER BY c.rn))) THEN '空'::text
                        ELSE '盤整'::text
                    END = ANY (ARRAY['多'::text, '空'::text])) AND (c."乖離5_calc" IS NOT NULL) AND (c."乖離5_calc" > (0.02)::double precision)) THEN '高檔'::text
                    WHEN ((
                    CASE
                        WHEN ((c."MA5_calc" > lag(c."MA5_calc") OVER (ORDER BY c.rn)) AND (c."MA10_calc" > lag(c."MA10_calc") OVER (ORDER BY c.rn))) THEN '多'::text
                        WHEN ((c."MA5_calc" <= lag(c."MA5_calc") OVER (ORDER BY c.rn)) AND (c."MA10_calc" <= lag(c."MA10_calc") OVER (ORDER BY c.rn))) THEN '空'::text
                        ELSE '盤整'::text
                    END = ANY (ARRAY['多'::text, '空'::text])) AND (c."乖離5_calc" IS NOT NULL) AND (c."乖離5_calc" < ('-0.02'::numeric)::double precision)) THEN '低檔'::text
                    ELSE '中間'::text
                END AS "位置"
           FROM calc2 c
        ), calc4 AS (
         SELECT c.id,
            c."日期",
            c."股票代號",
            c."年",
            c."月",
            c."星期",
            c."週次",
            c."天別",
            c."開盤價",
            c."收盤價",
            c."收盤_開盤",
            c."最高價",
            c."最低價",
            c."漲跌",
            c."日振幅",
            c."漲跌幅_pct",
            c."日振幅_昨收_pct",
            c."日振福_開盤_pct",
            c."成交量",
            c."實體_pct",
            c."上影_pct",
            c."下影_pct",
            c."K線型態",
            c."跳空缺口",
            c."跳空缺口_昨收_pct",
            c."當日價量",
            c."5日價量",
            c."10日價量",
            c."20日價量",
            c."60日價量",
            c."總成交金額_億",
            c."法人總買超_億",
            c."買超_外資_億",
            c."買超_投信_億",
            c."買超_自營商_億",
            c."買超_融資_億",
            c."法人淨買賣超_扣融資_億",
            c."法人買賣超_總成交_pct",
            c."漲跌幅百分位數",
            c."漲跌幅級距",
            c."三大法人買賣超百分位數",
            c."三大法人買賣超級距",
            c."資金走向判定",
            c.is_complete,
            c.updated_at,
            c.rn,
            c."昨收",
            c."昨量",
            c."MA5_calc",
            c."MA10_calc",
            c."MA20_calc",
            c."MA60_calc",
            c."VOLMA5_calc",
            c."VOLMA10_calc",
            c."VOLMA20_calc",
            c."VOLMA60_calc",
            c."抵5_calc",
            c."抵10_calc",
            c."抵20_calc",
            c."抵60_calc",
            c."MAXVOL5_calc",
            c."MAXVOL10_calc",
            c."MAXVOL20_calc",
            c."MAXVOL60_calc",
            c."MAXVOL5_DATE_calc",
            c."MAXVOL10_DATE_calc",
            c."MAXVOL20_DATE_calc",
            c."MAXVOL60_DATE_calc",
            c."收盤開盤_calc",
            c."日振幅_calc",
            c."漲跌_calc",
            c."漲跌幅_pct_calc",
            c."日振幅_昨收_pct_calc",
            c."日振福_開盤_pct_calc",
            c."量增率_pct_calc",
            c."量增率5_pct_calc",
            c."量增率10_pct_calc",
            c."量增率20_pct_calc",
            c."量增率60_pct_calc",
            c."抵5影響_pct_calc",
            c."抵10影響_pct_calc",
            c."抵20影響_pct_calc",
            c."抵60影響_pct_calc",
            c."抵5影響點_calc",
            c."抵10影響點_calc",
            c."抵20影響點_calc",
            c."抵60影響點_calc",
            c."乖離5_calc",
            c."乖離10_calc",
            c."乖離20_calc",
            c."乖離60_calc",
            c."實體_昨收_pct_calc",
            c."實體_開盤_pct_calc",
            c."MA5上升幅度_calc",
            c."MA10上升幅度_calc",
            c."MA20上升幅度_calc",
            c."MA60上升幅度_calc",
            c."MA5上升幅度_pct_calc",
            c."MA10上升幅度_pct_calc",
            c."MA20上升幅度_pct_calc",
            c."MA60上升幅度_pct_calc",
            c."價漲跌_昨日",
            c."量增減_昨日",
            c."量增減_五日",
            c."趨勢",
            c."位置",
                CASE
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_昨日" = '量增'::text) AND (c."趨勢" = '多'::text) AND (c."位置" = ANY (ARRAY['低檔'::text, '中間'::text, '高檔'::text]))) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_昨日" = '量增'::text) AND (c."趨勢" = '盤整'::text) AND (c."位置" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_昨日" = '量增'::text) AND (c."趨勢" = '盤整'::text) AND (c."位置" = '高檔'::text)) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_昨日" = '量增'::text) AND (c."趨勢" = '空'::text) AND (c."位置" = ANY (ARRAY['低檔'::text, '中間'::text, '高檔'::text]))) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_昨日" = '量縮'::text) AND (c."趨勢" = '多'::text) AND (c."位置" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_昨日" = '量縮'::text) AND (c."趨勢" = '多'::text) AND (c."位置" = '高檔'::text)) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_昨日" = '量縮'::text) AND (c."趨勢" = ANY (ARRAY['盤整'::text, '空'::text]))) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_昨日" = '量增'::text) AND (c."趨勢" = '多'::text) AND (c."位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_昨日" = '量增'::text) AND (c."趨勢" = '多'::text) AND (c."位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_昨日" = '量增'::text) AND (c."趨勢" = '盤整'::text) AND (c."位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_昨日" = '量增'::text) AND (c."趨勢" = '盤整'::text) AND (c."位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_昨日" = '量增'::text) AND (c."趨勢" = '空'::text)) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_昨日" = '量縮'::text) AND (c."趨勢" = '多'::text)) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_昨日" = '量縮'::text) AND (c."趨勢" = '盤整'::text) AND (c."位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_昨日" = '量縮'::text) AND (c."趨勢" = '盤整'::text) AND (c."位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_昨日" = '量縮'::text) AND (c."趨勢" = '空'::text) AND (c."位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_昨日" = '量縮'::text) AND (c."趨勢" = '空'::text) AND (c."位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    ELSE NULL::text
                END AS "策略_價量昨日_判斷",
                CASE
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_五日" = '量增'::text) AND (c."趨勢" = '多'::text) AND (c."位置" = ANY (ARRAY['低檔'::text, '中間'::text, '高檔'::text]))) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_五日" = '量增'::text) AND (c."趨勢" = '盤整'::text) AND (c."位置" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_五日" = '量增'::text) AND (c."趨勢" = '盤整'::text) AND (c."位置" = '高檔'::text)) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_五日" = '量增'::text) AND (c."趨勢" = '空'::text)) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_五日" = '量縮'::text) AND (c."趨勢" = '多'::text) AND (c."位置" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_五日" = '量縮'::text) AND (c."趨勢" = '多'::text) AND (c."位置" = '高檔'::text)) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價漲'::text) AND (c."量增減_五日" = '量縮'::text) AND (c."趨勢" = ANY (ARRAY['盤整'::text, '空'::text]))) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_五日" = '量增'::text) AND (c."趨勢" = '多'::text) AND (c."位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_五日" = '量增'::text) AND (c."趨勢" = '多'::text) AND (c."位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_五日" = '量增'::text) AND (c."趨勢" = '盤整'::text) AND (c."位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_五日" = '量增'::text) AND (c."趨勢" = '盤整'::text) AND (c."位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_五日" = '量增'::text) AND (c."趨勢" = '空'::text)) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_五日" = '量縮'::text) AND (c."趨勢" = '多'::text)) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_五日" = '量縮'::text) AND (c."趨勢" = '盤整'::text) AND (c."位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_五日" = '量縮'::text) AND (c."趨勢" = '盤整'::text) AND (c."位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_五日" = '量縮'::text) AND (c."趨勢" = '空'::text) AND (c."位置" = '低檔'::text)) THEN '多'::text
                    WHEN ((c."價漲跌_昨日" = '價跌'::text) AND (c."量增減_五日" = '量縮'::text) AND (c."趨勢" = '空'::text) AND (c."位置" = ANY (ARRAY['中間'::text, '高檔'::text]))) THEN '空'::text
                    ELSE NULL::text
                END AS "策略_價量五日_判斷"
           FROM calc3 c
        )
 SELECT "日期",
    "年",
    "月",
    "星期",
    "週次",
    "天別",
    round(("開盤價")::numeric, 2) AS "開盤價",
    round(("收盤價")::numeric, 2) AS "收盤價",
    round(("最高價")::numeric, 2) AS "最高價",
    round(("最低價")::numeric, 2) AS "最低價",
    round(("漲跌_calc")::numeric, 2) AS "漲跌",
    round(("漲跌幅_pct_calc")::numeric, 4) AS "漲跌幅_pct",
    round(("收盤開盤_calc")::numeric, 2) AS "收盤_開盤",
    round(("實體_昨收_pct_calc")::numeric, 4) AS "實體_昨收_pct",
    round(("實體_開盤_pct_calc")::numeric, 4) AS "實體_開盤_pct",
    round(("日振幅_calc")::numeric, 2) AS "日振幅",
    round(("日振幅_昨收_pct_calc")::numeric, 4) AS "日振幅_昨收_pct",
    round(("日振福_開盤_pct_calc")::numeric, 4) AS "日振福_開盤_pct",
    round(("成交量")::numeric, 2) AS "成交量",
    "MAXVOL5_DATE_calc" AS "5日最大量_日期",
    round(("MAXVOL5_calc")::numeric, 2) AS "5日最大量",
    "MAXVOL10_DATE_calc" AS "10日最大量_日期",
    round(("MAXVOL10_calc")::numeric, 2) AS "10日最大量",
    "MAXVOL20_DATE_calc" AS "20日最大量_日期",
    round(("MAXVOL20_calc")::numeric, 2) AS "20日最大量",
    "MAXVOL60_DATE_calc" AS "60日最大量_日期",
    round(("MAXVOL60_calc")::numeric, 2) AS "60日最大量",
    round(("VOLMA5_calc")::numeric, 2) AS "5日均量",
    round(("VOLMA10_calc")::numeric, 2) AS "10日均量",
    round(("VOLMA20_calc")::numeric, 2) AS "20日均量",
    round(("VOLMA60_calc")::numeric, 2) AS "60日均量",
    round(("量增率_pct_calc")::numeric, 4) AS "量增率_pct",
    round(("量增率5_pct_calc")::numeric, 4) AS "5日量增率_pct",
    round(("量增率10_pct_calc")::numeric, 4) AS "10日量增率_pct",
    round(("量增率20_pct_calc")::numeric, 4) AS "20日量增率_pct",
    round(("量增率60_pct_calc")::numeric, 4) AS "60日量增率_pct",
    "當日價量",
    "5日價量",
    "10日價量",
    "20日價量",
    "60日價量",
    "趨勢",
    "位置",
    "策略_價量昨日_判斷",
    "策略_價量五日_判斷",
    "K線型態",
    round(("跳空缺口")::numeric, 2) AS "跳空缺口",
    round(("跳空缺口_昨收_pct")::numeric, 4) AS "跳空缺口_昨收_pct",
    round(("實體_pct")::numeric, 4) AS "實體_pct",
    round(("上影_pct")::numeric, 4) AS "上影_pct",
    round(("下影_pct")::numeric, 4) AS "下影_pct",
    round(("MA5_calc")::numeric, 2) AS "5MA",
    round(("MA10_calc")::numeric, 2) AS "10MA",
    round(("MA20_calc")::numeric, 2) AS "20MA",
    round(("MA60_calc")::numeric, 2) AS "60MA",
    round(("MA5上升幅度_calc")::numeric, 2) AS "5日上升幅度",
    round(("MA10上升幅度_calc")::numeric, 2) AS "10日上升幅度",
    round(("MA20上升幅度_calc")::numeric, 2) AS "20日上升幅度",
    round(("MA60上升幅度_calc")::numeric, 2) AS "60日上升幅度",
    round(("MA5上升幅度_pct_calc")::numeric, 4) AS "5日上升幅度_pct",
    round(("MA10上升幅度_pct_calc")::numeric, 4) AS "10日上升幅度_pct",
    round(("MA20上升幅度_pct_calc")::numeric, 4) AS "20日上升幅度_pct",
    round(("MA60上升幅度_pct_calc")::numeric, 4) AS "60日上升幅度_pct",
    round(("抵5_calc")::numeric, 2) AS "5日扣抵值",
    round(("抵10_calc")::numeric, 2) AS "10日扣抵值",
    round(("抵20_calc")::numeric, 2) AS "20日扣抵值",
    round(("抵60_calc")::numeric, 2) AS "60日扣抵值",
    round(("抵5影響點_calc")::numeric, 2) AS "5日明日扣抵影響_點",
    round(("抵10影響點_calc")::numeric, 2) AS "10日明日扣抵影響_點",
    round(("抵20影響點_calc")::numeric, 2) AS "20日明日扣抵影響_點",
    round(("抵60影響點_calc")::numeric, 2) AS "60日明日扣抵影響_點",
    round(("抵5影響_pct_calc")::numeric, 4) AS "5日明日扣抵影響_pct",
    round(("抵10影響_pct_calc")::numeric, 4) AS "10日明日扣抵影響_pct",
    round(("抵20影響_pct_calc")::numeric, 4) AS "20日明日扣抵影響_pct",
    round(("抵60影響_pct_calc")::numeric, 4) AS "60日明日扣抵影響_pct",
    round(("乖離5_calc")::numeric, 4) AS "5日乖離",
    round(("乖離10_calc")::numeric, 4) AS "10日乖離",
    round(("乖離20_calc")::numeric, 4) AS "20日乖離",
    round(("乖離60_calc")::numeric, 4) AS "60日乖離",
    round(("總成交金額_億")::numeric, 2) AS "總成交金額_億",
    round(("法人總買超_億")::numeric, 2) AS "法人總買超_億",
    round(("買超_外資_億")::numeric, 2) AS "買超_外資_億",
    round(("買超_投信_億")::numeric, 2) AS "買超_投信_億",
    round(("買超_自營商_億")::numeric, 2) AS "買超_自營商_億",
    round(("買超_融資_億")::numeric, 2) AS "買超_融資_億",
    round(("法人淨買賣超_扣融資_億")::numeric, 2) AS "法人淨買賣超_扣融資_億",
    round(("法人買賣超_總成交_pct")::numeric, 4) AS "法人買賣超_總成交_pct",
    round(("漲跌幅百分位數")::numeric, 2) AS "漲跌幅百分位數",
    "漲跌幅級距",
    round(("三大法人買賣超百分位數")::numeric, 2) AS "三大法人買賣超百分位數",
    "三大法人買賣超級距",
    "資金走向判定"
   FROM calc4;


--
-- Name: v_taiex_report_column_source; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_taiex_report_column_source AS
 WITH futures_retail AS MATERIALIZED (
         SELECT f."日期",
            round((f."散戶多空比_微台" / 100.0), 4) AS "散戶多空比_微台"
           FROM report.v_futures_report_daily_model f
        )
 SELECT b."日期",
    b."年",
    b."月",
    b."星期",
    b."週次",
    b."天別",
    b."開盤價",
    b."收盤價",
    b."最高價",
    b."最低價",
    b."漲跌",
    b."漲跌幅_pct",
    b."收盤_開盤",
    b."實體_昨收_pct",
    b."實體_開盤_pct",
    b."日振幅",
    b."日振幅_昨收_pct",
    b."日振福_開盤_pct",
    b."成交量",
    b."5日最大量_日期",
    b."5日最大量",
    b."10日最大量_日期",
    b."10日最大量",
    b."20日最大量_日期",
    b."20日最大量",
    b."60日最大量_日期",
    b."60日最大量",
    b."5日均量",
    b."10日均量",
    b."20日均量",
    b."60日均量",
    b."量增率_pct",
    b."5日量增率_pct",
    b."10日量增率_pct",
    b."20日量增率_pct",
    b."60日量增率_pct",
    b."當日價量",
    b."5日價量",
    b."10日價量",
    b."20日價量",
    b."60日價量",
    b."趨勢",
    b."位置",
    b."策略_價量昨日_判斷",
    b."策略_價量五日_判斷",
    b."K線型態",
    b."跳空缺口",
    b."跳空缺口_昨收_pct",
    b."實體_pct",
    b."上影_pct",
    b."下影_pct",
    b."5MA",
    b."10MA",
    b."20MA",
    b."60MA",
    b."5日上升幅度",
    b."10日上升幅度",
    b."20日上升幅度",
    b."60日上升幅度",
    b."5日上升幅度_pct",
    b."10日上升幅度_pct",
    b."20日上升幅度_pct",
    b."60日上升幅度_pct",
    b."5日扣抵值",
    b."10日扣抵值",
    b."20日扣抵值",
    b."60日扣抵值",
    b."5日明日扣抵影響_點",
    b."10日明日扣抵影響_點",
    b."20日明日扣抵影響_點",
    b."60日明日扣抵影響_點",
    b."5日明日扣抵影響_pct",
    b."10日明日扣抵影響_pct",
    b."20日明日扣抵影響_pct",
    b."60日明日扣抵影響_pct",
    b."5日乖離",
    b."10日乖離",
    b."20日乖離",
    b."60日乖離",
    round(
        CASE
            WHEN (count(b."收盤價") OVER (ORDER BY b."日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) = 20) THEN stddev_samp(b."收盤價") OVER (ORDER BY b."日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
            ELSE NULL::numeric
        END, 2) AS "布林標準差",
    round((
        CASE
            WHEN (count(b."收盤價") OVER (ORDER BY b."日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) = 20) THEN ((b."20MA")::double precision + (((2)::numeric * stddev_samp(b."收盤價") OVER (ORDER BY b."日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)))::double precision)
            ELSE NULL::double precision
        END)::numeric, 2) AS "布林上軌",
    round((
        CASE
            WHEN (count(b."收盤價") OVER (ORDER BY b."日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) = 20) THEN ((b."20MA")::double precision - (((2)::numeric * stddev_samp(b."收盤價") OVER (ORDER BY b."日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)))::double precision)
            ELSE NULL::double precision
        END)::numeric, 2) AS "布林下軌",
    b."總成交金額_億",
    b."法人總買超_億",
    b."買超_外資_億",
    b."買超_投信_億",
    b."買超_自營商_億",
    b."買超_融資_億",
    b."法人淨買賣超_扣融資_億",
    b."法人買賣超_總成交_pct",
    fr."散戶多空比_微台",
    b."漲跌幅百分位數",
    b."漲跌幅級距",
    b."三大法人買賣超百分位數",
    b."三大法人買賣超級距",
    b."資金走向判定"
   FROM (report.v_taiex_report_base b
     LEFT JOIN futures_retail fr ON ((fr."日期" = b."日期")));


--
-- Name: v_taiex_report_rule_corrected; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_taiex_report_rule_corrected AS
 WITH rule_inputs AS (
         SELECT s."日期",
            s."年",
            s."月",
            s."星期",
            s."週次",
            s."天別",
            s."開盤價",
            s."收盤價",
            s."最高價",
            s."最低價",
            s."漲跌",
            s."漲跌幅_pct",
            s."收盤_開盤",
            s."實體_昨收_pct",
            s."實體_開盤_pct",
            s."日振幅",
            s."日振幅_昨收_pct",
            s."日振福_開盤_pct",
            s."成交量",
            s."5日最大量_日期",
            s."5日最大量",
            s."10日最大量_日期",
            s."10日最大量",
            s."20日最大量_日期",
            s."20日最大量",
            s."60日最大量_日期",
            s."60日最大量",
            s."5日均量",
            s."10日均量",
            s."20日均量",
            s."60日均量",
            s."量增率_pct",
            s."5日量增率_pct",
            s."10日量增率_pct",
            s."20日量增率_pct",
            s."60日量增率_pct",
            s."當日價量",
            s."5日價量",
            s."10日價量",
            s."20日價量",
            s."60日價量",
            s."趨勢",
            s."位置",
            s."策略_價量昨日_判斷",
            s."策略_價量五日_判斷",
            s."K線型態",
            s."跳空缺口",
            s."跳空缺口_昨收_pct",
            s."實體_pct",
            s."上影_pct",
            s."下影_pct",
            s."5MA",
            s."10MA",
            s."20MA",
            s."60MA",
            s."5日上升幅度",
            s."10日上升幅度",
            s."20日上升幅度",
            s."60日上升幅度",
            s."5日上升幅度_pct",
            s."10日上升幅度_pct",
            s."20日上升幅度_pct",
            s."60日上升幅度_pct",
            s."5日扣抵值",
            s."10日扣抵值",
            s."20日扣抵值",
            s."60日扣抵值",
            s."5日明日扣抵影響_點",
            s."10日明日扣抵影響_點",
            s."20日明日扣抵影響_點",
            s."60日明日扣抵影響_點",
            s."5日明日扣抵影響_pct",
            s."10日明日扣抵影響_pct",
            s."20日明日扣抵影響_pct",
            s."60日明日扣抵影響_pct",
            s."5日乖離",
            s."10日乖離",
            s."20日乖離",
            s."60日乖離",
            s."布林標準差",
            s."布林上軌",
            s."布林下軌",
            s."總成交金額_億",
            s."法人總買超_億",
            s."買超_外資_億",
            s."買超_投信_億",
            s."買超_自營商_億",
            s."買超_融資_億",
            s."法人淨買賣超_扣融資_億",
            s."法人買賣超_總成交_pct",
            s."散戶多空比_微台",
            s."漲跌幅百分位數",
            s."漲跌幅級距",
            s."三大法人買賣超百分位數",
            s."三大法人買賣超級距",
            s."資金走向判定",
                CASE
                    WHEN ((s."5日上升幅度" IS NULL) OR (s."10日上升幅度" IS NULL) OR (s."20日上升幅度" IS NULL)) THEN '資料不足'::text
                    WHEN ((s."5日上升幅度" = (0)::numeric) OR (s."10日上升幅度" = (0)::numeric) OR (s."20日上升幅度" = (0)::numeric)) THEN '盤整'::text
                    WHEN ((s."5日上升幅度" > (0)::numeric) AND (s."10日上升幅度" > (0)::numeric) AND (s."20日上升幅度" > (0)::numeric)) THEN '多'::text
                    WHEN ((s."5日上升幅度" > (0)::numeric) AND (s."10日上升幅度" > (0)::numeric) AND (s."20日上升幅度" < (0)::numeric)) THEN '多'::text
                    WHEN ((s."5日上升幅度" > (0)::numeric) AND (s."10日上升幅度" < (0)::numeric) AND (s."20日上升幅度" > (0)::numeric)) THEN '盤整'::text
                    WHEN ((s."5日上升幅度" > (0)::numeric) AND (s."10日上升幅度" < (0)::numeric) AND (s."20日上升幅度" < (0)::numeric)) THEN '空'::text
                    WHEN ((s."5日上升幅度" < (0)::numeric) AND (s."10日上升幅度" > (0)::numeric) AND (s."20日上升幅度" > (0)::numeric)) THEN '多'::text
                    WHEN ((s."5日上升幅度" < (0)::numeric) AND (s."10日上升幅度" > (0)::numeric) AND (s."20日上升幅度" < (0)::numeric)) THEN '盤整'::text
                    WHEN ((s."5日上升幅度" < (0)::numeric) AND (s."10日上升幅度" < (0)::numeric) AND (s."20日上升幅度" > (0)::numeric)) THEN '空'::text
                    WHEN ((s."5日上升幅度" < (0)::numeric) AND (s."10日上升幅度" < (0)::numeric) AND (s."20日上升幅度" < (0)::numeric)) THEN '空'::text
                    ELSE '資料不足'::text
                END AS "_趨勢修正",
                CASE
                    WHEN (s."漲跌" IS NULL) THEN '資料不足'::text
                    WHEN (s."漲跌" > (0)::numeric) THEN '價漲'::text
                    WHEN (s."漲跌" < (0)::numeric) THEN '價跌'::text
                    ELSE '價同'::text
                END AS "_價格狀態",
                CASE
                    WHEN (s."量增率_pct" IS NULL) THEN '資料不足'::text
                    WHEN (s."量增率_pct" > (0)::numeric) THEN '量增'::text
                    WHEN (s."量增率_pct" < (0)::numeric) THEN '量縮'::text
                    ELSE '量同'::text
                END AS "_昨日量狀態",
                CASE
                    WHEN (s."5日量增率_pct" IS NULL) THEN '資料不足'::text
                    WHEN (s."5日量增率_pct" > (0)::numeric) THEN '量增'::text
                    WHEN (s."5日量增率_pct" < (0)::numeric) THEN '量縮'::text
                    ELSE '量同'::text
                END AS "_五日量狀態"
           FROM report.v_taiex_report_column_source s
        ), position_calc AS (
         SELECT i."日期",
            i."年",
            i."月",
            i."星期",
            i."週次",
            i."天別",
            i."開盤價",
            i."收盤價",
            i."最高價",
            i."最低價",
            i."漲跌",
            i."漲跌幅_pct",
            i."收盤_開盤",
            i."實體_昨收_pct",
            i."實體_開盤_pct",
            i."日振幅",
            i."日振幅_昨收_pct",
            i."日振福_開盤_pct",
            i."成交量",
            i."5日最大量_日期",
            i."5日最大量",
            i."10日最大量_日期",
            i."10日最大量",
            i."20日最大量_日期",
            i."20日最大量",
            i."60日最大量_日期",
            i."60日最大量",
            i."5日均量",
            i."10日均量",
            i."20日均量",
            i."60日均量",
            i."量增率_pct",
            i."5日量增率_pct",
            i."10日量增率_pct",
            i."20日量增率_pct",
            i."60日量增率_pct",
            i."當日價量",
            i."5日價量",
            i."10日價量",
            i."20日價量",
            i."60日價量",
            i."趨勢",
            i."位置",
            i."策略_價量昨日_判斷",
            i."策略_價量五日_判斷",
            i."K線型態",
            i."跳空缺口",
            i."跳空缺口_昨收_pct",
            i."實體_pct",
            i."上影_pct",
            i."下影_pct",
            i."5MA",
            i."10MA",
            i."20MA",
            i."60MA",
            i."5日上升幅度",
            i."10日上升幅度",
            i."20日上升幅度",
            i."60日上升幅度",
            i."5日上升幅度_pct",
            i."10日上升幅度_pct",
            i."20日上升幅度_pct",
            i."60日上升幅度_pct",
            i."5日扣抵值",
            i."10日扣抵值",
            i."20日扣抵值",
            i."60日扣抵值",
            i."5日明日扣抵影響_點",
            i."10日明日扣抵影響_點",
            i."20日明日扣抵影響_點",
            i."60日明日扣抵影響_點",
            i."5日明日扣抵影響_pct",
            i."10日明日扣抵影響_pct",
            i."20日明日扣抵影響_pct",
            i."60日明日扣抵影響_pct",
            i."5日乖離",
            i."10日乖離",
            i."20日乖離",
            i."60日乖離",
            i."布林標準差",
            i."布林上軌",
            i."布林下軌",
            i."總成交金額_億",
            i."法人總買超_億",
            i."買超_外資_億",
            i."買超_投信_億",
            i."買超_自營商_億",
            i."買超_融資_億",
            i."法人淨買賣超_扣融資_億",
            i."法人買賣超_總成交_pct",
            i."散戶多空比_微台",
            i."漲跌幅百分位數",
            i."漲跌幅級距",
            i."三大法人買賣超百分位數",
            i."三大法人買賣超級距",
            i."資金走向判定",
            i."_趨勢修正",
            i."_價格狀態",
            i."_昨日量狀態",
            i."_五日量狀態",
                CASE
                    WHEN ((i."_趨勢修正" = '資料不足'::text) OR (i."收盤價" IS NULL)) THEN '資料不足'::text
                    WHEN ((i."_趨勢修正" = ANY (ARRAY['多'::text, '空'::text])) AND (i."5日乖離" IS NULL)) THEN '資料不足'::text
                    WHEN ((i."_趨勢修正" = ANY (ARRAY['多'::text, '空'::text])) AND (i."5日乖離" > 0.02)) THEN '高檔'::text
                    WHEN ((i."_趨勢修正" = ANY (ARRAY['多'::text, '空'::text])) AND (i."5日乖離" < '-0.02'::numeric)) THEN '低檔'::text
                    WHEN (i."_趨勢修正" = ANY (ARRAY['多'::text, '空'::text])) THEN '中間'::text
                    WHEN ((i."布林上軌" IS NULL) OR (i."布林下軌" IS NULL) OR ((i."布林上軌" - i."布林下軌") <= (0)::numeric)) THEN '資料不足'::text
                    WHEN (i."收盤價" >= (i."布林下軌" + (0.8 * (i."布林上軌" - i."布林下軌")))) THEN '高檔'::text
                    WHEN (i."收盤價" <= (i."布林下軌" + (0.2 * (i."布林上軌" - i."布林下軌")))) THEN '低檔'::text
                    ELSE '中間'::text
                END AS "_位置修正"
           FROM rule_inputs i
        ), strategy_calc AS (
         SELECT p."日期",
            p."年",
            p."月",
            p."星期",
            p."週次",
            p."天別",
            p."開盤價",
            p."收盤價",
            p."最高價",
            p."最低價",
            p."漲跌",
            p."漲跌幅_pct",
            p."收盤_開盤",
            p."實體_昨收_pct",
            p."實體_開盤_pct",
            p."日振幅",
            p."日振幅_昨收_pct",
            p."日振福_開盤_pct",
            p."成交量",
            p."5日最大量_日期",
            p."5日最大量",
            p."10日最大量_日期",
            p."10日最大量",
            p."20日最大量_日期",
            p."20日最大量",
            p."60日最大量_日期",
            p."60日最大量",
            p."5日均量",
            p."10日均量",
            p."20日均量",
            p."60日均量",
            p."量增率_pct",
            p."5日量增率_pct",
            p."10日量增率_pct",
            p."20日量增率_pct",
            p."60日量增率_pct",
            p."當日價量",
            p."5日價量",
            p."10日價量",
            p."20日價量",
            p."60日價量",
            p."趨勢",
            p."位置",
            p."策略_價量昨日_判斷",
            p."策略_價量五日_判斷",
            p."K線型態",
            p."跳空缺口",
            p."跳空缺口_昨收_pct",
            p."實體_pct",
            p."上影_pct",
            p."下影_pct",
            p."5MA",
            p."10MA",
            p."20MA",
            p."60MA",
            p."5日上升幅度",
            p."10日上升幅度",
            p."20日上升幅度",
            p."60日上升幅度",
            p."5日上升幅度_pct",
            p."10日上升幅度_pct",
            p."20日上升幅度_pct",
            p."60日上升幅度_pct",
            p."5日扣抵值",
            p."10日扣抵值",
            p."20日扣抵值",
            p."60日扣抵值",
            p."5日明日扣抵影響_點",
            p."10日明日扣抵影響_點",
            p."20日明日扣抵影響_點",
            p."60日明日扣抵影響_點",
            p."5日明日扣抵影響_pct",
            p."10日明日扣抵影響_pct",
            p."20日明日扣抵影響_pct",
            p."60日明日扣抵影響_pct",
            p."5日乖離",
            p."10日乖離",
            p."20日乖離",
            p."60日乖離",
            p."布林標準差",
            p."布林上軌",
            p."布林下軌",
            p."總成交金額_億",
            p."法人總買超_億",
            p."買超_外資_億",
            p."買超_投信_億",
            p."買超_自營商_億",
            p."買超_融資_億",
            p."法人淨買賣超_扣融資_億",
            p."法人買賣超_總成交_pct",
            p."散戶多空比_微台",
            p."漲跌幅百分位數",
            p."漲跌幅級距",
            p."三大法人買賣超百分位數",
            p."三大法人買賣超級距",
            p."資金走向判定",
            p."_趨勢修正",
            p."_價格狀態",
            p."_昨日量狀態",
            p."_五日量狀態",
            p."_位置修正",
                CASE
                    WHEN ((p."_價格狀態" = ANY (ARRAY['價同'::text, '資料不足'::text])) OR (p."_昨日量狀態" = ANY (ARRAY['量同'::text, '資料不足'::text])) OR (p."_趨勢修正" = '資料不足'::text) OR (p."_位置修正" = '資料不足'::text)) THEN '無'::text
                    WHEN ((p."_價格狀態" = '價漲'::text) AND (p."_昨日量狀態" = '量增'::text) AND (p."_趨勢修正" = '盤整'::text) AND (p."_位置修正" = '高檔'::text)) THEN '空'::text
                    WHEN ((p."_價格狀態" = '價漲'::text) AND (p."_昨日量狀態" = '量增'::text)) THEN '多'::text
                    WHEN ((p."_價格狀態" = '價漲'::text) AND (p."_昨日量狀態" = '量縮'::text) AND (p."_趨勢修正" = '多'::text) AND (p."_位置修正" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((p."_價格狀態" = '價漲'::text) AND (p."_昨日量狀態" = '量縮'::text) AND (p."_趨勢修正" = '空'::text) AND (p."_位置修正" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_價格狀態" = '價漲'::text) AND (p."_昨日量狀態" = '量縮'::text)) THEN '空'::text
                    WHEN ((p."_價格狀態" = '價跌'::text) AND (p."_昨日量狀態" = '量增'::text) AND (p."_趨勢修正" = ANY (ARRAY['多'::text, '盤整'::text])) AND (p."_位置修正" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_價格狀態" = '價跌'::text) AND (p."_昨日量狀態" = '量增'::text)) THEN '空'::text
                    WHEN ((p."_價格狀態" = '價跌'::text) AND (p."_昨日量狀態" = '量縮'::text) AND (p."_位置修正" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_價格狀態" = '價跌'::text) AND (p."_昨日量狀態" = '量縮'::text) AND (p."_趨勢修正" = '多'::text) AND (p."_位置修正" = '中間'::text)) THEN '多'::text
                    WHEN ((p."_價格狀態" = '價跌'::text) AND (p."_昨日量狀態" = '量縮'::text)) THEN '空'::text
                    ELSE '無'::text
                END AS "_策略昨日修正",
                CASE
                    WHEN ((p."_價格狀態" = ANY (ARRAY['價同'::text, '資料不足'::text])) OR (p."_五日量狀態" = ANY (ARRAY['量同'::text, '資料不足'::text])) OR (p."_趨勢修正" = '資料不足'::text) OR (p."_位置修正" = '資料不足'::text)) THEN '無'::text
                    WHEN ((p."_價格狀態" = '價漲'::text) AND (p."_五日量狀態" = '量增'::text) AND (p."_趨勢修正" = '盤整'::text) AND (p."_位置修正" = '高檔'::text)) THEN '空'::text
                    WHEN ((p."_價格狀態" = '價漲'::text) AND (p."_五日量狀態" = '量增'::text)) THEN '多'::text
                    WHEN ((p."_價格狀態" = '價漲'::text) AND (p."_五日量狀態" = '量縮'::text) AND (p."_趨勢修正" = '多'::text) AND (p."_位置修正" = ANY (ARRAY['低檔'::text, '中間'::text]))) THEN '多'::text
                    WHEN ((p."_價格狀態" = '價漲'::text) AND (p."_五日量狀態" = '量縮'::text) AND (p."_趨勢修正" = '空'::text) AND (p."_位置修正" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_價格狀態" = '價漲'::text) AND (p."_五日量狀態" = '量縮'::text)) THEN '空'::text
                    WHEN ((p."_價格狀態" = '價跌'::text) AND (p."_五日量狀態" = '量增'::text) AND (p."_趨勢修正" = ANY (ARRAY['多'::text, '盤整'::text])) AND (p."_位置修正" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_價格狀態" = '價跌'::text) AND (p."_五日量狀態" = '量增'::text)) THEN '空'::text
                    WHEN ((p."_價格狀態" = '價跌'::text) AND (p."_五日量狀態" = '量縮'::text) AND (p."_位置修正" = '低檔'::text)) THEN '多'::text
                    WHEN ((p."_價格狀態" = '價跌'::text) AND (p."_五日量狀態" = '量縮'::text) AND (p."_趨勢修正" = '多'::text) AND (p."_位置修正" = '中間'::text)) THEN '多'::text
                    WHEN ((p."_價格狀態" = '價跌'::text) AND (p."_五日量狀態" = '量縮'::text)) THEN '空'::text
                    ELSE '無'::text
                END AS "_策略五日修正"
           FROM position_calc p
        )
 SELECT "日期",
    "年",
    "月",
    "星期",
    "週次",
    "天別",
    "開盤價",
    "收盤價",
    "最高價",
    "最低價",
    "漲跌",
    "漲跌幅_pct",
    "收盤_開盤",
    "實體_昨收_pct",
    "實體_開盤_pct",
    "日振幅",
    "日振幅_昨收_pct",
    "日振福_開盤_pct",
    "成交量",
    "5日最大量_日期",
    "5日最大量",
    "10日最大量_日期",
    "10日最大量",
    "20日最大量_日期",
    "20日最大量",
    "60日最大量_日期",
    "60日最大量",
    "5日均量",
    "10日均量",
    "20日均量",
    "60日均量",
    "量增率_pct",
    "5日量增率_pct",
    "10日量增率_pct",
    "20日量增率_pct",
    "60日量增率_pct",
    "當日價量",
    "5日價量",
    "10日價量",
    "20日價量",
    "60日價量",
    "_趨勢修正" AS "趨勢",
    "_位置修正" AS "位置",
    "_策略昨日修正" AS "策略_價量昨日_判斷",
    "_策略五日修正" AS "策略_價量五日_判斷",
    "K線型態",
    "跳空缺口",
    "跳空缺口_昨收_pct",
    "實體_pct",
    "上影_pct",
    "下影_pct",
    "5MA",
    "10MA",
    "20MA",
    "60MA",
    "5日上升幅度",
    "10日上升幅度",
    "20日上升幅度",
    "60日上升幅度",
    "5日上升幅度_pct",
    "10日上升幅度_pct",
    "20日上升幅度_pct",
    "60日上升幅度_pct",
    "5日扣抵值",
    "10日扣抵值",
    "20日扣抵值",
    "60日扣抵值",
    "5日明日扣抵影響_點",
    "10日明日扣抵影響_點",
    "20日明日扣抵影響_點",
    "60日明日扣抵影響_點",
    "5日明日扣抵影響_pct",
    "10日明日扣抵影響_pct",
    "20日明日扣抵影響_pct",
    "60日明日扣抵影響_pct",
    "5日乖離",
    "10日乖離",
    "20日乖離",
    "60日乖離",
    "布林標準差",
    "布林上軌",
    "布林下軌",
    "總成交金額_億",
    "法人總買超_億",
    "買超_外資_億",
    "買超_投信_億",
    "買超_自營商_億",
    "買超_融資_億",
    "法人淨買賣超_扣融資_億",
    "法人買賣超_總成交_pct",
    "散戶多空比_微台",
    "漲跌幅百分位數",
    "漲跌幅級距",
    "三大法人買賣超百分位數",
    "三大法人買賣超級距",
    "資金走向判定"
   FROM strategy_calc c;


--
-- Name: VIEW v_taiex_report_rule_corrected; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON VIEW report.v_taiex_report_rule_corrected IS '加權指數報規則修正版：完整MA趨勢、布林位置、價同量同及資料不足防守處理';


--
-- Name: v_taiex_report_before_strategy_20260818; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_taiex_report_before_strategy_20260818 AS
 WITH original AS (
         SELECT src."日期",
            src."年",
            src."月",
            src."星期",
            src."週次",
            src."天別",
            src."開盤價",
            src."收盤價",
            src."最高價",
            src."最低價",
            src."漲跌",
            src."漲跌幅_pct",
            src."收盤_開盤",
            src."實體_昨收_pct",
            src."實體_開盤_pct",
            src."日振幅",
            src."日振幅_昨收_pct",
            src."日振福_開盤_pct",
            src."成交量",
            src."5日最大量_日期",
            src."5日最大量",
            src."10日最大量_日期",
            src."10日最大量",
            src."20日最大量_日期",
            src."20日最大量",
            src."60日最大量_日期",
            src."60日最大量",
            src."5日均量",
            src."10日均量",
            src."20日均量",
            src."60日均量",
            src."量增率_pct",
            src."5日量增率_pct",
            src."10日量增率_pct",
            src."20日量增率_pct",
            src."60日量增率_pct",
            src."當日價量",
            src."5日價量",
            src."10日價量",
            src."20日價量",
            src."60日價量",
            src."K線型態",
            src."跳空缺口",
            src."跳空缺口_昨收_pct",
            src."實體_pct",
            src."上影_pct",
            src."下影_pct",
            src."5MA",
            src."10MA",
            src."20MA",
            src."60MA",
            src."120MA",
            src."240MA",
            src."5日上升幅度",
            src."10日上升幅度",
            src."20日上升幅度",
            src."60日上升幅度",
            src."5日上升幅度_pct",
            src."10日上升幅度_pct",
            src."20日上升幅度_pct",
            src."60日上升幅度_pct",
            src."5日扣抵值",
            src."10日扣抵值",
            src."20日扣抵值",
            src."60日扣抵值",
            src."5日明日扣抵影響_點",
            src."10日明日扣抵影響_點",
            src."20日明日扣抵影響_點",
            src."60日明日扣抵影響_點",
            src."5日明日扣抵影響_pct",
            src."10日明日扣抵影響_pct",
            src."20日明日扣抵影響_pct",
            src."60日明日扣抵影響_pct",
            src."5日乖離",
            src."10日乖離",
            src."20日乖離",
            src."60日乖離",
            src."布林標準差",
            src."布林上軌",
            src."布林下軌",
            src."總成交金額_億",
            src."法人總買超_億",
            src."買超_外資_億",
            src."買超_投信_億",
            src."買超_自營商_億",
            src."買超_融資_億",
            src."法人淨買賣超_扣融資_億",
            src."法人買賣超_總成交_pct",
            src."散戶多空比_微台",
            src."漲跌幅百分位數",
            src."漲跌幅級距",
            src."三大法人買賣超百分位數",
            src."三大法人買賣超級距",
            src."資金走向判定",
            src."位置",
            src."策略_價量昨日_判斷",
            src."策略_價量五日_判斷",
            src."趨勢"
           FROM ( SELECT s."日期",
                    s."年",
                    s."月",
                    s."星期",
                    s."週次",
                    s."天別",
                    s."開盤價",
                    s."收盤價",
                    s."最高價",
                    s."最低價",
                    s."漲跌",
                    s."漲跌幅_pct",
                    s."收盤_開盤",
                    s."實體_昨收_pct",
                    s."實體_開盤_pct",
                    s."日振幅",
                    s."日振幅_昨收_pct",
                    s."日振福_開盤_pct",
                    s."成交量",
                    s."5日最大量_日期",
                    s."5日最大量",
                    s."10日最大量_日期",
                    s."10日最大量",
                    s."20日最大量_日期",
                    s."20日最大量",
                    s."60日最大量_日期",
                    s."60日最大量",
                    s."5日均量",
                    s."10日均量",
                    s."20日均量",
                    s."60日均量",
                    s."量增率_pct",
                    s."5日量增率_pct",
                    s."10日量增率_pct",
                    s."20日量增率_pct",
                    s."60日量增率_pct",
                    s."當日價量",
                    s."5日價量",
                    s."10日價量",
                    s."20日價量",
                    s."60日價量",
                    s."趨勢",
                    s."K線型態",
                    s."跳空缺口",
                    s."跳空缺口_昨收_pct",
                    s."實體_pct",
                    s."上影_pct",
                    s."下影_pct",
                    s."5MA",
                    s."10MA",
                    s."20MA",
                    s."60MA",
                    r."120MA",
                    r."240MA",
                    s."5日上升幅度",
                    s."10日上升幅度",
                    s."20日上升幅度",
                    s."60日上升幅度",
                    s."5日上升幅度_pct",
                    s."10日上升幅度_pct",
                    s."20日上升幅度_pct",
                    s."60日上升幅度_pct",
                    s."5日扣抵值",
                    s."10日扣抵值",
                    s."20日扣抵值",
                    s."60日扣抵值",
                    s."5日明日扣抵影響_點",
                    s."10日明日扣抵影響_點",
                    s."20日明日扣抵影響_點",
                    s."60日明日扣抵影響_點",
                    s."5日明日扣抵影響_pct",
                    s."10日明日扣抵影響_pct",
                    s."20日明日扣抵影響_pct",
                    s."60日明日扣抵影響_pct",
                    s."5日乖離",
                    s."10日乖離",
                    s."20日乖離",
                    s."60日乖離",
                    s."布林標準差",
                    s."布林上軌",
                    s."布林下軌",
                    s."總成交金額_億",
                    s."法人總買超_億",
                    s."買超_外資_億",
                    s."買超_投信_億",
                    s."買超_自營商_億",
                    s."買超_融資_億",
                    s."法人淨買賣超_扣融資_億",
                    s."法人買賣超_總成交_pct",
                    s."散戶多空比_微台",
                    s."漲跌幅百分位數",
                    s."漲跌幅級距",
                    s."三大法人買賣超百分位數",
                    s."三大法人買賣超級距",
                    s."資金走向判定",
                    s."位置",
                    s."策略_價量昨日_判斷",
                    s."策略_價量五日_判斷"
                   FROM (report.v_taiex_report_rule_corrected s
                     JOIN core.stock_report_daily r ON (((r."股票代號" = 'TAIEX'::text) AND (r."日期" = s."日期"))))) src
        ), ma_values AS (
         SELECT o."日期",
            o."年",
            o."月",
            o."星期",
            o."週次",
            o."天別",
            o."開盤價",
            o."收盤價",
            o."最高價",
            o."最低價",
            o."漲跌",
            o."漲跌幅_pct",
            o."收盤_開盤",
            o."實體_昨收_pct",
            o."實體_開盤_pct",
            o."日振幅",
            o."日振幅_昨收_pct",
            o."日振福_開盤_pct",
            o."成交量",
            o."5日最大量_日期",
            o."5日最大量",
            o."10日最大量_日期",
            o."10日最大量",
            o."20日最大量_日期",
            o."20日最大量",
            o."60日最大量_日期",
            o."60日最大量",
            o."5日均量",
            o."10日均量",
            o."20日均量",
            o."60日均量",
            o."量增率_pct",
            o."5日量增率_pct",
            o."10日量增率_pct",
            o."20日量增率_pct",
            o."60日量增率_pct",
            o."當日價量",
            o."5日價量",
            o."10日價量",
            o."20日價量",
            o."60日價量",
            o."K線型態",
            o."跳空缺口",
            o."跳空缺口_昨收_pct",
            o."實體_pct",
            o."上影_pct",
            o."下影_pct",
            o."5MA",
            o."10MA",
            o."20MA",
            o."60MA",
            o."120MA",
            o."240MA",
            o."5日上升幅度",
            o."10日上升幅度",
            o."20日上升幅度",
            o."60日上升幅度",
            o."5日上升幅度_pct",
            o."10日上升幅度_pct",
            o."20日上升幅度_pct",
            o."60日上升幅度_pct",
            o."5日扣抵值",
            o."10日扣抵值",
            o."20日扣抵值",
            o."60日扣抵值",
            o."5日明日扣抵影響_點",
            o."10日明日扣抵影響_點",
            o."20日明日扣抵影響_點",
            o."60日明日扣抵影響_點",
            o."5日明日扣抵影響_pct",
            o."10日明日扣抵影響_pct",
            o."20日明日扣抵影響_pct",
            o."60日明日扣抵影響_pct",
            o."5日乖離",
            o."10日乖離",
            o."20日乖離",
            o."60日乖離",
            o."布林標準差",
            o."布林上軌",
            o."布林下軌",
            o."總成交金額_億",
            o."法人總買超_億",
            o."買超_外資_億",
            o."買超_投信_億",
            o."買超_自營商_億",
            o."買超_融資_億",
            o."法人淨買賣超_扣融資_億",
            o."法人買賣超_總成交_pct",
            o."散戶多空比_微台",
            o."漲跌幅百分位數",
            o."漲跌幅級距",
            o."三大法人買賣超百分位數",
            o."三大法人買賣超級距",
            o."資金走向判定",
            o."位置",
            o."策略_價量昨日_判斷",
            o."策略_價量五日_判斷",
            o."趨勢",
                CASE
                    WHEN (count(o."收盤價") OVER w5 = 5) THEN avg(o."收盤價") OVER w5
                    ELSE NULL::numeric
                END AS "_原始5MA",
                CASE
                    WHEN (count(o."收盤價") OVER w10 = 10) THEN avg(o."收盤價") OVER w10
                    ELSE NULL::numeric
                END AS "_原始10MA",
                CASE
                    WHEN (count(o."收盤價") OVER w20 = 20) THEN avg(o."收盤價") OVER w20
                    ELSE NULL::numeric
                END AS "_原始20MA"
           FROM original o
          WINDOW w5 AS (ORDER BY o."日期" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), w10 AS (ORDER BY o."日期" ROWS BETWEEN 9 PRECEDING AND CURRENT ROW), w20 AS (ORDER BY o."日期" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        ), calculated AS (
         SELECT m."日期",
            m."年",
            m."月",
            m."星期",
            m."週次",
            m."天別",
            m."開盤價",
            m."收盤價",
            m."最高價",
            m."最低價",
            m."漲跌",
            m."漲跌幅_pct",
            m."收盤_開盤",
            m."實體_昨收_pct",
            m."實體_開盤_pct",
            m."日振幅",
            m."日振幅_昨收_pct",
            m."日振福_開盤_pct",
            m."成交量",
            m."5日最大量_日期",
            m."5日最大量",
            m."10日最大量_日期",
            m."10日最大量",
            m."20日最大量_日期",
            m."20日最大量",
            m."60日最大量_日期",
            m."60日最大量",
            m."5日均量",
            m."10日均量",
            m."20日均量",
            m."60日均量",
            m."量增率_pct",
            m."5日量增率_pct",
            m."10日量增率_pct",
            m."20日量增率_pct",
            m."60日量增率_pct",
            m."當日價量",
            m."5日價量",
            m."10日價量",
            m."20日價量",
            m."60日價量",
            m."K線型態",
            m."跳空缺口",
            m."跳空缺口_昨收_pct",
            m."實體_pct",
            m."上影_pct",
            m."下影_pct",
            m."5MA",
            m."10MA",
            m."20MA",
            m."60MA",
            m."120MA",
            m."240MA",
            m."5日上升幅度",
            m."10日上升幅度",
            m."20日上升幅度",
            m."60日上升幅度",
            m."5日上升幅度_pct",
            m."10日上升幅度_pct",
            m."20日上升幅度_pct",
            m."60日上升幅度_pct",
            m."5日扣抵值",
            m."10日扣抵值",
            m."20日扣抵值",
            m."60日扣抵值",
            m."5日明日扣抵影響_點",
            m."10日明日扣抵影響_點",
            m."20日明日扣抵影響_點",
            m."60日明日扣抵影響_點",
            m."5日明日扣抵影響_pct",
            m."10日明日扣抵影響_pct",
            m."20日明日扣抵影響_pct",
            m."60日明日扣抵影響_pct",
            m."5日乖離",
            m."10日乖離",
            m."20日乖離",
            m."60日乖離",
            m."布林標準差",
            m."布林上軌",
            m."布林下軌",
            m."總成交金額_億",
            m."法人總買超_億",
            m."買超_外資_億",
            m."買超_投信_億",
            m."買超_自營商_億",
            m."買超_融資_億",
            m."法人淨買賣超_扣融資_億",
            m."法人買賣超_總成交_pct",
            m."散戶多空比_微台",
            m."漲跌幅百分位數",
            m."漲跌幅級距",
            m."三大法人買賣超百分位數",
            m."三大法人買賣超級距",
            m."資金走向判定",
            m."位置",
            m."策略_價量昨日_判斷",
            m."策略_價量五日_判斷",
            m."趨勢",
            m."_原始5MA",
            m."_原始10MA",
            m."_原始20MA",
                CASE
                    WHEN (m."_原始5MA" > lag(m."_原始5MA") OVER (ORDER BY m."日期")) THEN '上'::text
                    WHEN (m."_原始5MA" < lag(m."_原始5MA") OVER (ORDER BY m."日期")) THEN '下'::text
                    ELSE NULL::text
                END AS "_MA方向_5",
                CASE
                    WHEN (m."_原始10MA" > lag(m."_原始10MA") OVER (ORDER BY m."日期")) THEN '上'::text
                    WHEN (m."_原始10MA" < lag(m."_原始10MA") OVER (ORDER BY m."日期")) THEN '下'::text
                    ELSE NULL::text
                END AS "_MA方向_10",
                CASE
                    WHEN (m."_原始20MA" > lag(m."_原始20MA") OVER (ORDER BY m."日期")) THEN '上'::text
                    WHEN (m."_原始20MA" < lag(m."_原始20MA") OVER (ORDER BY m."日期")) THEN '下'::text
                    ELSE NULL::text
                END AS "_MA方向_20",
                CASE
                    WHEN (m."法人總買超_億" > (0)::numeric) THEN '買超'::text
                    WHEN (m."法人總買超_億" < (0)::numeric) THEN '賣超'::text
                    ELSE NULL::text
                END AS "_法人籌碼",
                CASE
                    WHEN (m."散戶多空比_微台" > (0)::numeric) THEN '多單'::text
                    WHEN (m."散戶多空比_微台" < (0)::numeric) THEN '空單'::text
                    ELSE NULL::text
                END AS "_散戶籌碼"
           FROM ma_values m
        )
 SELECT "日期",
    "年",
    "月",
    "星期",
    "週次",
    "天別",
    "開盤價",
    "收盤價",
    "最高價",
    "最低價",
    "漲跌",
    "漲跌幅_pct",
    "收盤_開盤",
    "實體_昨收_pct",
    "實體_開盤_pct",
    "日振幅",
    "日振幅_昨收_pct",
    "日振福_開盤_pct",
    "成交量",
    "5日最大量_日期",
    "5日最大量",
    "10日最大量_日期",
    "10日最大量",
    "20日最大量_日期",
    "20日最大量",
    "60日最大量_日期",
    "60日最大量",
    "5日均量",
    "10日均量",
    "20日均量",
    "60日均量",
    "量增率_pct",
    "5日量增率_pct",
    "10日量增率_pct",
    "20日量增率_pct",
    "60日量增率_pct",
    "當日價量",
    "5日價量",
    "10日價量",
    "20日價量",
    "60日價量",
    "K線型態",
    "跳空缺口",
    "跳空缺口_昨收_pct",
    "實體_pct",
    "上影_pct",
    "下影_pct",
    "5MA",
    "10MA",
    "20MA",
    "60MA",
    "120MA",
    "240MA",
    "5日上升幅度",
    "10日上升幅度",
    "20日上升幅度",
    "60日上升幅度",
    "5日上升幅度_pct",
    "10日上升幅度_pct",
    "20日上升幅度_pct",
    "60日上升幅度_pct",
    "5日扣抵值",
    "10日扣抵值",
    "20日扣抵值",
    "60日扣抵值",
    "5日明日扣抵影響_點",
    "10日明日扣抵影響_點",
    "20日明日扣抵影響_點",
    "60日明日扣抵影響_點",
    "5日明日扣抵影響_pct",
    "10日明日扣抵影響_pct",
    "20日明日扣抵影響_pct",
    "60日明日扣抵影響_pct",
    "5日乖離",
    "10日乖離",
    "20日乖離",
    "60日乖離",
    "布林標準差",
    "布林上軌",
    "布林下軌",
    "總成交金額_億",
    "法人總買超_億",
    "買超_外資_億",
    "買超_投信_億",
    "買超_自營商_億",
    "買超_融資_億",
    "法人淨買賣超_扣融資_億",
    "法人買賣超_總成交_pct",
    "散戶多空比_微台",
    "漲跌幅百分位數",
    "漲跌幅級距",
    "三大法人買賣超百分位數",
    "三大法人買賣超級距",
    "資金走向判定",
    "位置",
    "策略_價量昨日_判斷",
    "策略_價量五日_判斷",
    "_MA方向_5" AS "MA方向_5",
    "_MA方向_10" AS "MA方向_10",
    "_MA方向_20" AS "MA方向_20",
    "趨勢",
    "_法人籌碼" AS "法人籌碼",
    "_散戶籌碼" AS "散戶籌碼"
   FROM calculated c;


--
-- Name: v_taiex_report_composite_source_20260823; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_taiex_report_composite_source_20260823 AS
 WITH base AS (
         SELECT src."日期",
            src."年",
            src."月",
            src."星期",
            src."週次",
            src."天別",
            src."開盤價",
            src."收盤價",
            src."最高價",
            src."最低價",
            src."漲跌",
            src."漲跌幅_pct",
            src."收盤_開盤",
            src."實體_昨收_pct",
            src."實體_開盤_pct",
            src."日振幅",
            src."日振幅_昨收_pct",
            src."日振福_開盤_pct",
            src."成交量",
            src."5日最大量_日期",
            src."5日最大量",
            src."10日最大量_日期",
            src."10日最大量",
            src."20日最大量_日期",
            src."20日最大量",
            src."60日最大量_日期",
            src."60日最大量",
            src."5日均量",
            src."10日均量",
            src."20日均量",
            src."60日均量",
            src."量增率_pct",
            src."5日量增率_pct",
            src."10日量增率_pct",
            src."20日量增率_pct",
            src."60日量增率_pct",
            src."當日價量",
            src."5日價量",
            src."10日價量",
            src."20日價量",
            src."60日價量",
            src."K線型態",
            src."跳空缺口",
            src."跳空缺口_昨收_pct",
            src."實體_pct",
            src."上影_pct",
            src."下影_pct",
            src."5MA",
            src."10MA",
            src."20MA",
            src."60MA",
            src."120MA",
            src."240MA",
            src."5日上升幅度",
            src."10日上升幅度",
            src."20日上升幅度",
            src."60日上升幅度",
            src."5日上升幅度_pct",
            src."10日上升幅度_pct",
            src."20日上升幅度_pct",
            src."60日上升幅度_pct",
            src."5日扣抵值",
            src."10日扣抵值",
            src."20日扣抵值",
            src."60日扣抵值",
            src."5日明日扣抵影響_點",
            src."10日明日扣抵影響_點",
            src."20日明日扣抵影響_點",
            src."60日明日扣抵影響_點",
            src."5日明日扣抵影響_pct",
            src."10日明日扣抵影響_pct",
            src."20日明日扣抵影響_pct",
            src."60日明日扣抵影響_pct",
            src."5日乖離",
            src."10日乖離",
            src."20日乖離",
            src."60日乖離",
            src."布林標準差",
            src."布林上軌",
            src."布林下軌",
            src."總成交金額_億",
            src."法人總買超_億",
            src."買超_外資_億",
            src."買超_投信_億",
            src."買超_自營商_億",
            src."買超_融資_億",
            src."法人淨買賣超_扣融資_億",
            src."法人買賣超_總成交_pct",
            src."散戶多空比_微台",
            src."漲跌幅百分位數",
            src."漲跌幅級距",
            src."三大法人買賣超百分位數",
            src."三大法人買賣超級距",
            src."資金走向判定",
            src."位置",
            src."策略_價量昨日_判斷",
            src."策略_價量五日_判斷",
            src."MA方向_5",
            src."MA方向_10",
            src."MA方向_20",
            src."趨勢",
            src."法人籌碼",
            src."散戶籌碼",
                CASE
                    WHEN ((src."法人籌碼" = '買超'::text) AND (src."散戶籌碼" = '空單'::text)) THEN '多'::text
                    WHEN ((src."法人籌碼" = '賣超'::text) AND (src."散戶籌碼" = '多單'::text)) THEN '空'::text
                    ELSE '無'::text
                END AS "策略_籌碼_判定"
           FROM report.v_taiex_report_before_strategy_20260818 src
        )
 SELECT "日期",
    "年",
    "月",
    "星期",
    "週次",
    "天別",
    "開盤價",
    "收盤價",
    "最高價",
    "最低價",
    "漲跌",
    "漲跌幅_pct",
    "收盤_開盤",
    "實體_昨收_pct",
    "實體_開盤_pct",
    "日振幅",
    "日振幅_昨收_pct",
    "日振福_開盤_pct",
    "成交量",
    "5日最大量_日期",
    "5日最大量",
    "10日最大量_日期",
    "10日最大量",
    "20日最大量_日期",
    "20日最大量",
    "60日最大量_日期",
    "60日最大量",
    "5日均量",
    "10日均量",
    "20日均量",
    "60日均量",
    "量增率_pct",
    "5日量增率_pct",
    "10日量增率_pct",
    "20日量增率_pct",
    "60日量增率_pct",
    "當日價量",
    "5日價量",
    "10日價量",
    "20日價量",
    "60日價量",
    "K線型態",
    "跳空缺口",
    "跳空缺口_昨收_pct",
    "實體_pct",
    "上影_pct",
    "下影_pct",
    "5MA",
    "10MA",
    "20MA",
    "60MA",
    "120MA",
    "240MA",
    "5日上升幅度",
    "10日上升幅度",
    "20日上升幅度",
    "60日上升幅度",
    "5日上升幅度_pct",
    "10日上升幅度_pct",
    "20日上升幅度_pct",
    "60日上升幅度_pct",
    "5日扣抵值",
    "10日扣抵值",
    "20日扣抵值",
    "60日扣抵值",
    "5日明日扣抵影響_點",
    "10日明日扣抵影響_點",
    "20日明日扣抵影響_點",
    "60日明日扣抵影響_點",
    "5日明日扣抵影響_pct",
    "10日明日扣抵影響_pct",
    "20日明日扣抵影響_pct",
    "60日明日扣抵影響_pct",
    "5日乖離",
    "10日乖離",
    "20日乖離",
    "60日乖離",
    "布林標準差",
    "布林上軌",
    "布林下軌",
    "總成交金額_億",
    "法人總買超_億",
    "買超_外資_億",
    "買超_投信_億",
    "買超_自營商_億",
    "買超_融資_億",
    "法人淨買賣超_扣融資_億",
    "法人買賣超_總成交_pct",
    "散戶多空比_微台",
    "漲跌幅百分位數",
    "漲跌幅級距",
    "三大法人買賣超百分位數",
    "三大法人買賣超級距",
    "資金走向判定",
    "位置",
    "策略_價量昨日_判斷",
    "策略_價量五日_判斷",
    "MA方向_5",
    "MA方向_10",
    "MA方向_20",
    "趨勢",
    "法人籌碼",
    "散戶籌碼",
    "策略_籌碼_判定",
        CASE
            WHEN (("策略_價量昨日_判斷" = '多'::text) AND ("趨勢" = '多'::text)) THEN '多'::text
            WHEN (("策略_價量昨日_判斷" = '空'::text) AND ("趨勢" = '空'::text)) THEN '空'::text
            WHEN (("策略_價量昨日_判斷" = ANY (ARRAY['多'::text, '空'::text])) AND ("趨勢" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
            ELSE '無'::text
        END AS "策略_價量昨日_線型",
        CASE
            WHEN (("策略_價量五日_判斷" = '多'::text) AND ("趨勢" = '多'::text)) THEN '多'::text
            WHEN (("策略_價量五日_判斷" = '空'::text) AND ("趨勢" = '空'::text)) THEN '空'::text
            WHEN (("策略_價量五日_判斷" = ANY (ARRAY['多'::text, '空'::text])) AND ("趨勢" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
            ELSE '無'::text
        END AS "策略_價量五日_線型",
        CASE
            WHEN (("策略_價量昨日_判斷" = '多'::text) AND ("策略_籌碼_判定" = '多'::text)) THEN '多'::text
            WHEN (("策略_價量昨日_判斷" = '空'::text) AND ("策略_籌碼_判定" = '空'::text)) THEN '空'::text
            WHEN (("策略_價量昨日_判斷" = ANY (ARRAY['多'::text, '空'::text])) AND ("策略_籌碼_判定" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
            ELSE '無'::text
        END AS "策略_價量昨日_籌碼",
        CASE
            WHEN (("策略_價量五日_判斷" = '多'::text) AND ("策略_籌碼_判定" = '多'::text)) THEN '多'::text
            WHEN (("策略_價量五日_判斷" = '空'::text) AND ("策略_籌碼_判定" = '空'::text)) THEN '空'::text
            WHEN (("策略_價量五日_判斷" = ANY (ARRAY['多'::text, '空'::text])) AND ("策略_籌碼_判定" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
            ELSE '無'::text
        END AS "策略_價量五日_籌碼",
        CASE
            WHEN (("趨勢" = '多'::text) AND ("策略_籌碼_判定" = '多'::text)) THEN '多'::text
            WHEN (("趨勢" = '空'::text) AND ("策略_籌碼_判定" = '空'::text)) THEN '空'::text
            WHEN (("趨勢" = ANY (ARRAY['多'::text, '空'::text])) AND ("策略_籌碼_判定" = ANY (ARRAY['多'::text, '空'::text]))) THEN 'X'::text
            ELSE '無'::text
        END AS "策略_線型_籌碼",
        CASE
            WHEN ("策略_價量五日_判斷" = '多'::text) THEN
            CASE
                WHEN ("趨勢" = '多'::text) THEN '多'::text
                WHEN ("趨勢" = '空'::text) THEN
                CASE
                    WHEN ("策略_籌碼_判定" = '多'::text) THEN '多'::text
                    ELSE '空'::text
                END
                WHEN ("趨勢" = '無'::text) THEN '多'::text
                ELSE '多'::text
            END
            WHEN ("策略_價量五日_判斷" = '空'::text) THEN
            CASE
                WHEN ("趨勢" = '多'::text) THEN
                CASE
                    WHEN ("策略_籌碼_判定" = '空'::text) THEN '空'::text
                    ELSE '多'::text
                END
                WHEN ("趨勢" = '空'::text) THEN '空'::text
                WHEN ("趨勢" = '無'::text) THEN '空'::text
                ELSE '空'::text
            END
            WHEN ("策略_價量五日_判斷" = '無'::text) THEN
            CASE
                WHEN ("趨勢" = '多'::text) THEN '多'::text
                WHEN ("趨勢" = '空'::text) THEN '空'::text
                WHEN ("趨勢" = '無'::text) THEN
                CASE
                    WHEN ("策略_籌碼_判定" = '多'::text) THEN '多'::text
                    WHEN ("策略_籌碼_判定" = '空'::text) THEN '空'::text
                    ELSE '無'::text
                END
                ELSE '無'::text
            END
            ELSE '無'::text
        END AS "策略_核心"
   FROM base b;


--
-- Name: VIEW v_taiex_report_composite_source_20260823; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON VIEW report.v_taiex_report_composite_source_20260823 IS '加權指數報；2026-08-18 新增籌碼判定及價量／線型／籌碼組合策略欄位。';


--
-- Name: v_taiex_report_position_source_20260902; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_taiex_report_position_source_20260902 AS
 WITH normalized AS (
         SELECT s."日期",
            s."年",
            s."月",
            s."星期",
            s."週次",
            s."天別",
            s."開盤價",
            s."收盤價",
            s."最高價",
            s."最低價",
            s."漲跌",
            s."漲跌幅_pct",
            s."收盤_開盤",
            s."實體_昨收_pct",
            s."實體_開盤_pct",
            s."日振幅",
            s."日振幅_昨收_pct",
            s."日振福_開盤_pct",
            s."成交量",
            s."5日最大量_日期",
            s."5日最大量",
            s."10日最大量_日期",
            s."10日最大量",
            s."20日最大量_日期",
            s."20日最大量",
            s."60日最大量_日期",
            s."60日最大量",
            s."5日均量",
            s."10日均量",
            s."20日均量",
            s."60日均量",
            s."量增率_pct",
            s."5日量增率_pct",
            s."10日量增率_pct",
            s."20日量增率_pct",
            s."60日量增率_pct",
            s."當日價量",
            s."5日價量",
            s."10日價量",
            s."20日價量",
            s."60日價量",
            s."K線型態",
            s."跳空缺口",
            s."跳空缺口_昨收_pct",
            s."實體_pct",
            s."上影_pct",
            s."下影_pct",
            s."5MA",
            s."10MA",
            s."20MA",
            s."60MA",
            s."120MA",
            s."240MA",
            s."5日上升幅度",
            s."10日上升幅度",
            s."20日上升幅度",
            s."60日上升幅度",
            s."5日上升幅度_pct",
            s."10日上升幅度_pct",
            s."20日上升幅度_pct",
            s."60日上升幅度_pct",
            s."5日扣抵值",
            s."10日扣抵值",
            s."20日扣抵值",
            s."60日扣抵值",
            s."5日明日扣抵影響_點",
            s."10日明日扣抵影響_點",
            s."20日明日扣抵影響_點",
            s."60日明日扣抵影響_點",
            s."5日明日扣抵影響_pct",
            s."10日明日扣抵影響_pct",
            s."20日明日扣抵影響_pct",
            s."60日明日扣抵影響_pct",
            s."5日乖離",
            s."10日乖離",
            s."20日乖離",
            s."60日乖離",
            s."布林標準差",
            s."布林上軌",
            s."布林下軌",
            s."總成交金額_億",
            s."法人總買超_億",
            s."買超_外資_億",
            s."買超_投信_億",
            s."買超_自營商_億",
            s."買超_融資_億",
            s."法人淨買賣超_扣融資_億",
            s."法人買賣超_總成交_pct",
            s."散戶多空比_微台",
            s."漲跌幅百分位數",
            s."漲跌幅級距",
            s."三大法人買賣超百分位數",
            s."三大法人買賣超級距",
            s."資金走向判定",
            s."位置",
            s."策略_價量昨日_判斷",
            s."策略_價量五日_判斷",
            s."MA方向_5",
            s."MA方向_10",
            s."MA方向_20",
            s."趨勢",
            s."法人籌碼",
            s."散戶籌碼",
            s."策略_籌碼_判定",
            s."策略_價量昨日_線型",
            s."策略_價量五日_線型",
            s."策略_價量昨日_籌碼",
            s."策略_價量五日_籌碼",
            s."策略_線型_籌碼",
            s."策略_核心",
                CASE
                    WHEN ((s."趨勢" IS NULL) OR (s."趨勢" = '資料不足'::text) OR (s."位置" IS NULL) OR (s."位置" = '資料不足'::text)) THEN '資料不足'::text
                    WHEN (s."趨勢" = '多'::text) THEN '多'::text
                    WHEN (s."趨勢" = '空'::text) THEN '空'::text
                    WHEN ((s."趨勢" = '盤整'::text) AND (s."位置" = '高檔'::text)) THEN '盤整(高)'::text
                    WHEN ((s."趨勢" = '盤整'::text) AND (s."位置" = '中間'::text)) THEN '盤整(中)'::text
                    WHEN ((s."趨勢" = '盤整'::text) AND (s."位置" = '低檔'::text)) THEN '盤整(低)'::text
                    ELSE '資料不足'::text
                END AS "_線型位置",
                CASE
                    WHEN ((s."法人總買超_億" IS NULL) OR (s."散戶多空比_微台" IS NULL)) THEN '資料不足'::text
                    WHEN (s."策略_籌碼_判定" = '多'::text) THEN '多'::text
                    WHEN (s."策略_籌碼_判定" = '空'::text) THEN '空'::text
                    WHEN ((s."策略_籌碼_判定" = '無'::text) AND (s."法人籌碼" = '買超'::text)) THEN '無(法人買超)'::text
                    WHEN ((s."策略_籌碼_判定" = '無'::text) AND (s."法人籌碼" = '賣超'::text)) THEN '無(法人賣超)'::text
                    WHEN (s."策略_籌碼_判定" = '無'::text) THEN '無'::text
                    ELSE '資料不足'::text
                END AS "_複合籌碼狀態",
                CASE
                    WHEN ((s."法人總買超_億" IS NULL) OR (s."散戶多空比_微台" IS NULL)) THEN '資料不足'::text
                    WHEN (s."策略_籌碼_判定" = ANY (ARRAY['多'::text, '空'::text, '無'::text])) THEN s."策略_籌碼_判定"
                    ELSE '資料不足'::text
                END AS "_核心籌碼狀態"
           FROM report.v_taiex_report_composite_source_20260823 s
        ), calculated AS (
         SELECT n."日期",
            n."年",
            n."月",
            n."星期",
            n."週次",
            n."天別",
            n."開盤價",
            n."收盤價",
            n."最高價",
            n."最低價",
            n."漲跌",
            n."漲跌幅_pct",
            n."收盤_開盤",
            n."實體_昨收_pct",
            n."實體_開盤_pct",
            n."日振幅",
            n."日振幅_昨收_pct",
            n."日振福_開盤_pct",
            n."成交量",
            n."5日最大量_日期",
            n."5日最大量",
            n."10日最大量_日期",
            n."10日最大量",
            n."20日最大量_日期",
            n."20日最大量",
            n."60日最大量_日期",
            n."60日最大量",
            n."5日均量",
            n."10日均量",
            n."20日均量",
            n."60日均量",
            n."量增率_pct",
            n."5日量增率_pct",
            n."10日量增率_pct",
            n."20日量增率_pct",
            n."60日量增率_pct",
            n."當日價量",
            n."5日價量",
            n."10日價量",
            n."20日價量",
            n."60日價量",
            n."K線型態",
            n."跳空缺口",
            n."跳空缺口_昨收_pct",
            n."實體_pct",
            n."上影_pct",
            n."下影_pct",
            n."5MA",
            n."10MA",
            n."20MA",
            n."60MA",
            n."120MA",
            n."240MA",
            n."5日上升幅度",
            n."10日上升幅度",
            n."20日上升幅度",
            n."60日上升幅度",
            n."5日上升幅度_pct",
            n."10日上升幅度_pct",
            n."20日上升幅度_pct",
            n."60日上升幅度_pct",
            n."5日扣抵值",
            n."10日扣抵值",
            n."20日扣抵值",
            n."60日扣抵值",
            n."5日明日扣抵影響_點",
            n."10日明日扣抵影響_點",
            n."20日明日扣抵影響_點",
            n."60日明日扣抵影響_點",
            n."5日明日扣抵影響_pct",
            n."10日明日扣抵影響_pct",
            n."20日明日扣抵影響_pct",
            n."60日明日扣抵影響_pct",
            n."5日乖離",
            n."10日乖離",
            n."20日乖離",
            n."60日乖離",
            n."布林標準差",
            n."布林上軌",
            n."布林下軌",
            n."總成交金額_億",
            n."法人總買超_億",
            n."買超_外資_億",
            n."買超_投信_億",
            n."買超_自營商_億",
            n."買超_融資_億",
            n."法人淨買賣超_扣融資_億",
            n."法人買賣超_總成交_pct",
            n."散戶多空比_微台",
            n."漲跌幅百分位數",
            n."漲跌幅級距",
            n."三大法人買賣超百分位數",
            n."三大法人買賣超級距",
            n."資金走向判定",
            n."位置",
            n."策略_價量昨日_判斷",
            n."策略_價量五日_判斷",
            n."MA方向_5",
            n."MA方向_10",
            n."MA方向_20",
            n."趨勢",
            n."法人籌碼",
            n."散戶籌碼",
            n."策略_籌碼_判定",
            n."策略_價量昨日_線型",
            n."策略_價量五日_線型",
            n."策略_價量昨日_籌碼",
            n."策略_價量五日_籌碼",
            n."策略_線型_籌碼",
            n."策略_核心",
            n."_線型位置",
            n."_複合籌碼狀態",
            n."_核心籌碼狀態",
                CASE
                    WHEN ((n."策略_價量昨日_判斷" IS NULL) OR (n."策略_價量昨日_判斷" = '資料不足'::text) OR (n."_線型位置" = '資料不足'::text)) THEN '資料不足'::text
                    WHEN (n."策略_價量昨日_判斷" = '無'::text) THEN '無'::text
                    WHEN ((n."策略_價量昨日_判斷" = '多'::text) AND (n."_線型位置" = '多'::text)) THEN '多'::text
                    WHEN ((n."策略_價量昨日_判斷" = '多'::text) AND (n."_線型位置" = ANY (ARRAY['空'::text, '盤整(高)'::text, '盤整(中)'::text]))) THEN 'X'::text
                    WHEN ((n."策略_價量昨日_判斷" = '多'::text) AND (n."_線型位置" = '盤整(低)'::text)) THEN '多'::text
                    WHEN ((n."策略_價量昨日_判斷" = '空'::text) AND (n."_線型位置" = '多'::text)) THEN 'X'::text
                    WHEN ((n."策略_價量昨日_判斷" = '空'::text) AND (n."_線型位置" = ANY (ARRAY['空'::text, '盤整(高)'::text]))) THEN '空'::text
                    WHEN ((n."策略_價量昨日_判斷" = '空'::text) AND (n."_線型位置" = ANY (ARRAY['盤整(中)'::text, '盤整(低)'::text]))) THEN 'X'::text
                    ELSE '資料不足'::text
                END AS "_策略_價量昨日_線型修正",
                CASE
                    WHEN ((n."策略_價量五日_判斷" IS NULL) OR (n."策略_價量五日_判斷" = '資料不足'::text) OR (n."_線型位置" = '資料不足'::text)) THEN '資料不足'::text
                    WHEN (n."策略_價量五日_判斷" = '無'::text) THEN '無'::text
                    WHEN ((n."策略_價量五日_判斷" = '多'::text) AND (n."_線型位置" = '多'::text)) THEN '多'::text
                    WHEN ((n."策略_價量五日_判斷" = '多'::text) AND (n."_線型位置" = ANY (ARRAY['空'::text, '盤整(高)'::text, '盤整(中)'::text]))) THEN 'X'::text
                    WHEN ((n."策略_價量五日_判斷" = '多'::text) AND (n."_線型位置" = '盤整(低)'::text)) THEN '多'::text
                    WHEN ((n."策略_價量五日_判斷" = '空'::text) AND (n."_線型位置" = '多'::text)) THEN 'X'::text
                    WHEN ((n."策略_價量五日_判斷" = '空'::text) AND (n."_線型位置" = ANY (ARRAY['空'::text, '盤整(高)'::text]))) THEN '空'::text
                    WHEN ((n."策略_價量五日_判斷" = '空'::text) AND (n."_線型位置" = ANY (ARRAY['盤整(中)'::text, '盤整(低)'::text]))) THEN 'X'::text
                    ELSE '資料不足'::text
                END AS "_策略_價量五日_線型修正",
                CASE
                    WHEN ((n."策略_價量昨日_判斷" IS NULL) OR (n."策略_價量昨日_判斷" = '資料不足'::text) OR (n."_複合籌碼狀態" = '資料不足'::text)) THEN '資料不足'::text
                    WHEN (n."策略_價量昨日_判斷" = '無'::text) THEN '無'::text
                    WHEN ((n."策略_價量昨日_判斷" = '多'::text) AND (n."_複合籌碼狀態" = ANY (ARRAY['多'::text, '無(法人買超)'::text]))) THEN '多'::text
                    WHEN ((n."策略_價量昨日_判斷" = '多'::text) AND (n."_複合籌碼狀態" = ANY (ARRAY['空'::text, '無(法人賣超)'::text]))) THEN 'X'::text
                    WHEN ((n."策略_價量昨日_判斷" = '空'::text) AND (n."_複合籌碼狀態" = ANY (ARRAY['多'::text, '無(法人買超)'::text]))) THEN 'X'::text
                    WHEN ((n."策略_價量昨日_判斷" = '空'::text) AND (n."_複合籌碼狀態" = ANY (ARRAY['空'::text, '無(法人賣超)'::text]))) THEN '空'::text
                    WHEN (n."_複合籌碼狀態" = '無'::text) THEN '無'::text
                    ELSE '資料不足'::text
                END AS "_策略_價量昨日_籌碼修正",
                CASE
                    WHEN ((n."策略_價量五日_判斷" IS NULL) OR (n."策略_價量五日_判斷" = '資料不足'::text) OR (n."_複合籌碼狀態" = '資料不足'::text)) THEN '資料不足'::text
                    WHEN (n."策略_價量五日_判斷" = '無'::text) THEN '無'::text
                    WHEN ((n."策略_價量五日_判斷" = '多'::text) AND (n."_複合籌碼狀態" = ANY (ARRAY['多'::text, '無(法人買超)'::text]))) THEN '多'::text
                    WHEN ((n."策略_價量五日_判斷" = '多'::text) AND (n."_複合籌碼狀態" = ANY (ARRAY['空'::text, '無(法人賣超)'::text]))) THEN 'X'::text
                    WHEN ((n."策略_價量五日_判斷" = '空'::text) AND (n."_複合籌碼狀態" = ANY (ARRAY['多'::text, '無(法人買超)'::text]))) THEN 'X'::text
                    WHEN ((n."策略_價量五日_判斷" = '空'::text) AND (n."_複合籌碼狀態" = ANY (ARRAY['空'::text, '無(法人賣超)'::text]))) THEN '空'::text
                    WHEN (n."_複合籌碼狀態" = '無'::text) THEN '無'::text
                    ELSE '資料不足'::text
                END AS "_策略_價量五日_籌碼修正",
                CASE
                    WHEN ((n."_線型位置" = '資料不足'::text) OR (n."_複合籌碼狀態" = '資料不足'::text)) THEN '資料不足'::text
                    WHEN (n."_複合籌碼狀態" = '無'::text) THEN '無'::text
                    WHEN ((n."_線型位置" = '多'::text) AND (n."_複合籌碼狀態" = ANY (ARRAY['多'::text, '無(法人買超)'::text]))) THEN '多'::text
                    WHEN ((n."_線型位置" = '多'::text) AND (n."_複合籌碼狀態" = ANY (ARRAY['空'::text, '無(法人賣超)'::text]))) THEN 'X'::text
                    WHEN ((n."_線型位置" = ANY (ARRAY['空'::text, '盤整(高)'::text])) AND (n."_複合籌碼狀態" = ANY (ARRAY['多'::text, '無(法人買超)'::text]))) THEN 'X'::text
                    WHEN ((n."_線型位置" = ANY (ARRAY['空'::text, '盤整(高)'::text])) AND (n."_複合籌碼狀態" = ANY (ARRAY['空'::text, '無(法人賣超)'::text]))) THEN '空'::text
                    WHEN (n."_線型位置" = '盤整(中)'::text) THEN 'X'::text
                    WHEN ((n."_線型位置" = '盤整(低)'::text) AND (n."_複合籌碼狀態" = ANY (ARRAY['多'::text, '無(法人買超)'::text]))) THEN '多'::text
                    WHEN ((n."_線型位置" = '盤整(低)'::text) AND (n."_複合籌碼狀態" = ANY (ARRAY['空'::text, '無(法人賣超)'::text]))) THEN 'X'::text
                    ELSE '資料不足'::text
                END AS "_策略_線型_籌碼修正",
                CASE
                    WHEN ((n."策略_價量五日_判斷" IS NULL) OR (n."策略_價量五日_判斷" = '資料不足'::text) OR (n."_線型位置" = '資料不足'::text) OR (n."_核心籌碼狀態" = '資料不足'::text)) THEN '資料不足'::text
                    WHEN (n."策略_價量五日_判斷" = '無'::text) THEN '無'::text
                    WHEN ((n."策略_價量五日_判斷" = '多'::text) AND (n."_線型位置" = '多'::text)) THEN '多'::text
                    WHEN ((n."策略_價量五日_判斷" = '多'::text) AND (n."_線型位置" = ANY (ARRAY['空'::text, '盤整(高)'::text]))) THEN
                    CASE
                        WHEN (n."_核心籌碼狀態" = '多'::text) THEN '多'::text
                        ELSE '空'::text
                    END
                    WHEN ((n."策略_價量五日_判斷" = '多'::text) AND (n."_線型位置" = '盤整(中)'::text)) THEN
                    CASE
                        WHEN (n."_核心籌碼狀態" = '空'::text) THEN '空'::text
                        ELSE '多'::text
                    END
                    WHEN ((n."策略_價量五日_判斷" = '多'::text) AND (n."_線型位置" = '盤整(低)'::text)) THEN '多'::text
                    WHEN ((n."策略_價量五日_判斷" = '空'::text) AND (n."_線型位置" = '多'::text)) THEN
                    CASE
                        WHEN (n."_核心籌碼狀態" = '空'::text) THEN '空'::text
                        ELSE '多'::text
                    END
                    WHEN ((n."策略_價量五日_判斷" = '空'::text) AND (n."_線型位置" = ANY (ARRAY['空'::text, '盤整(高)'::text]))) THEN '空'::text
                    WHEN ((n."策略_價量五日_判斷" = '空'::text) AND (n."_線型位置" = '盤整(中)'::text)) THEN
                    CASE
                        WHEN (n."_核心籌碼狀態" = '多'::text) THEN '多'::text
                        ELSE '空'::text
                    END
                    WHEN ((n."策略_價量五日_判斷" = '空'::text) AND (n."_線型位置" = '盤整(低)'::text)) THEN
                    CASE
                        WHEN (n."_核心籌碼狀態" = '空'::text) THEN '空'::text
                        ELSE '多'::text
                    END
                    ELSE '資料不足'::text
                END AS "_策略_核心修正"
           FROM normalized n
        )
 SELECT "日期",
    "年",
    "月",
    "星期",
    "週次",
    "天別",
    "開盤價",
    "收盤價",
    "最高價",
    "最低價",
    "漲跌",
    "漲跌幅_pct",
    "收盤_開盤",
    "實體_昨收_pct",
    "實體_開盤_pct",
    "日振幅",
    "日振幅_昨收_pct",
    "日振福_開盤_pct",
    "成交量",
    "5日最大量_日期",
    "5日最大量",
    "10日最大量_日期",
    "10日最大量",
    "20日最大量_日期",
    "20日最大量",
    "60日最大量_日期",
    "60日最大量",
    "5日均量",
    "10日均量",
    "20日均量",
    "60日均量",
    "量增率_pct",
    "5日量增率_pct",
    "10日量增率_pct",
    "20日量增率_pct",
    "60日量增率_pct",
    "當日價量",
    "5日價量",
    "10日價量",
    "20日價量",
    "60日價量",
    "K線型態",
    "跳空缺口",
    "跳空缺口_昨收_pct",
    "實體_pct",
    "上影_pct",
    "下影_pct",
    "5MA",
    "10MA",
    "20MA",
    "60MA",
    "120MA",
    "240MA",
    "5日上升幅度",
    "10日上升幅度",
    "20日上升幅度",
    "60日上升幅度",
    "5日上升幅度_pct",
    "10日上升幅度_pct",
    "20日上升幅度_pct",
    "60日上升幅度_pct",
    "5日扣抵值",
    "10日扣抵值",
    "20日扣抵值",
    "60日扣抵值",
    "5日明日扣抵影響_點",
    "10日明日扣抵影響_點",
    "20日明日扣抵影響_點",
    "60日明日扣抵影響_點",
    "5日明日扣抵影響_pct",
    "10日明日扣抵影響_pct",
    "20日明日扣抵影響_pct",
    "60日明日扣抵影響_pct",
    "5日乖離",
    "10日乖離",
    "20日乖離",
    "60日乖離",
    "布林標準差",
    "布林上軌",
    "布林下軌",
    "總成交金額_億",
    "法人總買超_億",
    "買超_外資_億",
    "買超_投信_億",
    "買超_自營商_億",
    "買超_融資_億",
    "法人淨買賣超_扣融資_億",
    "法人買賣超_總成交_pct",
    "散戶多空比_微台",
    "漲跌幅百分位數",
    "漲跌幅級距",
    "三大法人買賣超百分位數",
    "三大法人買賣超級距",
    "資金走向判定",
    "位置",
    "策略_價量昨日_判斷",
    "策略_價量五日_判斷",
    "MA方向_5",
    "MA方向_10",
    "MA方向_20",
    "趨勢",
    "法人籌碼",
    "散戶籌碼",
    "策略_籌碼_判定",
    "_策略_價量昨日_線型修正" AS "策略_價量昨日_線型",
    "_策略_價量五日_線型修正" AS "策略_價量五日_線型",
    "_策略_價量昨日_籌碼修正" AS "策略_價量昨日_籌碼",
    "_策略_價量五日_籌碼修正" AS "策略_價量五日_籌碼",
    "_策略_線型_籌碼修正" AS "策略_線型_籌碼",
    "_策略_核心修正" AS "策略_核心"
   FROM calculated c;


--
-- Name: VIEW v_taiex_report_position_source_20260902; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON VIEW report.v_taiex_report_position_source_20260902 IS '加權指數報；2026-08-23 修正複合策略與核心策略，納入盤整位置、法人籌碼補判及資料不足處理';


--
-- Name: v_taiex_report_position_calc_20260902; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_taiex_report_position_calc_20260902 AS
 WITH positioned AS (
         SELECT s."日期",
            s."趨勢",
            s."法人籌碼",
            s."策略_籌碼_判定",
            public.fn_report_trend_position(s."趨勢", s."收盤價", s."5MA", s."布林上軌", s."布林下軌") AS "位置",
                CASE
                    WHEN (s."漲跌" > (0)::numeric) THEN '價漲'::text
                    WHEN (s."漲跌" < (0)::numeric) THEN '價跌'::text
                    WHEN (s."漲跌" = (0)::numeric) THEN '價同'::text
                    ELSE NULL::text
                END AS "價漲跌_昨日",
                CASE
                    WHEN (s."量增率_pct" > (0)::numeric) THEN '量增'::text
                    WHEN (s."量增率_pct" < (0)::numeric) THEN '量縮'::text
                    WHEN (s."量增率_pct" = (0)::numeric) THEN '量同'::text
                    ELSE NULL::text
                END AS "量增減_昨日",
                CASE
                    WHEN (s."5日量增率_pct" > (0)::numeric) THEN '量增'::text
                    WHEN (s."5日量增率_pct" < (0)::numeric) THEN '量縮'::text
                    WHEN (s."5日量增率_pct" = (0)::numeric) THEN '量同'::text
                    ELSE NULL::text
                END AS "量增減_五日"
           FROM report.v_taiex_report_position_source_20260902 s
        ), price_volume AS (
         SELECT p."日期",
            p."趨勢",
            p."法人籌碼",
            p."策略_籌碼_判定",
            p."位置",
            p."價漲跌_昨日",
            p."量增減_昨日",
            p."量增減_五日",
            public.fn_futures_price_volume_strategy(p."價漲跌_昨日", p."量增減_昨日", p."趨勢", p."位置") AS "策略_價量昨日_判斷",
            public.fn_futures_price_volume_strategy(p."價漲跌_昨日", p."量增減_五日", p."趨勢", p."位置") AS "策略_價量五日_判斷"
           FROM positioned p
        )
 SELECT "日期",
    "位置",
    "策略_價量昨日_判斷",
    "策略_價量五日_判斷",
    public.fn_futures_price_line_strategy("策略_價量昨日_判斷", "趨勢", "位置") AS "策略_價量昨日_線型",
    public.fn_futures_price_line_strategy("策略_價量五日_判斷", "趨勢", "位置") AS "策略_價量五日_線型",
    public.fn_futures_price_chip_strategy("策略_價量昨日_判斷", "策略_籌碼_判定", "法人籌碼") AS "策略_價量昨日_籌碼",
    public.fn_futures_price_chip_strategy("策略_價量五日_判斷", "策略_籌碼_判定", "法人籌碼") AS "策略_價量五日_籌碼",
    public.fn_report_line_chip_strategy("趨勢", "位置", "策略_籌碼_判定", "法人籌碼") AS "策略_線型_籌碼",
    public.fn_report_core_strategy("策略_價量五日_判斷", "趨勢", "位置", "策略_籌碼_判定", "法人籌碼") AS "策略_核心",
    public.fn_report_core_strategy("策略_價量昨日_判斷", "趨勢", "位置", "策略_籌碼_判定", "法人籌碼") AS "策略_核心_昨價量",
    public.fn_report_core_strategy("策略_價量五日_判斷", "趨勢", "位置", "策略_籌碼_判定", "法人籌碼") AS "策略_核心_五日價量"
   FROM price_volume;


--
-- Name: VIEW v_taiex_report_position_calc_20260902; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON VIEW report.v_taiex_report_position_calc_20260902 IS '加權指數位置及策略窄表；2026-09-09 新增核心策略昨日價量版與五日價量版，舊核心欄位維持五日版相容。';


--
-- Name: v_taiex_report; Type: VIEW; Schema: report; Owner: -
--

CREATE VIEW report.v_taiex_report AS
 SELECT s."日期",
    s."年",
    s."月",
    s."星期",
    s."週次",
    s."天別",
    s."開盤價",
    s."收盤價",
    s."最高價",
    s."最低價",
    s."漲跌",
    s."漲跌幅_pct",
    s."收盤_開盤",
    s."實體_昨收_pct",
    s."實體_開盤_pct",
    s."日振幅",
    s."日振幅_昨收_pct",
    s."日振福_開盤_pct",
    s."成交量",
    s."5日最大量_日期",
    s."5日最大量",
    s."10日最大量_日期",
    s."10日最大量",
    s."20日最大量_日期",
    s."20日最大量",
    s."60日最大量_日期",
    s."60日最大量",
    s."5日均量",
    s."10日均量",
    s."20日均量",
    s."60日均量",
    s."量增率_pct",
    s."5日量增率_pct",
    s."10日量增率_pct",
    s."20日量增率_pct",
    s."60日量增率_pct",
    s."當日價量",
    s."5日價量",
    s."10日價量",
    s."20日價量",
    s."60日價量",
    s."K線型態",
    s."跳空缺口",
    s."跳空缺口_昨收_pct",
    s."實體_pct",
    s."上影_pct",
    s."下影_pct",
    s."5MA",
    s."10MA",
    s."20MA",
    s."60MA",
    s."120MA",
    s."240MA",
    s."5日上升幅度",
    s."10日上升幅度",
    s."20日上升幅度",
    s."60日上升幅度",
    s."5日上升幅度_pct",
    s."10日上升幅度_pct",
    s."20日上升幅度_pct",
    s."60日上升幅度_pct",
    s."5日扣抵值",
    s."10日扣抵值",
    s."20日扣抵值",
    s."60日扣抵值",
    s."5日明日扣抵影響_點",
    s."10日明日扣抵影響_點",
    s."20日明日扣抵影響_點",
    s."60日明日扣抵影響_點",
    s."5日明日扣抵影響_pct",
    s."10日明日扣抵影響_pct",
    s."20日明日扣抵影響_pct",
    s."60日明日扣抵影響_pct",
    s."5日乖離",
    s."10日乖離",
    s."20日乖離",
    s."60日乖離",
    s."布林標準差",
    s."布林上軌",
    s."布林下軌",
    s."總成交金額_億",
    s."法人總買超_億",
    s."買超_外資_億",
    s."買超_投信_億",
    s."買超_自營商_億",
    s."買超_融資_億",
    s."法人淨買賣超_扣融資_億",
    s."法人買賣超_總成交_pct",
    s."散戶多空比_微台",
    s."漲跌幅百分位數",
    s."漲跌幅級距",
    s."三大法人買賣超百分位數",
    s."三大法人買賣超級距",
    s."資金走向判定",
    c."位置",
    c."策略_價量昨日_判斷",
    c."策略_價量五日_判斷",
    s."MA方向_5",
    s."MA方向_10",
    s."MA方向_20",
    s."趨勢",
    s."法人籌碼",
    s."散戶籌碼",
    s."策略_籌碼_判定",
    c."策略_價量昨日_線型",
    c."策略_價量五日_線型",
    c."策略_價量昨日_籌碼",
    c."策略_價量五日_籌碼",
    c."策略_線型_籌碼",
    c."策略_核心",
    c."策略_核心_昨價量",
    c."策略_核心_五日價量"
   FROM (report.v_taiex_report_position_source_20260902 s
     JOIN report.v_taiex_report_position_calc_20260902 c USING ("日期"));


--
-- Name: VIEW v_taiex_report; Type: COMMENT; Schema: report; Owner: -
--

COMMENT ON VIEW report.v_taiex_report IS '加權指數對外報表；2026-09-09 核心策略拆分為昨價量與五日價量兩欄，舊核心欄位維持五日版相容。';


--
-- Name: analysis_run; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.analysis_run (
    run_id uuid NOT NULL,
    analysis_name text NOT NULL,
    futures_id text DEFAULT 'TX'::text NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    bucket_edges jsonb NOT NULL,
    metric_version text NOT NULL,
    source_relation text NOT NULL,
    status text NOT NULL,
    source_rows integer DEFAULT 0 NOT NULL,
    valid_rows integer DEFAULT 0 NOT NULL,
    excluded_rows integer DEFAULT 0 NOT NULL,
    exclusion_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    started_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    finished_at timestamp without time zone,
    CONSTRAINT ck_research_analysis_run_dates CHECK ((start_date <= end_date)),
    CONSTRAINT ck_research_analysis_run_status CHECK ((status = ANY (ARRAY['running'::text, 'completed'::text, 'failed'::text])))
);


--
-- Name: futures_night_bucket_daily; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.futures_night_bucket_daily (
    run_id uuid NOT NULL,
    futures_id text NOT NULL,
    group_date date NOT NULL,
    result_date date NOT NULL,
    bucket_id smallint NOT NULL,
    bucket_label text NOT NULL,
    base_day_close double precision NOT NULL,
    night_open double precision,
    night_high double precision,
    night_low double precision,
    night_close double precision NOT NULL,
    result_day_open double precision NOT NULL,
    result_day_high double precision NOT NULL,
    result_day_low double precision NOT NULL,
    result_day_close double precision NOT NULL,
    night_return_points double precision NOT NULL,
    night_return_pct double precision NOT NULL,
    next_open_return_points double precision NOT NULL,
    next_open_return_pct double precision NOT NULL,
    next_close_return_points double precision NOT NULL,
    next_close_return_pct double precision NOT NULL,
    next_intraday_return_points double precision NOT NULL,
    next_intraday_return_pct double precision NOT NULL,
    next_range_points double precision NOT NULL,
    next_range_prev_close_pct double precision NOT NULL,
    next_range_open_pct double precision NOT NULL,
    night_close_to_next_open_points double precision CONSTRAINT futures_night_bucket_daily_night_close_to_next_open_po_not_null NOT NULL,
    night_close_to_next_open_pct double precision CONSTRAINT futures_night_bucket_daily_night_close_to_next_open_pc_not_null NOT NULL,
    max_up_from_open_points double precision NOT NULL,
    max_up_from_open_pct double precision NOT NULL,
    max_down_from_open_points double precision NOT NULL,
    max_down_from_open_pct double precision NOT NULL,
    close_location_pct double precision,
    next_day_direction smallint NOT NULL,
    direction_continued boolean,
    direction_reversed boolean,
    filled_base_day_close boolean NOT NULL,
    data_quality text DEFAULT 'valid'::text NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_research_group_result_dates CHECK ((group_date < result_date)),
    CONSTRAINT ck_research_next_day_direction CHECK ((next_day_direction = ANY (ARRAY['-1'::integer, 0, 1]))),
    CONSTRAINT ck_research_night_bucket_id CHECK (((bucket_id >= 1) AND (bucket_id <= 10)))
);


--
-- Name: futures_night_bucket_summary; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.futures_night_bucket_summary (
    run_id uuid NOT NULL,
    bucket_id smallint NOT NULL,
    bucket_label text NOT NULL,
    metric_name text NOT NULL,
    sample_count integer NOT NULL,
    mean_value double precision,
    median_value double precision,
    std_value double precision,
    min_value double precision,
    q10_value double precision,
    q25_value double precision,
    q75_value double precision,
    q90_value double precision,
    max_value double precision,
    positive_count integer DEFAULT 0 NOT NULL,
    positive_rate_pct double precision,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_research_night_summary_bucket CHECK (((bucket_id >= 1) AND (bucket_id <= 10)))
);


--
-- Name: futures_night_bucket_yearly; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.futures_night_bucket_yearly (
    run_id uuid NOT NULL,
    result_year integer NOT NULL,
    bucket_id smallint NOT NULL,
    bucket_label text NOT NULL,
    metric_name text NOT NULL,
    sample_count integer NOT NULL,
    mean_value double precision,
    median_value double precision,
    std_value double precision,
    min_value double precision,
    q10_value double precision,
    q25_value double precision,
    q75_value double precision,
    q90_value double precision,
    max_value double precision,
    positive_count integer DEFAULT 0 NOT NULL,
    positive_rate_pct double precision,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_research_night_yearly_bucket CHECK (((bucket_id >= 1) AND (bucket_id <= 10)))
);


--
-- Name: market_daily_feature; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.market_daily_feature (
    market_type text NOT NULL,
    instrument_id text NOT NULL,
    session text NOT NULL,
    trade_date date NOT NULL,
    feature_name text NOT NULL,
    feature_version text DEFAULT '1.0.0'::text NOT NULL,
    value_numeric double precision,
    value_text text,
    source_relation text NOT NULL,
    calculated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_market_daily_feature_instrument_id_nonempty CHECK ((btrim(instrument_id) <> ''::text)),
    CONSTRAINT ck_market_daily_feature_market_type_nonempty CHECK ((btrim(market_type) <> ''::text)),
    CONSTRAINT ck_market_daily_feature_name_nonempty CHECK ((btrim(feature_name) <> ''::text)),
    CONSTRAINT ck_market_daily_feature_one_value CHECK ((num_nonnulls(value_numeric, value_text) = 1)),
    CONSTRAINT ck_market_daily_feature_session_nonempty CHECK ((btrim(session) <> ''::text)),
    CONSTRAINT ck_market_daily_feature_version_nonempty CHECK ((btrim(feature_version) <> ''::text))
);


--
-- Name: TABLE market_daily_feature; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON TABLE research.market_daily_feature IS '對應正式行情／報表的通用每日研究特徵長表；供探索、統計、模型與回測使用，不增加正式日報表欄位。';


--
-- Name: COLUMN market_daily_feature.market_type; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON COLUMN research.market_daily_feature.market_type IS '市場類型，例如 FUTURES、INDEX。';


--
-- Name: COLUMN market_daily_feature.instrument_id; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON COLUMN research.market_daily_feature.instrument_id IS '商品代碼，例如 TX、TAIEX。';


--
-- Name: COLUMN market_daily_feature.session; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON COLUMN research.market_daily_feature.session IS '交易時段，例如 DAY、NIGHT、FULL。';


--
-- Name: COLUMN market_daily_feature.feature_name; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON COLUMN research.market_daily_feature.feature_name IS '穩定的英文特徵鍵；新增探索指標時增加新鍵即可。';


--
-- Name: COLUMN market_daily_feature.feature_version; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON COLUMN research.market_daily_feature.feature_version IS '公式版本；公式定義改變時建立新版本，避免覆蓋舊研究結果。';


--
-- Name: COLUMN market_daily_feature.value_numeric; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON COLUMN research.market_daily_feature.value_numeric IS '數值型特徵值；pct 特徵採百分點，例如 0.82 代表 0.82%。';


--
-- Name: COLUMN market_daily_feature.value_text; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON COLUMN research.market_daily_feature.value_text IS '文字／分類型特徵值；與 value_numeric 必須且只能填一個。';


--
-- Name: COLUMN market_daily_feature.source_relation; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON COLUMN research.market_daily_feature.source_relation IS '產生特徵的來源 relation。';


--
-- Name: night_retrace_backtest_run; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.night_retrace_backtest_run (
    run_id uuid NOT NULL,
    strategy_name text NOT NULL,
    strategy_version text NOT NULL,
    futures_id text DEFAULT 'TX'::text NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    config jsonb NOT NULL,
    status text NOT NULL,
    candidate_days integer DEFAULT 0 NOT NULL,
    bar_rows integer DEFAULT 0 NOT NULL,
    missing_days integer DEFAULT 0 NOT NULL,
    error_message text,
    started_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    finished_at timestamp without time zone,
    CONSTRAINT ck_night_retrace_run_dates CHECK ((start_date <= end_date)),
    CONSTRAINT ck_night_retrace_run_status CHECK ((status = ANY (ARRAY['running'::text, 'completed'::text, 'failed'::text])))
);


--
-- Name: night_retrace_parameter_summary; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.night_retrace_parameter_summary (
    run_id uuid NOT NULL,
    parameter_id text NOT NULL,
    x_label text NOT NULL,
    x_min_pct double precision NOT NULL,
    x_max_pct double precision,
    y_retrace_pct double precision NOT NULL,
    z_stop_pct double precision NOT NULL,
    segment text NOT NULL,
    signal_count integer NOT NULL,
    entered_count integer NOT NULL,
    entry_rate_pct double precision,
    missing_count integer NOT NULL,
    win_rate_pct double precision,
    total_net_points double precision NOT NULL,
    average_net_points double precision,
    median_net_points double precision,
    profit_factor double precision,
    max_drawdown_points double precision,
    max_losing_streak integer NOT NULL,
    take_profit_count integer NOT NULL,
    stop_loss_count integer NOT NULL,
    close_exit_count integer NOT NULL,
    ambiguous_count integer NOT NULL,
    positive_year_count integer NOT NULL,
    year_count integer NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT ck_night_retrace_segment CHECK ((segment = ANY (ARRAY['OVERALL'::text, 'TRAIN'::text, 'VALIDATION'::text, 'TEST'::text])))
);


--
-- Name: night_retrace_trade_detail; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.night_retrace_trade_detail (
    run_id uuid NOT NULL,
    rank integer NOT NULL,
    parameter_id text NOT NULL,
    x_label text NOT NULL,
    group_date date NOT NULL,
    trade_date date,
    contract_date text,
    base_day_close double precision NOT NULL,
    night_close double precision NOT NULL,
    night_return_pct double precision NOT NULL,
    y_retrace_pct double precision NOT NULL,
    z_stop_pct double precision NOT NULL,
    entry_limit_price double precision NOT NULL,
    entry_status text NOT NULL,
    exit_reason text NOT NULL,
    entry_at timestamp without time zone,
    entry_price double precision,
    simulated_entry_price double precision,
    stop_price double precision,
    target_price double precision,
    exit_at timestamp without time zone,
    exit_price double precision,
    simulated_exit_price double precision,
    gross_points double precision NOT NULL,
    net_points double precision NOT NULL,
    mfe_points double precision,
    mae_points double precision,
    ambiguous_bar boolean NOT NULL,
    data_quality text NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: night_retrace_yearly_summary; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.night_retrace_yearly_summary (
    run_id uuid NOT NULL,
    parameter_id text NOT NULL,
    x_label text NOT NULL,
    x_min_pct double precision NOT NULL,
    x_max_pct double precision,
    y_retrace_pct double precision NOT NULL,
    z_stop_pct double precision NOT NULL,
    result_year integer NOT NULL,
    signal_count integer NOT NULL,
    entered_count integer NOT NULL,
    entry_rate_pct double precision,
    missing_count integer NOT NULL,
    win_rate_pct double precision,
    total_net_points double precision NOT NULL,
    average_net_points double precision,
    median_net_points double precision,
    profit_factor double precision,
    max_drawdown_points double precision,
    max_losing_streak integer NOT NULL,
    take_profit_count integer NOT NULL,
    stop_loss_count integer NOT NULL,
    close_exit_count integer NOT NULL,
    ambiguous_count integer NOT NULL,
    positive_year_count integer NOT NULL,
    year_count integer NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: yu_candidate_missed_levels; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.yu_candidate_missed_levels (
    missed_id bigint NOT NULL,
    run_id uuid NOT NULL,
    market text NOT NULL,
    missed_level numeric NOT NULL,
    role text,
    source_date date,
    source_rule text,
    reason_code text DEFAULT 'MISSING'::text,
    note text,
    reviewer text,
    reviewed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE yu_candidate_missed_levels; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON TABLE research.yu_candidate_missed_levels IS 'Yu補登但程式沒有產生的漏點，用來改善下一版候選規則。';


--
-- Name: yu_candidate_missed_levels_missed_id_seq; Type: SEQUENCE; Schema: research; Owner: -
--

ALTER TABLE research.yu_candidate_missed_levels ALTER COLUMN missed_id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME research.yu_candidate_missed_levels_missed_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: yu_candidate_raw_levels; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.yu_candidate_raw_levels (
    run_id uuid NOT NULL,
    raw_level_id text NOT NULL,
    market text NOT NULL,
    source_date date NOT NULL,
    source_rule text NOT NULL,
    source_part text NOT NULL,
    level_value numeric NOT NULL,
    formation_score numeric,
    validity_score numeric,
    current_status text,
    attributes jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_category text,
    original_role text
);


--
-- Name: TABLE yu_candidate_raw_levels; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON TABLE research.yu_candidate_raw_levels IS '尚未合併的原始候選點，永久保留形成規則、K棒部位與當時分數。';


--
-- Name: COLUMN yu_candidate_raw_levels.source_category; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON COLUMN research.yu_candidate_raw_levels.source_category IS 'STATIC_K_LEVEL、DYNAMIC_INDICATOR或EXTERNAL_MARKET。';


--
-- Name: COLUMN yu_candidate_raw_levels.original_role; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON COLUMN research.yu_candidate_raw_levels.original_role IS '形成時的支撐／壓力／雙向角色，用來判斷突破後角色互換。';


--
-- Name: yu_candidate_reviews; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.yu_candidate_reviews (
    run_id uuid NOT NULL,
    zone_id text NOT NULL,
    is_key_level boolean,
    is_trade_candidate boolean,
    current_status text,
    review_action text,
    reason_codes text[],
    corrected_level numeric,
    corrected_role text,
    reviewer text,
    note text,
    reviewed_at timestamp with time zone
);


--
-- Name: TABLE yu_candidate_reviews; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON TABLE research.yu_candidate_reviews IS 'Yu人工標記；關鍵點位與當日交易候選分開記錄。';


--
-- Name: yu_candidate_runs; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.yu_candidate_runs (
    run_id uuid NOT NULL,
    generator_code text NOT NULL,
    generator_version integer NOT NULL,
    as_of_date date NOT NULL,
    source_data_max_date date NOT NULL,
    config jsonb NOT NULL,
    raw_level_count integer DEFAULT 0 NOT NULL,
    zone_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE yu_candidate_runs; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON TABLE research.yu_candidate_runs IS '每個資料截止日的一次Yu關鍵點位候選生成；刪除run會連動刪除該次候選與審核。';


--
-- Name: yu_candidate_zone_sources; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.yu_candidate_zone_sources (
    run_id uuid NOT NULL,
    zone_id text NOT NULL,
    raw_level_id text NOT NULL
);


--
-- Name: yu_candidate_zones; Type: TABLE; Schema: research; Owner: -
--

CREATE TABLE research.yu_candidate_zones (
    run_id uuid NOT NULL,
    zone_id text NOT NULL,
    market text NOT NULL,
    zone_lower numeric NOT NULL,
    zone_upper numeric NOT NULL,
    representative_level numeric NOT NULL,
    source_count integer NOT NULL,
    rule_count integer NOT NULL,
    formation_score numeric,
    validity_score numeric,
    current_status text,
    source_category text,
    review_score numeric,
    selected_for_review boolean DEFAULT false NOT NULL
);


--
-- Name: TABLE yu_candidate_zones; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON TABLE research.yu_candidate_zones IS '相近原始點合併後的可審核區域；不可取代raw_levels的來源資訊。';


--
-- Name: COLUMN yu_candidate_zones.selected_for_review; Type: COMMENT; Schema: research; Owner: -
--

COMMENT ON COLUMN research.yu_candidate_zones.selected_for_review IS 'true代表該區域進入當日最多30區的Yu人工審核表。';


--
-- Name: futures_core_next_day_details futures_core_next_day_details_pkey; Type: CONSTRAINT; Schema: backtest; Owner: -
--

ALTER TABLE ONLY backtest.futures_core_next_day_details
    ADD CONSTRAINT futures_core_next_day_details_pkey PRIMARY KEY (run_id, signal_date);


--
-- Name: strategy_runs strategy_runs_pkey; Type: CONSTRAINT; Schema: backtest; Owner: -
--

ALTER TABLE ONLY backtest.strategy_runs
    ADD CONSTRAINT strategy_runs_pkey PRIMARY KEY (run_id);


--
-- Name: trade_executions trade_executions_pkey; Type: CONSTRAINT; Schema: backtest; Owner: -
--

ALTER TABLE ONLY backtest.trade_executions
    ADD CONSTRAINT trade_executions_pkey PRIMARY KEY (plan_id);


--
-- Name: trade_plans trade_plans_pkey; Type: CONSTRAINT; Schema: backtest; Owner: -
--

ALTER TABLE ONLY backtest.trade_plans
    ADD CONSTRAINT trade_plans_pkey PRIMARY KEY (plan_id);


--
-- Name: trade_plans uq_trade_plans_run_date_no; Type: CONSTRAINT; Schema: backtest; Owner: -
--

ALTER TABLE ONLY backtest.trade_plans
    ADD CONSTRAINT uq_trade_plans_run_date_no UNIQUE (run_id, trade_date, plan_no);


--
-- Name: data_source pk_data_source_1; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.data_source
    ADD CONSTRAINT pk_data_source_1 PRIMARY KEY (id);


--
-- Name: date_span pk_date_span_1; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.date_span
    ADD CONSTRAINT pk_date_span_1 PRIMARY KEY (id);


--
-- Name: futures_report_daily_raw pk_futures_report_daily_raw_1; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.futures_report_daily_raw
    ADD CONSTRAINT pk_futures_report_daily_raw_1 PRIMARY KEY (id);


--
-- Name: relation_punish_notice pk_relation_punish_notice_1; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.relation_punish_notice
    ADD CONSTRAINT pk_relation_punish_notice_1 PRIMARY KEY (id);


--
-- Name: stock_price_involve_report pk_stock_price_involve_report_1; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.stock_price_involve_report
    ADD CONSTRAINT pk_stock_price_involve_report_1 PRIMARY KEY (id);


--
-- Name: stock_report_daily pk_stock_report_daily_1; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.stock_report_daily
    ADD CONSTRAINT pk_stock_report_daily_1 PRIMARY KEY (id);


--
-- Name: report_cache_status report_cache_status_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.report_cache_status
    ADD CONSTRAINT report_cache_status_pkey PRIMARY KEY (cache_name);


--
-- Name: date_span uq_date_span_1; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.date_span
    ADD CONSTRAINT uq_date_span_1 UNIQUE (target_table, idx_key);


--
-- Name: futures_report_daily_raw uq_futures_report_daily_raw_1; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.futures_report_daily_raw
    ADD CONSTRAINT uq_futures_report_daily_raw_1 UNIQUE (futures_id, "日期");


--
-- Name: relation_punish_notice uq_relation_punish_notice_1; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.relation_punish_notice
    ADD CONSTRAINT uq_relation_punish_notice_1 UNIQUE (punish_id, notice_id, type);


--
-- Name: stock_report_daily uq_stock_report_daily_1; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.stock_report_daily
    ADD CONSTRAINT uq_stock_report_daily_1 UNIQUE ("股票代號", "日期");


--
-- Name: ml_model ml_model_model_key_key; Type: CONSTRAINT; Schema: ml; Owner: -
--

ALTER TABLE ONLY ml.ml_model
    ADD CONSTRAINT ml_model_model_key_key UNIQUE (model_key);


--
-- Name: ml_model ml_model_pkey; Type: CONSTRAINT; Schema: ml; Owner: -
--

ALTER TABLE ONLY ml.ml_model
    ADD CONSTRAINT ml_model_pkey PRIMARY KEY (model_id);


--
-- Name: ml_model_version ml_model_version_model_id_config_hash_key; Type: CONSTRAINT; Schema: ml; Owner: -
--

ALTER TABLE ONLY ml.ml_model_version
    ADD CONSTRAINT ml_model_version_model_id_config_hash_key UNIQUE (model_id, config_hash);


--
-- Name: ml_model_version ml_model_version_model_id_version_no_key; Type: CONSTRAINT; Schema: ml; Owner: -
--

ALTER TABLE ONLY ml.ml_model_version
    ADD CONSTRAINT ml_model_version_model_id_version_no_key UNIQUE (model_id, version_no);


--
-- Name: ml_model_version ml_model_version_pkey; Type: CONSTRAINT; Schema: ml; Owner: -
--

ALTER TABLE ONLY ml.ml_model_version
    ADD CONSTRAINT ml_model_version_pkey PRIMARY KEY (model_version_id);


--
-- Name: ml_prediction ml_prediction_pkey; Type: CONSTRAINT; Schema: ml; Owner: -
--

ALTER TABLE ONLY ml.ml_prediction
    ADD CONSTRAINT ml_prediction_pkey PRIMARY KEY (prediction_id);


--
-- Name: ml_prediction ml_prediction_run_id_target_date_key; Type: CONSTRAINT; Schema: ml; Owner: -
--

ALTER TABLE ONLY ml.ml_prediction
    ADD CONSTRAINT ml_prediction_run_id_target_date_key UNIQUE (run_id, target_date);


--
-- Name: ml_prediction_run ml_prediction_run_pkey; Type: CONSTRAINT; Schema: ml; Owner: -
--

ALTER TABLE ONLY ml.ml_prediction_run
    ADD CONSTRAINT ml_prediction_run_pkey PRIMARY KEY (run_id);


--
-- Name: tpex_bulletin_attention pk_tpex_bulletin_attention_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange.tpex_bulletin_attention
    ADD CONSTRAINT pk_tpex_bulletin_attention_1 PRIMARY KEY (id);


--
-- Name: tpex_bulletin_disposal pk_tpex_bulletin_disposal_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange.tpex_bulletin_disposal
    ADD CONSTRAINT pk_tpex_bulletin_disposal_1 PRIMARY KEY (id);


--
-- Name: twse_announcement_notice pk_twse_announcement_notice_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange.twse_announcement_notice
    ADD CONSTRAINT pk_twse_announcement_notice_1 PRIMARY KEY (id);


--
-- Name: twse_announcement_punish pk_twse_announcement_punish_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange.twse_announcement_punish
    ADD CONSTRAINT pk_twse_announcement_punish_1 PRIMARY KEY (id);


--
-- Name: twse_exchangeReport_fmtqik pk_twse_exchangeReport_fmtqik_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange."twse_exchangeReport_fmtqik"
    ADD CONSTRAINT "pk_twse_exchangeReport_fmtqik_1" PRIMARY KEY (id);


--
-- Name: twse_indicesReport_mi_5mins_hist pk_twse_indicesReport_mi_5mins_hist_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange."twse_indicesReport_mi_5mins_hist"
    ADD CONSTRAINT "pk_twse_indicesReport_mi_5mins_hist_1" PRIMARY KEY (id);


--
-- Name: twse_marginTrading_miMargn pk_twse_marginTrading_miMargn_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange."twse_marginTrading_miMargn"
    ADD CONSTRAINT "pk_twse_marginTrading_miMargn_1" PRIMARY KEY (id);


--
-- Name: taifex_futures_daily taifex_futures_daily_pkey; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange.taifex_futures_daily
    ADD CONSTRAINT taifex_futures_daily_pkey PRIMARY KEY (id);


--
-- Name: taifex_futures_daily uq_taifex_futures_daily; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange.taifex_futures_daily
    ADD CONSTRAINT uq_taifex_futures_daily UNIQUE (trade_date, futures_id, contract_month, trading_session);


--
-- Name: tpex_bulletin_attention uq_tpex_bulletin_attention_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange.tpex_bulletin_attention
    ADD CONSTRAINT uq_tpex_bulletin_attention_1 UNIQUE ("證券代號", "公告日期");


--
-- Name: tpex_bulletin_disposal uq_tpex_bulletin_disposal_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange.tpex_bulletin_disposal
    ADD CONSTRAINT uq_tpex_bulletin_disposal_1 UNIQUE ("證券代號", "公布日期");


--
-- Name: twse_announcement_notice uq_twse_announcement_notice_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange.twse_announcement_notice
    ADD CONSTRAINT uq_twse_announcement_notice_1 UNIQUE ("證券代號", "日期");


--
-- Name: twse_announcement_punish uq_twse_announcement_punish_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange.twse_announcement_punish
    ADD CONSTRAINT uq_twse_announcement_punish_1 UNIQUE ("證券代號", "公布日期");


--
-- Name: twse_exchangeReport_fmtqik uq_twse_exchangeReport_fmtqik_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange."twse_exchangeReport_fmtqik"
    ADD CONSTRAINT "uq_twse_exchangeReport_fmtqik_1" UNIQUE (date_ad);


--
-- Name: twse_indicesReport_mi_5mins_hist uq_twse_indicesReport_mi_5mins_hist_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange."twse_indicesReport_mi_5mins_hist"
    ADD CONSTRAINT "uq_twse_indicesReport_mi_5mins_hist_1" UNIQUE (date_ad);


--
-- Name: twse_marginTrading_miMargn uq_twse_marginTrading_miMargn_1; Type: CONSTRAINT; Schema: raw_exchange; Owner: -
--

ALTER TABLE ONLY raw_exchange."twse_marginTrading_miMargn"
    ADD CONSTRAINT "uq_twse_marginTrading_miMargn_1" UNIQUE ("日期", "項目");


--
-- Name: fm_taiwan_futures_kbar_1m fm_taiwan_futures_kbar_1m_pkey; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_futures_kbar_1m
    ADD CONSTRAINT fm_taiwan_futures_kbar_1m_pkey PRIMARY KEY (id);


--
-- Name: fm_taiwan_futures_tick fm_taiwan_futures_tick_pkey; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_futures_tick
    ADD CONSTRAINT fm_taiwan_futures_tick_pkey PRIMARY KEY (id);


--
-- Name: fm_taiwan_stock_kbar_1m fm_taiwan_stock_kbar_1m_pkey; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_stock_kbar_1m
    ADD CONSTRAINT fm_taiwan_stock_kbar_1m_pkey PRIMARY KEY (id);


--
-- Name: fm_taiwan_futures_daily pk_fm_taiwan_futures_daily_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_futures_daily
    ADD CONSTRAINT pk_fm_taiwan_futures_daily_1 PRIMARY KEY (id);


--
-- Name: fm_taiwan_futures_final_settlement_price pk_fm_taiwan_futures_final_settlement_price_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_futures_final_settlement_price
    ADD CONSTRAINT pk_fm_taiwan_futures_final_settlement_price_1 PRIMARY KEY (id);


--
-- Name: fm_taiwan_futures_institutional_investors pk_fm_taiwan_futures_institutional_investors_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_futures_institutional_investors
    ADD CONSTRAINT pk_fm_taiwan_futures_institutional_investors_1 PRIMARY KEY (id);


--
-- Name: fm_taiwan_futures_institutional_investors_after_hours pk_fm_taiwan_futures_institutional_investors_after_hours_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_futures_institutional_investors_after_hours
    ADD CONSTRAINT pk_fm_taiwan_futures_institutional_investors_after_hours_1 PRIMARY KEY (id);


--
-- Name: fm_taiwan_option_daily pk_fm_taiwan_option_daily_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_option_daily
    ADD CONSTRAINT pk_fm_taiwan_option_daily_1 PRIMARY KEY (id);


--
-- Name: fm_taiwan_option_final_settlement_price pk_fm_taiwan_option_final_settlement_price_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_option_final_settlement_price
    ADD CONSTRAINT pk_fm_taiwan_option_final_settlement_price_1 PRIMARY KEY (id);


--
-- Name: fm_taiwan_stock_daily pk_fm_taiwan_stock_daily_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_stock_daily
    ADD CONSTRAINT pk_fm_taiwan_stock_daily_1 PRIMARY KEY (id);


--
-- Name: fm_taiwan_stock_institutional_total pk_fm_taiwan_stock_institutional_total_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_stock_institutional_total
    ADD CONSTRAINT pk_fm_taiwan_stock_institutional_total_1 PRIMARY KEY (id);


--
-- Name: fm_taiwan_stock_margin_total pk_fm_taiwan_stock_margin_total_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_stock_margin_total
    ADD CONSTRAINT pk_fm_taiwan_stock_margin_total_1 PRIMARY KEY (id);


--
-- Name: fm_taiwan_stock_trading_date pk_fm_taiwan_stock_trading_date_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_stock_trading_date
    ADD CONSTRAINT pk_fm_taiwan_stock_trading_date_1 PRIMARY KEY (date);


--
-- Name: fm_taiwan_futures_daily uq_fm_taiwan_futures_daily_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_futures_daily
    ADD CONSTRAINT uq_fm_taiwan_futures_daily_1 UNIQUE (date, futures_id, contract_date, trading_session);


--
-- Name: fm_taiwan_futures_final_settlement_price uq_fm_taiwan_futures_final_settlement_price_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_futures_final_settlement_price
    ADD CONSTRAINT uq_fm_taiwan_futures_final_settlement_price_1 UNIQUE (date, contract_month, futures_id);


--
-- Name: fm_taiwan_futures_institutional_investors uq_fm_taiwan_futures_institutional_investors_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_futures_institutional_investors
    ADD CONSTRAINT uq_fm_taiwan_futures_institutional_investors_1 UNIQUE (date, data_id, institutional_investors);


--
-- Name: fm_taiwan_futures_institutional_investors_after_hours uq_fm_taiwan_futures_institutional_investors_after_hours_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_futures_institutional_investors_after_hours
    ADD CONSTRAINT uq_fm_taiwan_futures_institutional_investors_after_hours_1 UNIQUE (date, futures_id, institutional_investors);


--
-- Name: fm_taiwan_futures_kbar_1m uq_fm_taiwan_futures_kbar_1m; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_futures_kbar_1m
    ADD CONSTRAINT uq_fm_taiwan_futures_kbar_1m UNIQUE (futures_id, contract_date, bar_datetime);


--
-- Name: fm_taiwan_futures_tick uq_fm_taiwan_futures_tick_source_row; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_futures_tick
    ADD CONSTRAINT uq_fm_taiwan_futures_tick_source_row UNIQUE (futures_id, source_date, source_row_no);


--
-- Name: fm_taiwan_option_daily uq_fm_taiwan_option_daily_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_option_daily
    ADD CONSTRAINT uq_fm_taiwan_option_daily_1 UNIQUE (date, option_id, contract_date, strike_price, call_put, trading_session);


--
-- Name: fm_taiwan_option_final_settlement_price uq_fm_taiwan_option_final_settlement_price_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_option_final_settlement_price
    ADD CONSTRAINT uq_fm_taiwan_option_final_settlement_price_1 UNIQUE (date, contract_month, option_id);


--
-- Name: fm_taiwan_stock_daily uq_fm_taiwan_stock_daily_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_stock_daily
    ADD CONSTRAINT uq_fm_taiwan_stock_daily_1 UNIQUE (date, stock_id);


--
-- Name: fm_taiwan_stock_institutional_total uq_fm_taiwan_stock_institutional_total_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_stock_institutional_total
    ADD CONSTRAINT uq_fm_taiwan_stock_institutional_total_1 UNIQUE (date, name);


--
-- Name: fm_taiwan_stock_kbar_1m uq_fm_taiwan_stock_kbar_1m; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_stock_kbar_1m
    ADD CONSTRAINT uq_fm_taiwan_stock_kbar_1m UNIQUE (date, minute, stock_id);


--
-- Name: fm_taiwan_stock_margin_total uq_fm_taiwan_stock_margin_total_1; Type: CONSTRAINT; Schema: raw_finmind; Owner: -
--

ALTER TABLE ONLY raw_finmind.fm_taiwan_stock_margin_total
    ADD CONSTRAINT uq_fm_taiwan_stock_margin_total_1 UNIQUE (date, name);


--
-- Name: futures_turning_5m_bars futures_turning_5m_bars_pkey; Type: CONSTRAINT; Schema: report; Owner: -
--

ALTER TABLE ONLY report.futures_turning_5m_bars
    ADD CONSTRAINT futures_turning_5m_bars_pkey PRIMARY KEY (bar_time);


--
-- Name: futures_turning_levels futures_turning_levels_pkey; Type: CONSTRAINT; Schema: report; Owner: -
--

ALTER TABLE ONLY report.futures_turning_levels
    ADD CONSTRAINT futures_turning_levels_pkey PRIMARY KEY (futures_id, "日期");


--
-- Name: analysis_run analysis_run_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.analysis_run
    ADD CONSTRAINT analysis_run_pkey PRIMARY KEY (run_id);


--
-- Name: futures_night_bucket_daily futures_night_bucket_daily_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.futures_night_bucket_daily
    ADD CONSTRAINT futures_night_bucket_daily_pkey PRIMARY KEY (run_id, group_date);


--
-- Name: futures_night_bucket_summary futures_night_bucket_summary_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.futures_night_bucket_summary
    ADD CONSTRAINT futures_night_bucket_summary_pkey PRIMARY KEY (run_id, bucket_id, metric_name);


--
-- Name: futures_night_bucket_yearly futures_night_bucket_yearly_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.futures_night_bucket_yearly
    ADD CONSTRAINT futures_night_bucket_yearly_pkey PRIMARY KEY (run_id, result_year, bucket_id, metric_name);


--
-- Name: night_retrace_backtest_run night_retrace_backtest_run_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.night_retrace_backtest_run
    ADD CONSTRAINT night_retrace_backtest_run_pkey PRIMARY KEY (run_id);


--
-- Name: night_retrace_parameter_summary night_retrace_parameter_summary_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.night_retrace_parameter_summary
    ADD CONSTRAINT night_retrace_parameter_summary_pkey PRIMARY KEY (run_id, parameter_id, segment);


--
-- Name: night_retrace_trade_detail night_retrace_trade_detail_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.night_retrace_trade_detail
    ADD CONSTRAINT night_retrace_trade_detail_pkey PRIMARY KEY (run_id, parameter_id, group_date);


--
-- Name: night_retrace_yearly_summary night_retrace_yearly_summary_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.night_retrace_yearly_summary
    ADD CONSTRAINT night_retrace_yearly_summary_pkey PRIMARY KEY (run_id, parameter_id, result_year);


--
-- Name: market_daily_feature pk_market_daily_feature; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.market_daily_feature
    ADD CONSTRAINT pk_market_daily_feature PRIMARY KEY (market_type, instrument_id, session, trade_date, feature_name, feature_version);


--
-- Name: yu_candidate_missed_levels yu_candidate_missed_levels_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_missed_levels
    ADD CONSTRAINT yu_candidate_missed_levels_pkey PRIMARY KEY (missed_id);


--
-- Name: yu_candidate_raw_levels yu_candidate_raw_levels_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_raw_levels
    ADD CONSTRAINT yu_candidate_raw_levels_pkey PRIMARY KEY (run_id, raw_level_id);


--
-- Name: yu_candidate_reviews yu_candidate_reviews_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_reviews
    ADD CONSTRAINT yu_candidate_reviews_pkey PRIMARY KEY (run_id, zone_id);


--
-- Name: yu_candidate_runs yu_candidate_runs_generator_code_generator_version_as_of_da_key; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_runs
    ADD CONSTRAINT yu_candidate_runs_generator_code_generator_version_as_of_da_key UNIQUE (generator_code, generator_version, as_of_date);


--
-- Name: yu_candidate_runs yu_candidate_runs_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_runs
    ADD CONSTRAINT yu_candidate_runs_pkey PRIMARY KEY (run_id);


--
-- Name: yu_candidate_zone_sources yu_candidate_zone_sources_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_zone_sources
    ADD CONSTRAINT yu_candidate_zone_sources_pkey PRIMARY KEY (run_id, zone_id, raw_level_id);


--
-- Name: yu_candidate_zones yu_candidate_zones_pkey; Type: CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_zones
    ADD CONSTRAINT yu_candidate_zones_pkey PRIMARY KEY (run_id, zone_id);


--
-- Name: ix_backtest_strategy_runs_lookup; Type: INDEX; Schema: backtest; Owner: -
--

CREATE INDEX ix_backtest_strategy_runs_lookup ON backtest.strategy_runs USING btree (strategy_code, start_date, end_date, created_at DESC);


--
-- Name: ix_futures_core_next_day_details_evaluation; Type: INDEX; Schema: backtest; Owner: -
--

CREATE INDEX ix_futures_core_next_day_details_evaluation ON backtest.futures_core_next_day_details USING btree (evaluation_date);


--
-- Name: ix_trade_executions_result; Type: INDEX; Schema: backtest; Owner: -
--

CREATE INDEX ix_trade_executions_result ON backtest.trade_executions USING btree (is_confirmed, exit_reason, resolution_level);


--
-- Name: ix_trade_plans_market_date; Type: INDEX; Schema: backtest; Owner: -
--

CREATE INDEX ix_trade_plans_market_date ON backtest.trade_plans USING btree (futures_id, trade_date, trading_session, contract_date, planned_entry_at);


--
-- Name: ix_trade_plans_strategy_date; Type: INDEX; Schema: backtest; Owner: -
--

CREATE INDEX ix_trade_plans_strategy_date ON backtest.trade_plans USING btree (strategy_code, trade_date DESC);


--
-- Name: idx_data_source_name; Type: INDEX; Schema: core; Owner: -
--

CREATE UNIQUE INDEX idx_data_source_name ON core.data_source USING btree (name);


--
-- Name: ix_ml_prediction_run_version; Type: INDEX; Schema: ml; Owner: -
--

CREATE INDEX ix_ml_prediction_run_version ON ml.ml_prediction_run USING btree (model_version_id, started_at DESC);


--
-- Name: ix_ml_prediction_unrealized; Type: INDEX; Schema: ml; Owner: -
--

CREATE INDEX ix_ml_prediction_unrealized ON ml.ml_prediction USING btree (target_date) WHERE (actual_class IS NULL);


--
-- Name: ix_ml_prediction_version_date; Type: INDEX; Schema: ml; Owner: -
--

CREATE INDEX ix_ml_prediction_version_date ON ml.ml_prediction USING btree (model_version_id, target_date);


--
-- Name: idx_fmtqik_date_ad; Type: INDEX; Schema: raw_exchange; Owner: -
--

CREATE INDEX idx_fmtqik_date_ad ON raw_exchange."twse_exchangeReport_fmtqik" USING btree (date_ad);


--
-- Name: idx_fmtqik_date_ts; Type: INDEX; Schema: raw_exchange; Owner: -
--

CREATE INDEX idx_fmtqik_date_ts ON raw_exchange."twse_exchangeReport_fmtqik" USING btree (date_ts);


--
-- Name: idx_mi5_date_ad; Type: INDEX; Schema: raw_exchange; Owner: -
--

CREATE INDEX idx_mi5_date_ad ON raw_exchange."twse_indicesReport_mi_5mins_hist" USING btree (date_ad);


--
-- Name: idx_mi5_date_ts; Type: INDEX; Schema: raw_exchange; Owner: -
--

CREATE INDEX idx_mi5_date_ts ON raw_exchange."twse_indicesReport_mi_5mins_hist" USING btree (date_ts);


--
-- Name: idx_taifex_futures_daily_lookup; Type: INDEX; Schema: raw_exchange; Owner: -
--

CREATE INDEX idx_taifex_futures_daily_lookup ON raw_exchange.taifex_futures_daily USING btree (futures_id, trade_date DESC, trading_session, contract_month);


--
-- Name: idx_taifex_futures_daily_night; Type: INDEX; Schema: raw_exchange; Owner: -
--

CREATE INDEX idx_taifex_futures_daily_night ON raw_exchange.taifex_futures_daily USING btree (futures_id, trade_date DESC, contract_month) WHERE (trading_session = 'after_market'::text);


--
-- Name: idx_fm_fut_daily_main; Type: INDEX; Schema: raw_finmind; Owner: -
--

CREATE INDEX idx_fm_fut_daily_main ON raw_finmind.fm_taiwan_futures_daily USING btree (futures_id, date, contract_date, trading_session);


--
-- Name: idx_fm_futures_kbar_1m_period_query; Type: INDEX; Schema: raw_finmind; Owner: -
--

CREATE INDEX idx_fm_futures_kbar_1m_period_query ON raw_finmind.fm_taiwan_futures_kbar_1m USING btree (futures_id, trading_session, bar_datetime, contract_date) INCLUDE (open, high, low, close, volume, trading_date);


--
-- Name: idx_fm_futures_kbar_1m_source_date; Type: INDEX; Schema: raw_finmind; Owner: -
--

CREATE INDEX idx_fm_futures_kbar_1m_source_date ON raw_finmind.fm_taiwan_futures_kbar_1m USING btree (futures_id, source_date);


--
-- Name: idx_fm_futures_kbar_1m_trading_lookup; Type: INDEX; Schema: raw_finmind; Owner: -
--

CREATE INDEX idx_fm_futures_kbar_1m_trading_lookup ON raw_finmind.fm_taiwan_futures_kbar_1m USING btree (futures_id, trading_date, trading_session, contract_date, bar_datetime) INCLUDE (open, high, low, close, volume, tick_count);


--
-- Name: idx_fm_futures_tick_contract_time; Type: INDEX; Schema: raw_finmind; Owner: -
--

CREATE INDEX idx_fm_futures_tick_contract_time ON raw_finmind.fm_taiwan_futures_tick USING btree (futures_id, contract_date, trade_datetime, source_row_no) INCLUDE (price, volume, source_date);


--
-- Name: idx_fm_futures_tick_source_date; Type: INDEX; Schema: raw_finmind; Owner: -
--

CREATE INDEX idx_fm_futures_tick_source_date ON raw_finmind.fm_taiwan_futures_tick USING btree (futures_id, source_date);


--
-- Name: idx_fm_stock_kbar_1m_lookup; Type: INDEX; Schema: raw_finmind; Owner: -
--

CREATE INDEX idx_fm_stock_kbar_1m_lookup ON raw_finmind.fm_taiwan_stock_kbar_1m USING btree (stock_id, date, minute);


--
-- Name: idx_fm_stock_kbar_1m_period_query; Type: INDEX; Schema: raw_finmind; Owner: -
--

CREATE INDEX idx_fm_stock_kbar_1m_period_query ON raw_finmind.fm_taiwan_stock_kbar_1m USING btree (stock_id, date, minute) INCLUDE (open, high, low, close, volume);


--
-- Name: idx_futures_dashboard_cache_date; Type: INDEX; Schema: report; Owner: -
--

CREATE INDEX idx_futures_dashboard_cache_date ON report.futures_report_daily_dashboard_cache USING btree ("日期" DESC);


--
-- Name: idx_mv_futures_report_daily_base_date; Type: INDEX; Schema: report; Owner: -
--

CREATE INDEX idx_mv_futures_report_daily_base_date ON report.mv_futures_report_daily_base USING btree ("日期" DESC);


--
-- Name: uq_futures_dashboard_cache_id; Type: INDEX; Schema: report; Owner: -
--

CREATE UNIQUE INDEX uq_futures_dashboard_cache_id ON report.futures_report_daily_dashboard_cache USING btree (id);


--
-- Name: uq_futures_dashboard_cache_market_date; Type: INDEX; Schema: report; Owner: -
--

CREATE UNIQUE INDEX uq_futures_dashboard_cache_market_date ON report.futures_report_daily_dashboard_cache USING btree (futures_id, "日期");


--
-- Name: uq_futures_report_daily_tail_cache_id; Type: INDEX; Schema: report; Owner: -
--

CREATE UNIQUE INDEX uq_futures_report_daily_tail_cache_id ON report.futures_report_daily_tail_cache USING btree (id);


--
-- Name: uq_futures_report_daily_tail_cache_market_date; Type: INDEX; Schema: report; Owner: -
--

CREATE UNIQUE INDEX uq_futures_report_daily_tail_cache_market_date ON report.futures_report_daily_tail_cache USING btree (futures_id, "日期");


--
-- Name: uq_mv_futures_report_daily_base_id; Type: INDEX; Schema: report; Owner: -
--

CREATE UNIQUE INDEX uq_mv_futures_report_daily_base_id ON report.mv_futures_report_daily_base USING btree (id);


--
-- Name: uq_mv_futures_report_daily_base_market_date; Type: INDEX; Schema: report; Owner: -
--

CREATE UNIQUE INDEX uq_mv_futures_report_daily_base_market_date ON report.mv_futures_report_daily_base USING btree (futures_id, "日期");


--
-- Name: idx_night_retrace_parameter_rank; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX idx_night_retrace_parameter_rank ON research.night_retrace_parameter_summary USING btree (run_id, segment, total_net_points DESC);


--
-- Name: idx_night_retrace_run_lookup; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX idx_night_retrace_run_lookup ON research.night_retrace_backtest_run USING btree (strategy_name, status, finished_at DESC);


--
-- Name: idx_night_retrace_trade_lookup; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX idx_night_retrace_trade_lookup ON research.night_retrace_trade_detail USING btree (run_id, parameter_id, trade_date);


--
-- Name: idx_research_analysis_run_lookup; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX idx_research_analysis_run_lookup ON research.analysis_run USING btree (analysis_name, status, finished_at DESC);


--
-- Name: idx_research_night_bucket_daily_filter; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX idx_research_night_bucket_daily_filter ON research.futures_night_bucket_daily USING btree (run_id, bucket_id, result_date);


--
-- Name: idx_research_night_bucket_daily_year; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX idx_research_night_bucket_daily_year ON research.futures_night_bucket_daily USING btree (run_id, EXTRACT(year FROM result_date), bucket_id);


--
-- Name: idx_research_night_yearly_lookup; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX idx_research_night_yearly_lookup ON research.futures_night_bucket_yearly USING btree (run_id, metric_name, result_year, bucket_id);


--
-- Name: ix_market_daily_feature_lookup; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX ix_market_daily_feature_lookup ON research.market_daily_feature USING btree (feature_name, feature_version, market_type, instrument_id, session, trade_date);


--
-- Name: ix_yu_candidate_raw_source; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX ix_yu_candidate_raw_source ON research.yu_candidate_raw_levels USING btree (market, source_date DESC, source_rule);


--
-- Name: ix_yu_candidate_reviews_action; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX ix_yu_candidate_reviews_action ON research.yu_candidate_reviews USING btree (review_action, is_key_level);


--
-- Name: ix_yu_candidate_runs_asof; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX ix_yu_candidate_runs_asof ON research.yu_candidate_runs USING btree (as_of_date DESC);


--
-- Name: ix_yu_candidate_zone_level; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX ix_yu_candidate_zone_level ON research.yu_candidate_zones USING btree (market, representative_level);


--
-- Name: ix_yu_candidate_zone_review; Type: INDEX; Schema: research; Owner: -
--

CREATE INDEX ix_yu_candidate_zone_review ON research.yu_candidate_zones USING btree (run_id, selected_for_review, review_score DESC);


--
-- Name: futures_core_next_day_details fk_futures_core_next_day_run; Type: FK CONSTRAINT; Schema: backtest; Owner: -
--

ALTER TABLE ONLY backtest.futures_core_next_day_details
    ADD CONSTRAINT fk_futures_core_next_day_run FOREIGN KEY (run_id) REFERENCES backtest.strategy_runs(run_id) ON DELETE CASCADE;


--
-- Name: trade_executions fk_trade_executions_plan; Type: FK CONSTRAINT; Schema: backtest; Owner: -
--

ALTER TABLE ONLY backtest.trade_executions
    ADD CONSTRAINT fk_trade_executions_plan FOREIGN KEY (plan_id) REFERENCES backtest.trade_plans(plan_id) ON DELETE CASCADE;


--
-- Name: trade_plans fk_trade_plans_run; Type: FK CONSTRAINT; Schema: backtest; Owner: -
--

ALTER TABLE ONLY backtest.trade_plans
    ADD CONSTRAINT fk_trade_plans_run FOREIGN KEY (run_id) REFERENCES backtest.strategy_runs(run_id) ON DELETE CASCADE;


--
-- Name: ml_model_version ml_model_version_model_id_fkey; Type: FK CONSTRAINT; Schema: ml; Owner: -
--

ALTER TABLE ONLY ml.ml_model_version
    ADD CONSTRAINT ml_model_version_model_id_fkey FOREIGN KEY (model_id) REFERENCES ml.ml_model(model_id) ON DELETE CASCADE;


--
-- Name: ml_prediction ml_prediction_model_version_id_fkey; Type: FK CONSTRAINT; Schema: ml; Owner: -
--

ALTER TABLE ONLY ml.ml_prediction
    ADD CONSTRAINT ml_prediction_model_version_id_fkey FOREIGN KEY (model_version_id) REFERENCES ml.ml_model_version(model_version_id) ON DELETE CASCADE;


--
-- Name: ml_prediction ml_prediction_run_id_fkey; Type: FK CONSTRAINT; Schema: ml; Owner: -
--

ALTER TABLE ONLY ml.ml_prediction
    ADD CONSTRAINT ml_prediction_run_id_fkey FOREIGN KEY (run_id) REFERENCES ml.ml_prediction_run(run_id) ON DELETE CASCADE;


--
-- Name: ml_prediction_run ml_prediction_run_model_version_id_fkey; Type: FK CONSTRAINT; Schema: ml; Owner: -
--

ALTER TABLE ONLY ml.ml_prediction_run
    ADD CONSTRAINT ml_prediction_run_model_version_id_fkey FOREIGN KEY (model_version_id) REFERENCES ml.ml_model_version(model_version_id) ON DELETE CASCADE;


--
-- Name: futures_night_bucket_daily futures_night_bucket_daily_run_id_fkey; Type: FK CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.futures_night_bucket_daily
    ADD CONSTRAINT futures_night_bucket_daily_run_id_fkey FOREIGN KEY (run_id) REFERENCES research.analysis_run(run_id) ON DELETE CASCADE;


--
-- Name: futures_night_bucket_summary futures_night_bucket_summary_run_id_fkey; Type: FK CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.futures_night_bucket_summary
    ADD CONSTRAINT futures_night_bucket_summary_run_id_fkey FOREIGN KEY (run_id) REFERENCES research.analysis_run(run_id) ON DELETE CASCADE;


--
-- Name: futures_night_bucket_yearly futures_night_bucket_yearly_run_id_fkey; Type: FK CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.futures_night_bucket_yearly
    ADD CONSTRAINT futures_night_bucket_yearly_run_id_fkey FOREIGN KEY (run_id) REFERENCES research.analysis_run(run_id) ON DELETE CASCADE;


--
-- Name: night_retrace_parameter_summary night_retrace_parameter_summary_run_id_fkey; Type: FK CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.night_retrace_parameter_summary
    ADD CONSTRAINT night_retrace_parameter_summary_run_id_fkey FOREIGN KEY (run_id) REFERENCES research.night_retrace_backtest_run(run_id) ON DELETE CASCADE;


--
-- Name: night_retrace_trade_detail night_retrace_trade_detail_run_id_fkey; Type: FK CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.night_retrace_trade_detail
    ADD CONSTRAINT night_retrace_trade_detail_run_id_fkey FOREIGN KEY (run_id) REFERENCES research.night_retrace_backtest_run(run_id) ON DELETE CASCADE;


--
-- Name: night_retrace_yearly_summary night_retrace_yearly_summary_run_id_fkey; Type: FK CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.night_retrace_yearly_summary
    ADD CONSTRAINT night_retrace_yearly_summary_run_id_fkey FOREIGN KEY (run_id) REFERENCES research.night_retrace_backtest_run(run_id) ON DELETE CASCADE;


--
-- Name: yu_candidate_missed_levels yu_candidate_missed_levels_run_id_fkey; Type: FK CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_missed_levels
    ADD CONSTRAINT yu_candidate_missed_levels_run_id_fkey FOREIGN KEY (run_id) REFERENCES research.yu_candidate_runs(run_id) ON DELETE CASCADE;


--
-- Name: yu_candidate_raw_levels yu_candidate_raw_levels_run_id_fkey; Type: FK CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_raw_levels
    ADD CONSTRAINT yu_candidate_raw_levels_run_id_fkey FOREIGN KEY (run_id) REFERENCES research.yu_candidate_runs(run_id) ON DELETE CASCADE;


--
-- Name: yu_candidate_reviews yu_candidate_reviews_run_id_zone_id_fkey; Type: FK CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_reviews
    ADD CONSTRAINT yu_candidate_reviews_run_id_zone_id_fkey FOREIGN KEY (run_id, zone_id) REFERENCES research.yu_candidate_zones(run_id, zone_id) ON DELETE CASCADE;


--
-- Name: yu_candidate_zone_sources yu_candidate_zone_sources_run_id_raw_level_id_fkey; Type: FK CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_zone_sources
    ADD CONSTRAINT yu_candidate_zone_sources_run_id_raw_level_id_fkey FOREIGN KEY (run_id, raw_level_id) REFERENCES research.yu_candidate_raw_levels(run_id, raw_level_id) ON DELETE CASCADE;


--
-- Name: yu_candidate_zone_sources yu_candidate_zone_sources_run_id_zone_id_fkey; Type: FK CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_zone_sources
    ADD CONSTRAINT yu_candidate_zone_sources_run_id_zone_id_fkey FOREIGN KEY (run_id, zone_id) REFERENCES research.yu_candidate_zones(run_id, zone_id) ON DELETE CASCADE;


--
-- Name: yu_candidate_zones yu_candidate_zones_run_id_fkey; Type: FK CONSTRAINT; Schema: research; Owner: -
--

ALTER TABLE ONLY research.yu_candidate_zones
    ADD CONSTRAINT yu_candidate_zones_run_id_fkey FOREIGN KEY (run_id) REFERENCES research.yu_candidate_runs(run_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict edyDRaZ5f6ChCtkS94jtKNHqmD1jz9zKHx7XrG9c5KIVJdbWld1zuqTBQdZqUKi

