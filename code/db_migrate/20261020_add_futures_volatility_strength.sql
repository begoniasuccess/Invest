-- 隔日台指期日盤波動強度基準回測入庫。
ROLLBACK;
BEGIN;

DO $check$
BEGIN
    IF to_regclass('research.analysis_run') IS NULL THEN
        RAISE EXCEPTION '缺少 research.analysis_run，請先建立既有 research schema';
    END IF;
END $check$;

CREATE TABLE IF NOT EXISTS research.futures_volatility_strength_daily (
    run_id uuid NOT NULL REFERENCES research.analysis_run(run_id) ON DELETE CASCADE,
    signal_date date NOT NULL,
    result_date date NOT NULL,
    bollinger_position_raw double precision,
    bollinger_position_percentile_pct double precision,
    retail_ratio_raw double precision,
    retail_ratio_percentile_pct double precision,
    ma10_slope_raw double precision,
    ma10_slope_percentile_pct double precision,
    main_net_oi_raw double precision,
    main_net_oi_percentile_pct double precision,
    bollinger_low_flag smallint,
    retail_high_flag smallint,
    ma10_low_flag smallint,
    main_net_oi_low_flag smallint,
    available_condition_count smallint NOT NULL,
    volatility_score smallint,
    volatility_level text,
    next_open_close_return_pct double precision,
    next_up_mfe_pct double precision,
    next_down_mfe_pct double precision,
    next_intraday_range_pct double precision,
    next_close_location_pct double precision,
    next_open_gap_day_close_pct double precision,
    next_open_gap_night_close_pct double precision,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, signal_date),
    CONSTRAINT ck_volatility_strength_dates CHECK (signal_date < result_date),
    CONSTRAINT ck_volatility_strength_available CHECK (available_condition_count BETWEEN 0 AND 4),
    CONSTRAINT ck_volatility_strength_score CHECK (volatility_score IS NULL OR volatility_score BETWEEN 0 AND 4),
    CONSTRAINT ck_volatility_strength_level CHECK (
        (volatility_score IS NULL AND volatility_level IS NULL)
        OR (volatility_score IS NOT NULL AND volatility_level IN ('低波動', '中波動', '高波動'))
    )
);

CREATE TABLE IF NOT EXISTS research.futures_volatility_strength_summary (
    run_id uuid NOT NULL REFERENCES research.analysis_run(run_id) ON DELETE CASCADE,
    period_name text NOT NULL,
    group_type text NOT NULL,
    group_label text NOT NULL,
    sample_count integer NOT NULL,
    range_mean_pct double precision,
    range_median_pct double precision,
    range_std_pct double precision,
    up_mfe_mean_pct double precision,
    down_mfe_mean_pct double precision,
    open_close_mean_pct double precision,
    up_probability_pct double precision,
    close_location_mean_pct double precision,
    range_ge_100_probability_pct double precision,
    range_ge_150_probability_pct double precision,
    range_ge_200_probability_pct double precision,
    up_ge_050_probability_pct double precision,
    down_ge_050_probability_pct double precision,
    up_ge_075_probability_pct double precision,
    down_ge_075_probability_pct double precision,
    up_ge_100_probability_pct double precision,
    down_ge_100_probability_pct double precision,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, period_name, group_type, group_label),
    CONSTRAINT ck_volatility_summary_period CHECK (period_name IN ('全期', '探索期', '驗證期')),
    CONSTRAINT ck_volatility_summary_group_type CHECK (group_type IN ('分數', '等級')),
    CONSTRAINT ck_volatility_summary_count CHECK (sample_count >= 0)
);

CREATE TABLE IF NOT EXISTS research.futures_volatility_strength_yearly (
    run_id uuid NOT NULL REFERENCES research.analysis_run(run_id) ON DELETE CASCADE,
    result_year integer NOT NULL,
    volatility_level text NOT NULL,
    sample_count integer NOT NULL,
    range_mean_pct double precision,
    range_median_pct double precision,
    range_std_pct double precision,
    up_mfe_mean_pct double precision,
    down_mfe_mean_pct double precision,
    open_close_mean_pct double precision,
    up_probability_pct double precision,
    close_location_mean_pct double precision,
    range_ge_100_probability_pct double precision,
    range_ge_150_probability_pct double precision,
    range_ge_200_probability_pct double precision,
    up_ge_050_probability_pct double precision,
    down_ge_050_probability_pct double precision,
    up_ge_075_probability_pct double precision,
    down_ge_075_probability_pct double precision,
    up_ge_100_probability_pct double precision,
    down_ge_100_probability_pct double precision,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, result_year, volatility_level),
    CONSTRAINT ck_volatility_year CHECK (result_year BETWEEN 2000 AND 2200),
    CONSTRAINT ck_volatility_year_level CHECK (volatility_level IN ('低波動', '中波動', '高波動')),
    CONSTRAINT ck_volatility_year_count CHECK (sample_count >= 0)
);

CREATE TABLE IF NOT EXISTS research.futures_volatility_strength_test (
    run_id uuid NOT NULL REFERENCES research.analysis_run(run_id) ON DELETE CASCADE,
    period_name text NOT NULL,
    test_type text NOT NULL,
    sample_count integer NOT NULL,
    low_count integer,
    high_count integer,
    statistic double precision,
    effect_value double precision,
    effect_direction text NOT NULL,
    effect_size double precision,
    p_value double precision,
    low_mean_pct double precision,
    high_mean_pct double precision,
    fdr_q_value double precision,
    fdr_significant boolean NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, period_name, test_type),
    CONSTRAINT ck_volatility_test_period CHECK (period_name IN ('全期', '探索期', '驗證期')),
    CONSTRAINT ck_volatility_test_count CHECK (sample_count >= 0)
);

CREATE INDEX IF NOT EXISTS ix_volatility_strength_daily_date
    ON research.futures_volatility_strength_daily (signal_date, volatility_level);
CREATE INDEX IF NOT EXISTS ix_volatility_strength_yearly_lookup
    ON research.futures_volatility_strength_yearly (result_year, volatility_level);

COMMENT ON TABLE research.futures_volatility_strength_daily IS
    '四個代表性盤前訊號形成的0至4分波動強度，以及隔日日盤實際結果。';
COMMENT ON TABLE research.futures_volatility_strength_summary IS
    '波動分數及低中高等級在全期、探索期與驗證期的統計。';
COMMENT ON TABLE research.futures_volatility_strength_yearly IS
    '低中高波動等級的逐年穩定性統計。';
COMMENT ON TABLE research.futures_volatility_strength_test IS
    '波動分數趨勢及高低波動組差異檢定。';

COMMIT;

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'research'
  AND table_name LIKE 'futures_volatility_strength_%'
ORDER BY table_name;
