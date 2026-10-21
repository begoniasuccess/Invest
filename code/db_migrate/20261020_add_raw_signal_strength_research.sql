-- 原始訊號強度研究結果入庫。
-- 可重複執行；既有資料不會被覆蓋，每次回測以 analysis_run.run_id 分版保存。
ROLLBACK;
BEGIN;

DO $check$
BEGIN
    IF to_regclass('research.analysis_run') IS NULL THEN
        RAISE EXCEPTION '缺少 research.analysis_run，請先建立既有 research schema';
    END IF;
END $check$;

CREATE TABLE IF NOT EXISTS research.raw_signal_strength_daily (
    run_id uuid NOT NULL REFERENCES research.analysis_run(run_id) ON DELETE CASCADE,
    signal_date date NOT NULL,
    result_date date NOT NULL,
    signal_category text NOT NULL,
    signal_code text NOT NULL,
    formula text NOT NULL,
    natural_direction boolean NOT NULL,
    raw_value double precision,
    history_percentile_pct double precision,
    bucket_id smallint,
    bucket_label text,
    raw_sign text NOT NULL,
    next_open_close_return_pct double precision,
    next_up_mfe_pct double precision,
    next_down_mfe_pct double precision,
    next_intraday_range_pct double precision,
    next_close_location_pct double precision,
    next_open_gap_day_close_pct double precision,
    next_open_gap_night_close_pct double precision,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, signal_date, signal_code),
    CONSTRAINT ck_raw_signal_daily_dates CHECK (signal_date < result_date),
    CONSTRAINT ck_raw_signal_daily_percentile CHECK (
        history_percentile_pct IS NULL OR history_percentile_pct BETWEEN 0 AND 100
    ),
    CONSTRAINT ck_raw_signal_daily_bucket CHECK (
        (bucket_id IS NULL AND bucket_label IS NULL)
        OR (bucket_id BETWEEN 1 AND 5 AND bucket_label IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS research.raw_signal_strength_bucket (
    run_id uuid NOT NULL REFERENCES research.analysis_run(run_id) ON DELETE CASCADE,
    signal_category text NOT NULL,
    signal_code text NOT NULL,
    bucket_id smallint NOT NULL,
    bucket_label text NOT NULL,
    sample_count integer NOT NULL,
    raw_mean double precision,
    raw_median double precision,
    history_percentile_mean_pct double precision,
    next_open_close_mean_pct double precision,
    next_open_close_median_pct double precision,
    next_up_probability_pct double precision,
    next_up_mfe_mean_pct double precision,
    next_down_mfe_mean_pct double precision,
    up_mfe_greater_probability_pct double precision,
    next_intraday_range_mean_pct double precision,
    next_close_location_mean_pct double precision,
    up_ge_025_probability_pct double precision,
    down_ge_025_probability_pct double precision,
    up_ge_050_probability_pct double precision,
    down_ge_050_probability_pct double precision,
    up_ge_075_probability_pct double precision,
    down_ge_075_probability_pct double precision,
    up_ge_100_probability_pct double precision,
    down_ge_100_probability_pct double precision,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, signal_code, bucket_id),
    CONSTRAINT ck_raw_signal_bucket_id CHECK (bucket_id BETWEEN 1 AND 5),
    CONSTRAINT ck_raw_signal_bucket_count CHECK (sample_count >= 0)
);

CREATE TABLE IF NOT EXISTS research.raw_signal_strength_test (
    run_id uuid NOT NULL REFERENCES research.analysis_run(run_id) ON DELETE CASCADE,
    signal_category text NOT NULL,
    signal_code text NOT NULL,
    outcome_name text NOT NULL,
    test_type text NOT NULL,
    sample_count integer NOT NULL,
    low_bucket_count integer,
    high_bucket_count integer,
    statistic double precision,
    effect_value double precision,
    effect_direction text NOT NULL,
    effect_size double precision,
    p_value double precision,
    low_bucket_mean double precision,
    high_bucket_mean double precision,
    fdr_q_value double precision,
    fdr_significant boolean NOT NULL,
    discovery_effect_value double precision,
    discovery_p_value double precision,
    discovery_q_value double precision,
    validation_effect_value double precision,
    validation_p_value double precision,
    validation_q_value double precision,
    same_direction boolean NOT NULL,
    comparable_year_count integer NOT NULL,
    same_direction_year_count integer NOT NULL,
    yearly_direction_consistency double precision,
    repeated_validation_pass boolean NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, signal_code, outcome_name, test_type),
    CONSTRAINT ck_raw_signal_test_count CHECK (sample_count >= 0),
    CONSTRAINT ck_raw_signal_test_year_consistency CHECK (
        yearly_direction_consistency IS NULL
        OR yearly_direction_consistency BETWEEN 0 AND 1
    )
);

CREATE TABLE IF NOT EXISTS research.raw_signal_strength_yearly (
    run_id uuid NOT NULL REFERENCES research.analysis_run(run_id) ON DELETE CASCADE,
    result_year integer NOT NULL,
    signal_category text NOT NULL,
    signal_code text NOT NULL,
    bucket_id smallint NOT NULL,
    bucket_label text NOT NULL,
    sample_count integer NOT NULL,
    raw_mean double precision,
    raw_median double precision,
    history_percentile_mean_pct double precision,
    next_open_close_mean_pct double precision,
    next_open_close_median_pct double precision,
    next_up_probability_pct double precision,
    next_up_mfe_mean_pct double precision,
    next_down_mfe_mean_pct double precision,
    up_mfe_greater_probability_pct double precision,
    next_intraday_range_mean_pct double precision,
    next_close_location_mean_pct double precision,
    up_ge_025_probability_pct double precision,
    down_ge_025_probability_pct double precision,
    up_ge_050_probability_pct double precision,
    down_ge_050_probability_pct double precision,
    up_ge_075_probability_pct double precision,
    down_ge_075_probability_pct double precision,
    up_ge_100_probability_pct double precision,
    down_ge_100_probability_pct double precision,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, result_year, signal_code, bucket_id),
    CONSTRAINT ck_raw_signal_year CHECK (result_year BETWEEN 2000 AND 2200),
    CONSTRAINT ck_raw_signal_yearly_bucket CHECK (bucket_id BETWEEN 1 AND 5),
    CONSTRAINT ck_raw_signal_yearly_count CHECK (sample_count >= 0)
);

CREATE INDEX IF NOT EXISTS ix_raw_signal_daily_date
    ON research.raw_signal_strength_daily (signal_date, signal_code);
CREATE INDEX IF NOT EXISTS ix_raw_signal_test_candidate
    ON research.raw_signal_strength_test (repeated_validation_pass, outcome_name);
CREATE INDEX IF NOT EXISTS ix_raw_signal_yearly_lookup
    ON research.raw_signal_strength_yearly (signal_code, result_year, bucket_id);

COMMENT ON TABLE research.raw_signal_strength_daily IS
    '原始連續訊號的逐日值、僅使用過去資料的歷史分位，以及隔日台指期日盤結果。';
COMMENT ON TABLE research.raw_signal_strength_bucket IS
    '每次原始訊號強度研究的全期間五分位統計。';
COMMENT ON TABLE research.raw_signal_strength_test IS
    '每次原始訊號強度研究的統計檢定、探索驗證結果與年度穩定性。';
COMMENT ON TABLE research.raw_signal_strength_yearly IS
    '每次原始訊號強度研究的逐年五分位統計。';

COMMIT;

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'research'
  AND table_name LIKE 'raw_signal_strength_%'
ORDER BY table_name;
