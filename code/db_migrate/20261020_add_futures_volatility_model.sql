-- 波動強度樣本外模型比較結果入庫。
ROLLBACK;
BEGIN;

DO $check$
BEGIN
    IF to_regclass('research.analysis_run') IS NULL THEN
        RAISE EXCEPTION '缺少 research.analysis_run，請先建立既有 research schema';
    END IF;
END $check$;

CREATE TABLE IF NOT EXISTS research.futures_volatility_model_prediction (
    run_id uuid NOT NULL REFERENCES research.analysis_run(run_id) ON DELETE CASCADE,
    signal_date date NOT NULL,
    result_date date NOT NULL,
    volatility_score smallint NOT NULL,
    actual_range_pct double precision NOT NULL,
    actual_range_ge_100 smallint NOT NULL,
    actual_range_ge_150 smallint NOT NULL,
    test_year integer NOT NULL,
    score_predicted_range_pct double precision NOT NULL,
    ridge_predicted_range_pct double precision NOT NULL,
    score_range_ge_100_probability double precision NOT NULL,
    logistic_range_ge_100_probability double precision NOT NULL,
    score_range_ge_150_probability double precision NOT NULL,
    logistic_range_ge_150_probability double precision NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, signal_date),
    CONSTRAINT ck_volatility_model_dates CHECK (signal_date < result_date),
    CONSTRAINT ck_volatility_model_score CHECK (volatility_score BETWEEN 0 AND 4),
    CONSTRAINT ck_volatility_model_actual_100 CHECK (actual_range_ge_100 IN (0, 1)),
    CONSTRAINT ck_volatility_model_actual_150 CHECK (actual_range_ge_150 IN (0, 1)),
    CONSTRAINT ck_volatility_model_test_year CHECK (test_year BETWEEN 2000 AND 2200),
    CONSTRAINT ck_volatility_model_score_prob_100 CHECK (score_range_ge_100_probability BETWEEN 0 AND 1),
    CONSTRAINT ck_volatility_model_logistic_prob_100 CHECK (logistic_range_ge_100_probability BETWEEN 0 AND 1),
    CONSTRAINT ck_volatility_model_score_prob_150 CHECK (score_range_ge_150_probability BETWEEN 0 AND 1),
    CONSTRAINT ck_volatility_model_logistic_prob_150 CHECK (logistic_range_ge_150_probability BETWEEN 0 AND 1)
);

CREATE TABLE IF NOT EXISTS research.futures_volatility_model_metric (
    run_id uuid NOT NULL REFERENCES research.analysis_run(run_id) ON DELETE CASCADE,
    evaluation_period text NOT NULL,
    target_name text NOT NULL,
    model_name text NOT NULL,
    sample_count integer NOT NULL,
    mae double precision,
    rmse double precision,
    spearman double precision,
    brier double precision,
    roc_auc double precision,
    log_loss double precision,
    top20_actual double precision,
    top20_lift double precision,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, evaluation_period, target_name, model_name),
    CONSTRAINT ck_volatility_model_metric_count CHECK (sample_count >= 0),
    CONSTRAINT ck_volatility_model_metric_name CHECK (model_name IN ('簡單分數', 'Ridge', 'Logistic')),
    CONSTRAINT ck_volatility_model_auc CHECK (roc_auc IS NULL OR roc_auc BETWEEN 0 AND 1)
);

CREATE TABLE IF NOT EXISTS research.futures_volatility_model_coefficient (
    run_id uuid NOT NULL REFERENCES research.analysis_run(run_id) ON DELETE CASCADE,
    test_year integer NOT NULL,
    target_name text NOT NULL,
    model_name text NOT NULL,
    feature_name text NOT NULL,
    standardized_coefficient double precision NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, test_year, target_name, model_name, feature_name),
    CONSTRAINT ck_volatility_model_coefficient_year CHECK (test_year BETWEEN 2000 AND 2200),
    CONSTRAINT ck_volatility_model_coefficient_name CHECK (model_name IN ('Ridge', 'Logistic'))
);

CREATE INDEX IF NOT EXISTS ix_volatility_model_prediction_date
    ON research.futures_volatility_model_prediction (signal_date, test_year);
CREATE INDEX IF NOT EXISTS ix_volatility_model_metric_lookup
    ON research.futures_volatility_model_metric (evaluation_period, target_name, model_name);

COMMENT ON TABLE research.futures_volatility_model_prediction IS
    '每年只用過往年度訓練後產生的隔日日盤波動樣本外預測。';
COMMENT ON TABLE research.futures_volatility_model_metric IS
    '簡單波動分數與正則化 Ridge、Logistic 模型的樣本外成效比較。';
COMMENT ON TABLE research.futures_volatility_model_coefficient IS
    '各樣本外年度模型的標準化特徵係數，用於檢查訊號方向及穩定性。';

COMMIT;

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'research'
  AND table_name LIKE 'futures_volatility_model_%'
ORDER BY table_name;
