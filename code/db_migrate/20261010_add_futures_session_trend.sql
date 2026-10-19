-- 若前一次執行已進入「current transaction is aborted」，先解除該狀態。
ROLLBACK;
BEGIN;

DO $trend$
DECLARE
    fields text;
    additions text :=
        'CASE WHEN b."日盤高" IS NULL OR b."日盤低" IS NULL OR b."日盤收" IS NULL THEN NULL '
        'WHEN abs(b."日盤高"-b."日盤收") < abs(b."日盤收"-b."日盤低") THEN ''漲勢盤'' '
        'ELSE ''跌勢盤'' END AS "日盤走勢", '
        't."日盤轉折支撐", t."日盤轉折壓力", '
        'CASE WHEN b."夜盤高" IS NULL OR b."夜盤低" IS NULL OR b."夜盤收" IS NULL THEN NULL '
        'WHEN abs(b."夜盤高"-b."夜盤收") < abs(b."夜盤收"-b."夜盤低") THEN ''漲勢盤'' '
        'ELSE ''跌勢盤'' END AS "夜盤走勢", '
        't."夜盤轉折支撐", t."夜盤轉折壓力"';
BEGIN
    PERFORM pg_advisory_xact_lock(26100451);

    IF to_regclass('report.v_futures_report_daily_without_turning') IS NULL THEN
        RAISE EXCEPTION '缺少 report.v_futures_report_daily_without_turning，請先完成期貨轉折欄位安裝';
    END IF;

    -- CREATE OR REPLACE VIEW 無法在既有欄位中間插入新欄位。
    -- 將舊 View 改名保留而不刪除，依賴舊 OID 的物件可繼續運作；原名稱則建立新版。
    IF to_regclass('report.v_futures_report_daily') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM pg_attribute
           WHERE attrelid='report.v_futures_report_daily'::regclass
             AND attname='日盤走勢' AND attnum>0 AND NOT attisdropped
       ) THEN
        IF to_regclass('report.v_futures_report_daily_without_session_trend') IS NOT NULL THEN
            RAISE EXCEPTION 'report.v_futures_report_daily_without_session_trend 已存在，請先確認先前遷移狀態';
        END IF;
        ALTER VIEW report.v_futures_report_daily
            RENAME TO v_futures_report_daily_without_session_trend;
    END IF;

    IF to_regclass('report.v_futures_report_daily_corrected_turning') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM pg_attribute
           WHERE attrelid='report.v_futures_report_daily_corrected_turning'::regclass
             AND attname='日盤走勢' AND attnum>0 AND NOT attisdropped
       ) THEN
        IF to_regclass('report.v_futures_report_daily_corrected_turning_without_session_trend') IS NOT NULL THEN
            RAISE EXCEPTION '舊版 corrected turning 相容 View 已存在，請先確認先前遷移狀態';
        END IF;
        ALTER VIEW report.v_futures_report_daily_corrected_turning
            RENAME TO v_futures_report_daily_corrected_turning_without_session_trend;
    END IF;

    IF to_regclass('report.v_futures_dashboard_turning') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM pg_attribute
           WHERE attrelid='report.v_futures_dashboard_turning'::regclass
             AND attname='日盤走勢' AND attnum>0 AND NOT attisdropped
       ) THEN
        IF to_regclass('report.v_futures_dashboard_turning_without_session_trend') IS NOT NULL THEN
            RAISE EXCEPTION '舊版 Dashboard 相容 View 已存在，請先確認先前遷移狀態';
        END IF;
        ALTER VIEW report.v_futures_dashboard_turning
            RENAME TO v_futures_dashboard_turning_without_session_trend;
    END IF;

    SELECT string_agg(
               format('b.%I', attname)
               || CASE WHEN attname='買賣權比_週五' THEN ', '||additions ELSE '' END,
               ', ' ORDER BY attnum
           )
      INTO fields
      FROM pg_attribute
     WHERE attrelid='report.v_futures_report_daily_without_turning'::regclass
       AND attnum>0 AND NOT attisdropped;
    EXECUTE 'CREATE OR REPLACE VIEW report.v_futures_report_daily AS SELECT '||fields||
            ' FROM report.v_futures_report_daily_without_turning b '
            'LEFT JOIN report.futures_turning_levels t '
            'ON t.futures_id=b.futures_id AND t."日期"=b."日期"';

    SELECT string_agg(
               format('b.%I', attname)
               || CASE WHEN attname='買賣權比_週五' THEN ', '||additions ELSE '' END,
               ', ' ORDER BY attnum
           )
      INTO fields
      FROM pg_attribute
     WHERE attrelid='report.v_futures_report_daily_corrected'::regclass
       AND attnum>0 AND NOT attisdropped;
    EXECUTE 'CREATE OR REPLACE VIEW report.v_futures_report_daily_corrected_turning AS SELECT '||fields||
            ' FROM report.v_futures_report_daily_corrected b '
            'LEFT JOIN report.futures_turning_levels t '
            'ON t.futures_id=b.futures_id AND t."日期"=b."日期"';

    IF to_regclass('report.futures_report_daily_dashboard_cache') IS NOT NULL THEN
        SELECT string_agg(
                   format('b.%I', attname)
                   || CASE WHEN attname='買賣權比_週五' THEN ', '||additions ELSE '' END,
                   ', ' ORDER BY attnum
               )
          INTO fields
          FROM pg_attribute
         WHERE attrelid='report.futures_report_daily_dashboard_cache'::regclass
           AND attnum>0 AND NOT attisdropped;
        EXECUTE 'CREATE OR REPLACE VIEW report.v_futures_dashboard_turning AS SELECT '||fields||
                ' FROM report.futures_report_daily_dashboard_cache b '
                'LEFT JOIN report.futures_turning_levels t '
                'ON t.futures_id=b.futures_id AND t."日期"=b."日期"';
    END IF;
END $trend$;

COMMIT;

-- 驗證欄位順序與最近資料：
SELECT ordinal_position, column_name
FROM information_schema.columns
WHERE table_schema='report'
  AND table_name='v_futures_report_daily_corrected_turning'
  AND column_name IN ('日盤走勢','日盤轉折支撐','日盤轉折壓力',
                      '夜盤走勢','夜盤轉折支撐','夜盤轉折壓力')
ORDER BY ordinal_position;

SELECT "日期", "日盤高", "日盤低", "日盤收", "日盤走勢",
       "日盤轉折支撐", "日盤轉折壓力",
       "夜盤高", "夜盤低", "夜盤收", "夜盤走勢",
       "夜盤轉折支撐", "夜盤轉折壓力"
FROM report.v_futures_report_daily_corrected_turning
WHERE futures_id='TX'
ORDER BY "日期" DESC
LIMIT 20;
