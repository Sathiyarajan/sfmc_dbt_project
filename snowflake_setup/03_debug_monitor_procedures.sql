-- =====================================================================
-- 03_debug_monitor_procedures.sql
-- Debugging / monitoring helpers for the pipeline (run as needed)
-- =====================================================================

-- ---------- Was a file already loaded? (last 14 days of COPY history) ----------
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'SFMC_RAW_DEV.PUBLIC.RAW_EMAIL_ACCOUNT_SUMMARY',
  START_TIME => DATEADD('day', -14, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

-- ---------- Snowpipe load history / stuck files ----------
SELECT *
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
  PIPE_NAME => 'SFMC_RAW_DEV.PUBLIC.PIPE_EMAIL_ACCOUNT_SUMMARY',
  DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE())
));

SELECT SYSTEM$PIPE_STATUS('SFMC_RAW_DEV.PUBLIC.PIPE_EMAIL_ACCOUNT_SUMMARY');

-- ---------- dbt query tag correlation: find long-running / failed dbt models ----------
SELECT query_id, query_text, query_tag, execution_status, error_message,
       start_time, total_elapsed_time/1000 AS elapsed_sec, warehouse_name
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag ILIKE 'dbt_%'
  AND start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  AND (execution_status = 'FAIL' OR total_elapsed_time > 60000)
ORDER BY start_time DESC;

-- ---------- Warehouse credit usage by env (cost debugging) ----------
SELECT warehouse_name, DATE_TRUNC('day', start_time) AS day,
       SUM(credits_used) AS credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name IN ('SFMC_WH_DEV','SFMC_WH_UAT','SFMC_WH_PROD')
  AND start_time >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1,2
ORDER BY 2 DESC, 1;

-- ---------- Task history (if using Snowflake Tasks to schedule COPY/merge instead of Airflow) ----------
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY scheduled_time DESC
LIMIT 100;

-- ---------- Stored proc: reusable raw-load + error log wrapper ----------
CREATE OR REPLACE PROCEDURE SFMC_RAW_DEV.PUBLIC.SP_LOAD_RAW(
  p_stage        STRING,
  p_target_table STRING,
  p_file_format  STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  v_sql STRING;
  v_rows_loaded NUMBER DEFAULT 0;
BEGIN
  v_sql := 'COPY INTO ' || :p_target_table || ' (raw_payload, file_name) ' ||
           'FROM (SELECT $1, METADATA$FILENAME FROM @' || :p_stage || ') ' ||
           'FILE_FORMAT = (FORMAT_NAME = ' || :p_file_format || ') ' ||
           'ON_ERROR = ' || '''CONTINUE''';
  EXECUTE IMMEDIATE :v_sql;
  RETURN 'Load complete for ' || :p_target_table;
EXCEPTION
  WHEN OTHER THEN
    INSERT INTO SFMC_RAW_DEV.PUBLIC.LOAD_ERROR_LOG (proc_name, error_message, occurred_at)
    VALUES ('SP_LOAD_RAW', :SQLERRM, CURRENT_TIMESTAMP());
    RETURN 'FAILED: ' || :SQLERRM;
END;
$$;

CREATE TABLE IF NOT EXISTS SFMC_RAW_DEV.PUBLIC.LOAD_ERROR_LOG (
  proc_name     STRING,
  error_message STRING,
  occurred_at   TIMESTAMP_NTZ
);

-- Example call:
-- CALL SFMC_RAW_DEV.PUBLIC.SP_LOAD_RAW(
--   'SFMC_RAW_DEV.PUBLIC.STG_EMAIL_ACCOUNT_SUMMARY',
--   'SFMC_RAW_DEV.PUBLIC.RAW_EMAIL_ACCOUNT_SUMMARY',
--   'SFMC_RAW_DEV.PUBLIC.JSON_FF'
-- );

-- ---------- Snowflake Task to run the proc on a schedule (alternative/complement to Airflow) ----------
CREATE OR REPLACE TASK SFMC_RAW_DEV.PUBLIC.TASK_LOAD_EMAIL_ACCOUNT_SUMMARY
  WAREHOUSE = SFMC_WH_DEV
  SCHEDULE = 'USING CRON 0 */2 * * * UTC'   -- every 2 hours
AS
  CALL SFMC_RAW_DEV.PUBLIC.SP_LOAD_RAW(
    'SFMC_RAW_DEV.PUBLIC.STG_EMAIL_ACCOUNT_SUMMARY',
    'SFMC_RAW_DEV.PUBLIC.RAW_EMAIL_ACCOUNT_SUMMARY',
    'SFMC_RAW_DEV.PUBLIC.JSON_FF'
  );

-- ALTER TASK SFMC_RAW_DEV.PUBLIC.TASK_LOAD_EMAIL_ACCOUNT_SUMMARY RESUME;
