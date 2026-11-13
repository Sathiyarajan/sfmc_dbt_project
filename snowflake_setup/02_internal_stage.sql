-- =====================================================================
-- 02_internal_stage.sql
-- Alternative to 02_storage_integration_stage.sql: no S3/AWS needed.
-- Uses a Snowflake-managed internal stage; upload files with PUT or via
-- the Snowsight UI (Data > Databases > SFMC_RAW_DEV > PUBLIC > Stages).
-- Run as ACCOUNTADMIN (or the target-env dbt role, since no account-level
-- storage integration is involved).
-- =====================================================================

-- 1. File formats
CREATE FILE FORMAT IF NOT EXISTS SFMC_RAW_DEV.PUBLIC.JSON_FF
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  COMPRESSION = 'AUTO';

CREATE FILE FORMAT IF NOT EXISTS SFMC_RAW_DEV.PUBLIC.CSV_FF
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  COMPRESSION = 'AUTO';

-- 2. Internal stages (Snowflake-managed storage, no STORAGE_INTEGRATION/URL)
CREATE STAGE IF NOT EXISTS SFMC_RAW_DEV.PUBLIC.STG_EMAIL_ACCOUNT_SUMMARY
  FILE_FORMAT = SFMC_RAW_DEV.PUBLIC.JSON_FF;

CREATE STAGE IF NOT EXISTS SFMC_RAW_DEV.PUBLIC.STG_EMAIL_JOURNEY_SUMMARY
  FILE_FORMAT = SFMC_RAW_DEV.PUBLIC.JSON_FF;

-- 3. Raw landing tables (VARIANT) that dbt sources point to
CREATE TABLE IF NOT EXISTS SFMC_RAW_DEV.PUBLIC.RAW_EMAIL_ACCOUNT_SUMMARY (
  raw_payload   VARIANT,
  file_name     STRING,
  loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SFMC_RAW_DEV.PUBLIC.RAW_EMAIL_JOURNEY_SUMMARY (
  raw_payload   VARIANT,
  file_name     STRING,
  loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 4. Upload files (run from WSL, after `dbt debug` succeeds, using SnowSQL,
--    or drag-and-drop in Snowsight under the stage's "Files" tab):
--
--    snowsql -a JZQAAFS-VE18684 -u msclaw -d SFMC_RAW_DEV -s PUBLIC -q "
--      PUT file:///path/to/email_account_summary_*.json @STG_EMAIL_ACCOUNT_SUMMARY;
--      PUT file:///path/to/email_journey_summary_*.json @STG_EMAIL_JOURNEY_SUMMARY;
--    "

-- 5. COPY INTO from stage (idempotent - Snowflake skips already-loaded files
--    by name/checksum for 64 days). Also wrapped by SP_LOAD_RAW procedure
--    and TASK_LOAD_EMAIL_ACCOUNT_SUMMARY from 03_debug_monitor_procedures.sql
--    so this can run on the existing schedule instead of manually.
COPY INTO SFMC_RAW_DEV.PUBLIC.RAW_EMAIL_ACCOUNT_SUMMARY (raw_payload, file_name)
  FROM (
    SELECT $1, METADATA$FILENAME
    FROM @SFMC_RAW_DEV.PUBLIC.STG_EMAIL_ACCOUNT_SUMMARY
  )
  FILE_FORMAT = (FORMAT_NAME = SFMC_RAW_DEV.PUBLIC.JSON_FF)
  ON_ERROR = 'CONTINUE';

COPY INTO SFMC_RAW_DEV.PUBLIC.RAW_EMAIL_JOURNEY_SUMMARY (raw_payload, file_name)
  FROM (
    SELECT $1, METADATA$FILENAME
    FROM @SFMC_RAW_DEV.PUBLIC.STG_EMAIL_JOURNEY_SUMMARY
  )
  FILE_FORMAT = (FORMAT_NAME = SFMC_RAW_DEV.PUBLIC.JSON_FF)
  ON_ERROR = 'CONTINUE';

-- NOTE: no Snowpipe/AUTO_INGEST here — auto-ingest needs S3 event
-- notifications, which don't exist for internal stages. Either run the
-- COPY INTO statements above manually after each upload, or resume the
-- existing task (suspended by default):
--   ALTER TASK SFMC_RAW_DEV.PUBLIC.TASK_LOAD_EMAIL_ACCOUNT_SUMMARY RESUME;
