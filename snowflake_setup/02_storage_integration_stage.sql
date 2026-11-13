-- =====================================================================
-- 02_storage_integration_stage.sql
-- Creates the S3 storage integration + external stage bronze reads from
-- Run as ACCOUNTADMIN once per Snowflake account (integration is account-level)
-- =====================================================================

-- 1. Storage integration (trusts the IAM role your S3 bucket policy allows)
CREATE STORAGE INTEGRATION IF NOT EXISTS SFMC_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/snowflake-sfmc-role'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://sfmc-landing-bucket/email_account_summary/',
    's3://sfmc-landing-bucket/email_journey_summary/'
  );

-- Run this, then update the AWS IAM trust policy with the output:
DESC STORAGE INTEGRATION SFMC_S3_INT;
-- copy STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID into the IAM role trust relationship

-- 2. File formats
CREATE FILE FORMAT IF NOT EXISTS SFMC_RAW_DEV.PUBLIC.JSON_FF
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  COMPRESSION = 'AUTO';

CREATE FILE FORMAT IF NOT EXISTS SFMC_RAW_DEV.PUBLIC.CSV_FF
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  COMPRESSION = 'AUTO';

-- 3. External stages per env (repeat block for UAT / PROD databases + buckets)
CREATE STAGE IF NOT EXISTS SFMC_RAW_DEV.PUBLIC.STG_EMAIL_ACCOUNT_SUMMARY
  URL = 's3://sfmc-landing-bucket-dev/email_account_summary/'
  STORAGE_INTEGRATION = SFMC_S3_INT
  FILE_FORMAT = SFMC_RAW_DEV.PUBLIC.JSON_FF;

CREATE STAGE IF NOT EXISTS SFMC_RAW_DEV.PUBLIC.STG_EMAIL_JOURNEY_SUMMARY
  URL = 's3://sfmc-landing-bucket-dev/email_journey_summary/'
  STORAGE_INTEGRATION = SFMC_S3_INT
  FILE_FORMAT = SFMC_RAW_DEV.PUBLIC.JSON_FF;

-- 4. Raw landing tables (VARIANT) that dbt sources point to - loaded via COPY INTO / Snowpipe
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

-- 5. COPY INTO (idempotent - Snowflake skips already-loaded files by name/checksum for 64 days)
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

-- 6. OPTIONAL: automate with Snowpipe instead of manual COPY INTO
CREATE PIPE IF NOT EXISTS SFMC_RAW_DEV.PUBLIC.PIPE_EMAIL_ACCOUNT_SUMMARY
  AUTO_INGEST = TRUE
  AS
  COPY INTO SFMC_RAW_DEV.PUBLIC.RAW_EMAIL_ACCOUNT_SUMMARY (raw_payload, file_name)
  FROM (
    SELECT $1, METADATA$FILENAME
    FROM @SFMC_RAW_DEV.PUBLIC.STG_EMAIL_ACCOUNT_SUMMARY
  )
  FILE_FORMAT = (FORMAT_NAME = SFMC_RAW_DEV.PUBLIC.JSON_FF);

-- SHOW PIPES; -- copy the notification_channel ARN into the S3 event notification config
