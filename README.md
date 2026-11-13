# SFMC Email Analytics — dbt + Snowflake (Medallion Architecture)

Pipeline: **SFMC API → S3 → Snowflake (bronze/silver/gold) → BI**, orchestrated with **dbt-core**,
built to run from **WSL Ubuntu**, with isolated **dev / uat / prod** Snowflake environments.

Data models covered: **Email Account Summary** and **Email Journey Summary**.

---

## 1. Architecture

```
S3 (timestamped JSON)
   │  external stage + COPY INTO / Snowpipe
   ▼
BRONZE  (models/staging)      -- views, VARIANT flattened, 1 file-version dedupe
   │
   ▼
SILVER  (models/intermediate) -- Snowflake TRANSIENT tables, cleaning, rate metrics,
   │                             SCD2 join (dbt snapshots) for account attributes at time of report
   ▼
GOLD    (models/marts/email_analytics) -- incremental/merge fact tables + Type-1 dim, BI-ready
```

| Layer  | Folder                          | Materialization                     | Snowflake object   |
|--------|----------------------------------|--------------------------------------|---------------------|
| Bronze | `models/staging/sfmc`           | `view`                               | View                |
| Silver | `models/intermediate/sfmc`      | `transient_table` (custom macro)     | Transient table     |
| SCD2   | `snapshots/`                    | `snapshot` (timestamp strategy)      | Regular table (needs Time Travel) |
| Gold   | `models/marts/email_analytics`  | `incremental` + `merge`              | Table (clustered)   |

---

## 2. Prerequisites (WSL Ubuntu)

```bash
sudo apt update && sudo apt install -y python3-venv python3-pip git

# clone / place this project, then:
cd sfmc_dbt_project
python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install dbt-core dbt-snowflake
```

Verify:
```bash
dbt --version
```

---

## 3. Snowflake setup (run once, as ACCOUNTADMIN, via SnowSQL / Snowsight)

```bash
snowsql -a <account> -u <admin_user> -f snowflake_setup/01_databases_roles_warehouses.sql
snowsql -a <account> -u <admin_user> -f snowflake_setup/02_storage_integration_stage.sql
snowsql -a <account> -u <admin_user> -f snowflake_setup/03_debug_monitor_procedures.sql
```

This creates, per environment (dev/uat/prod):
- Warehouse, database, roles
- Bronze/Silver/Silver_snapshots/Gold schemas
- S3 storage integration + external stage + file formats
- Raw landing tables (`RAW_EMAIL_ACCOUNT_SUMMARY`, `RAW_EMAIL_JOURNEY_SUMMARY`)
- COPY INTO / Snowpipe automation
- Debug procedures + Snowflake Task scheduler alternative to Airflow

> After creating the storage integration, run `DESC STORAGE INTEGRATION SFMC_S3_INT;`
> and update the AWS IAM role trust policy with `STORAGE_AWS_IAM_USER_ARN` / `STORAGE_AWS_EXTERNAL_ID`.

### 3a. Alternative: no AWS/S3 (internal stage)

If you don't have an S3 bucket yet, use `snowflake_setup/02_internal_stage.sql` instead of
`02_storage_integration_stage.sql` — same file formats + raw tables, but the stages are
Snowflake-managed (no `STORAGE_INTEGRATION`, no AWS IAM trust policy step). No Snowpipe
auto-ingest in this mode (that needs S3 event notifications); load files with `PUT` +
`COPY INTO` (or the `SP_LOAD_RAW` proc / scheduled task from script 03) instead.

```bash
snowsql -a <account> -u <admin_user> -f snowflake_setup/01_databases_roles_warehouses.sql
snowsql -a <account> -u <admin_user> -f snowflake_setup/02_internal_stage.sql
snowsql -a <account> -u <admin_user> -f snowflake_setup/03_debug_monitor_procedures.sql
```

### 3b. Uploading export files to an internal stage

Export files from SFMC (or any source) as JSON arrays, one record per element — see
`raw_email_account_summary` / `raw_email_journey_summary` column definitions in
`models/staging/sfmc/_sfmc__sources.yml` and the `raw_payload:<field>::<type>` casts in
`models/staging/sfmc/stg_sfmc__*.sql` for the exact field names each stage expects
(`accountId`, `reportDate`, `modifiedDate`, etc. — camelCase, matching the SFMC API).

Upload with SnowSQL's `PUT` command (run from WSL, one file per stage):

```bash
snowsql -a <account> -u <user> -d SFMC_RAW_DEV -s PUBLIC -q "
  PUT file:///path/to/email_account_summary_<timestamp>.json @STG_EMAIL_ACCOUNT_SUMMARY AUTO_COMPRESS=TRUE;
  PUT file:///path/to/email_journey_summary_<timestamp>.json @STG_EMAIL_JOURNEY_SUMMARY AUTO_COMPRESS=TRUE;
"
```

Then load staged files into the raw tables via the reusable procedure from script 03
(idempotent — skips already-loaded files by name/checksum for 64 days):

```sql
CALL SFMC_RAW_DEV.PUBLIC.SP_LOAD_RAW(
  'SFMC_RAW_DEV.PUBLIC.STG_EMAIL_ACCOUNT_SUMMARY',
  'SFMC_RAW_DEV.PUBLIC.RAW_EMAIL_ACCOUNT_SUMMARY',
  'SFMC_RAW_DEV.PUBLIC.JSON_FF');

CALL SFMC_RAW_DEV.PUBLIC.SP_LOAD_RAW(
  'SFMC_RAW_DEV.PUBLIC.STG_EMAIL_JOURNEY_SUMMARY',
  'SFMC_RAW_DEV.PUBLIC.RAW_EMAIL_JOURNEY_SUMMARY',
  'SFMC_RAW_DEV.PUBLIC.JSON_FF');
```

> **No SnowSQL installed?** SnowSQL's installer lives behind a login-gated download page
> (no stable public URL to script around), so if it's not on your box yet, the `PUT` /
> `COPY INTO` / `CALL` statements above run identically from a Snowsight worksheet, or via
> `scripts/upload_to_stage.py` (uses `snowflake-connector-python`, already a dependency of
> `dbt-snowflake`, so it's in your `.venv` once `pip install dbt-snowflake` has run):
> ```bash
> export SNOWFLAKE_ACCOUNT="<account>" SNOWFLAKE_USER="<user>" SNOWFLAKE_PASSWORD="<password>"
> python3 scripts/upload_to_stage.py --feed account_summary /path/to/email_account_summary_<timestamp>.json
> python3 scripts/upload_to_stage.py --feed journey_summary /path/to/email_journey_summary_<timestamp>.json
> ```
> `--feed` picks the right stage + raw table; each call PUTs the file(s) then runs
> `SP_LOAD_RAW` (skip that with `--no-load` to only stage the file).

---

## 4. dbt profile setup

```bash
mkdir -p ~/.dbt
cp .dbt/profiles.yml.example ~/.dbt/profiles.yml
```

Set credentials as environment variables (add to `~/.bashrc` or a `.env` + `direnv`):

```bash
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USER="your_dev_user"
export SNOWFLAKE_PASSWORD="********"

# UAT / PROD service accounts
export SNOWFLAKE_UAT_USER="svc_uat"
export SNOWFLAKE_UAT_PASSWORD="********"
export SNOWFLAKE_PROD_USER="svc_prod"
export SNOWFLAKE_PROD_PK_PATH="/home/$(whoami)/.ssh/snowflake_prod_rsa_key.p8"
export SNOWFLAKE_PROD_PK_PASSPHRASE="********"
```

Test connection per environment:
```bash
dbt debug --target dev
dbt debug --target uat
dbt debug --target prod
```

---

## 5. Install packages

```bash
dbt deps       # installs dbt_utils, codegen, dbt_expectations, snowflake_utils
```

---

## 6. Core run commands (all materializations, by layer)

```bash
# --- everything, dev, in dependency order ---
dbt build --target dev            # run + test + snapshot + seed, respecting the DAG

# --- layer by layer (bronze -> silver -> gold) ---
dbt run --target dev --select tag:bronze
dbt test --target dev --select tag:bronze

dbt snapshot --target dev                       # SCD2 (must run before silver journey enrichment)
dbt run --target dev --select tag:silver
dbt test --target dev --select tag:silver

dbt run --target dev --select tag:gold
dbt test --target dev --select tag:gold

# --- single model + its dependencies upstream/downstream ---
dbt run --target dev --select +fct_email_account_summary     # this model + all ancestors
dbt run --target dev --select fct_email_journey_summary+     # this model + all descendants
dbt run --target dev --select stg_sfmc__email_account_summary+int_sfmc__email_account_summary_enriched

# --- full refresh (rebuild incremental tables from scratch) ---
dbt run --target dev --full-refresh --select fct_email_account_summary fct_email_journey_summary

# --- specific materialization override at runtime ---
dbt run --target dev --select stg_sfmc__email_account_summary --vars '{"materialized": "table"}'

# --- seeds (reference/lookup data, e.g. business unit mapping) ---
dbt seed --target dev

# --- generate + serve docs (lineage graph, column descriptions, exposures) ---
dbt docs generate --target dev
dbt docs serve --port 8080          # browse at http://localhost:8080 from WSL (or use --host 0.0.0.0)
```

### One-shot wrapper script
```bash
chmod +x scripts/run_pipeline.sh
./scripts/run_pipeline.sh dev full            # full bronze->silver->gold + docs
./scripts/run_pipeline.sh uat models_only
./scripts/run_pipeline.sh prod snapshot_only
./scripts/run_pipeline.sh dev test_only
```

---

## 7. Environment promotion (dev → uat → prod)

```bash
# dev: iterate freely
dbt build --target dev

# uat: validate against a stable dataset before prod release
dbt build --target uat --full-refresh

# prod: scheduled via Airflow (see below) — CLI equivalent:
dbt build --target prod
```

Recommended CI gate: `dbt build --target uat` must pass (including `dbt test`) before merging to the branch that triggers the prod Airflow DAG.

---

## 8. Testing

```bash
dbt test --target dev                                   # all tests
dbt test --target dev --select tag:bronze                # source freshness/uniqueness at bronze
dbt test --target dev --select fct_email_account_summary # tests for one model
dbt source freshness --target dev                        # check S3->Snowflake load latency vs SLA
```

Test types used in this project:
- `not_null`, `unique` (schema tests)
- `dbt_utils.unique_combination_of_columns` (composite natural key checks, bronze)
- `dbt_expectations.expect_column_values_to_be_between` (rate metrics 0–1, gold)

---

## 9. Debugging

### dbt-side
```bash
dbt debug --target dev                      # connection/config sanity check
dbt compile --select fct_email_account_summary --target dev   # see rendered SQL without running
dbt run --target dev --select fct_email_account_summary --debug   # verbose logs
cat logs/dbt.log                            # full run log
dbt ls --select tag:silver --target dev     # list resolved models in a selector (dry check)
dbt list --resource-type source             # confirm sources.yml resolves correctly
```

### Snowflake-side (see `snowflake_setup/03_debug_monitor_procedures.sql`)
```sql
-- Was a specific file already loaded / did it error?
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME=>'SFMC_RAW_DEV.PUBLIC.RAW_EMAIL_ACCOUNT_SUMMARY',
  START_TIME=>DATEADD('day',-1,CURRENT_TIMESTAMP())));

-- Snowpipe stuck/backlog?
SELECT SYSTEM$PIPE_STATUS('SFMC_RAW_DEV.PUBLIC.PIPE_EMAIL_ACCOUNT_SUMMARY');

-- Find failed/slow dbt-tagged queries in last 24h
SELECT query_id, query_text, error_message, total_elapsed_time/1000 elapsed_sec
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag ILIKE 'dbt_%' AND execution_status='FAIL'
ORDER BY start_time DESC;

-- Task run history (if using Snowflake Tasks instead of/alongside Airflow)
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY scheduled_time DESC;
```

Manual reprocess of one raw table:
```sql
CALL SFMC_RAW_DEV.PUBLIC.SP_LOAD_RAW(
  'SFMC_RAW_DEV.PUBLIC.STG_EMAIL_ACCOUNT_SUMMARY',
  'SFMC_RAW_DEV.PUBLIC.RAW_EMAIL_ACCOUNT_SUMMARY',
  'SFMC_RAW_DEV.PUBLIC.JSON_FF');
```

---

## 10. Airflow orchestration (production schedule)

Typical DAG task order (see your Airflow repo for the actual DAG file):
1. `extract_sfmc_api` (Python, pagination 100–200 records/page) → writes timestamped files to S3
2. `wait_for_s3_files` (S3KeySensor)
3. `snowflake_copy_into` (SnowflakeOperator, or rely on Snowpipe auto-ingest)
4. `dbt_run_bronze` → `BashOperator`: `dbt run --target prod --select tag:bronze`
5. `dbt_test_bronze`
6. `dbt_snapshot` → `dbt snapshot --target prod`
7. `dbt_run_silver` → `dbt run --target prod --select tag:silver`
8. `dbt_test_silver`
9. `dbt_run_gold` → `dbt run --target prod --select tag:gold`
10. `dbt_test_gold`
11. `dbt_docs_generate` (optional, publish to S3/static host)

---

## 11. Project structure

```
sfmc_dbt_project/
├── dbt_project.yml
├── packages.yml
├── README.md
├── .dbt/profiles.yml.example
├── snowflake_setup/
│   ├── 01_databases_roles_warehouses.sql
│   ├── 02_storage_integration_stage.sql
│   └── 03_debug_monitor_procedures.sql
├── models/
│   ├── staging/sfmc/           # BRONZE
│   │   ├── _sfmc__sources.yml
│   │   ├── _sfmc__staging.yml
│   │   ├── stg_sfmc__email_account_summary.sql
│   │   └── stg_sfmc__email_journey_summary.sql
│   ├── intermediate/sfmc/      # SILVER (transient tables)
│   │   ├── _sfmc__intermediate.yml
│   │   ├── int_sfmc__account_scd2.sql
│   │   ├── int_sfmc__email_account_summary_enriched.sql
│   │   └── int_sfmc__email_journey_summary_enriched.sql
│   └── marts/email_analytics/  # GOLD
│       ├── _email_analytics__marts.yml
│       ├── dim_email_account.sql
│       ├── fct_email_account_summary.sql
│       └── fct_email_journey_summary.sql
├── snapshots/
│   └── sfmc_email_account_scd2.sql   # SCD2 via dbt snapshot
├── macros/
│   ├── generate_surrogate_key.sql
│   ├── get_custom_schema.sql
│   ├── query_tag.sql
│   └── materializations/transient_table.sql
└── scripts/
    └── run_pipeline.sh
```

---

## 12. Notes / design decisions

- **Bronze as views**: cheap, always reflects raw table, no storage cost — acceptable since raw tables are small VARIANT landing tables.
- **Silver as transient tables**: rebuildable from bronze at any time, so we don't pay for Snowflake's Fail-safe (7-day) storage; transient tables skip Fail-safe entirely.
- **SCD2 via dbt snapshot**, not hand-rolled merge — dbt manages `dbt_valid_from/to` and change detection; a thin view (`int_sfmc__account_scd2`) renames these to the classic effective_start/end/is_active shape.
- **Gold as incremental + merge**: safe re-runs, only touches changed `modified_date` partitions, clustered by `report_date` for scan pruning at BI-query time.
- **Type 1 vs Type 2**: journey/account summary facts don't need history themselves (each report_date row is already immutable); only the account *dimension* (name/business unit reassignment) needs SCD2, which is why only `sfmc_email_account_scd2` is a snapshot.
