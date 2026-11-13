-- Standardizes dbt's native snapshot columns (dbt_scd_id, dbt_valid_from/to)
-- into the classic SCD2 shape: surrogate key, natural key, attrs, effective dates, active flag
{{ config(materialized='view') }}

select
    dbt_scd_id                                   as account_scd_sk,
    account_id,
    name                                         as account_name,
    business_unit_id,
    dbt_valid_from                               as effective_start_date,
    dbt_valid_to                                 as effective_end_date,
    case when dbt_valid_to is null then true else false end as is_active
from {{ ref('sfmc_email_account_scd2') }}
