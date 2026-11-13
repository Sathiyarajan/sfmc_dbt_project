-- Gold - Type 1 "current state" dimension for easy BI joins (no history)
{{ config(materialized='table') }}

select
    account_id,
    account_name,
    business_unit_id,
    effective_start_date,
    effective_end_date,
    is_active
from {{ ref('int_sfmc__account_scd2') }}
where is_active = true
