{% snapshot sfmc_email_account_scd2 %}

{{
    config(
        target_schema='silver_snapshots',
        unique_key='account_id',
        strategy='timestamp',
        updated_at='modified_date',
        invalidate_hard_deletes=True
    )
}}

select
    account_id,
    account_name as name,
    business_unit_id,
    modified_date
from {{ ref('stg_sfmc__email_account_summary') }}
qualify row_number() over (partition by account_id order by modified_date desc) = 1

{% endsnapshot %}
