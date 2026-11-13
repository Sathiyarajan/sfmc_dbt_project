-- Gold layer: reporting-ready fact table for email account summary
-- Incremental + merge strategy on the surrogate key, keyed to report_date for pruning

{{
    config(
        materialized='incremental',
        unique_key='account_summary_sk',
        incremental_strategy='merge',
        merge_update_columns=[
            'emails_sent','emails_delivered','emails_bounced','emails_opened','emails_clicked',
            'unsubscribes','delivery_rate','open_rate','click_through_rate','bounce_rate',
            'unsubscribe_rate','modified_date','dbt_enriched_at'
        ],
        cluster_by=['report_date']
    )
}}

select
    account_summary_sk,
    account_id,
    account_name,
    business_unit_id,
    report_date,
    emails_sent,
    emails_delivered,
    emails_bounced,
    emails_opened,
    emails_clicked,
    unsubscribes,
    delivery_rate,
    open_rate,
    click_through_rate,
    bounce_rate,
    unsubscribe_rate,
    modified_date,
    dbt_enriched_at
from {{ ref('int_sfmc__email_account_summary_enriched') }}

{% if is_incremental() %}
where modified_date > (select coalesce(max(modified_date), '1900-01-01') from {{ this }})
{% endif %}
