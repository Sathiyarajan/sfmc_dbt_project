{{
    config(
        materialized='incremental',
        unique_key='journey_summary_sk',
        incremental_strategy='merge',
        merge_update_columns=[
            'emails_sent','emails_delivered','emails_opened','emails_clicked','goal_conversions',
            'delivery_rate','open_rate','click_through_rate','conversion_rate',
            'modified_date','dbt_enriched_at'
        ],
        cluster_by=['report_date']
    )
}}

select
    journey_summary_sk,
    journey_id,
    journey_name,
    journey_version,
    account_id,
    account_name,
    business_unit_id,
    email_activity_id,
    report_date,
    emails_sent,
    emails_delivered,
    emails_opened,
    emails_clicked,
    goal_conversions,
    delivery_rate,
    open_rate,
    click_through_rate,
    conversion_rate,
    modified_date,
    dbt_enriched_at
from {{ ref('int_sfmc__email_journey_summary_enriched') }}

{% if is_incremental() %}
where modified_date > (select coalesce(max(modified_date), '1900-01-01') from {{ this }})
{% endif %}
