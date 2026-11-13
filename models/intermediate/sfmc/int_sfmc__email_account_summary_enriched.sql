-- Silver layer: cleaning, deduplication, derived/enriched metrics
-- Materialized as a Snowflake TRANSIENT table (see macros/materializations/transient_table.sql)

with stg as (
    select * from {{ ref('stg_sfmc__email_account_summary') }}
),

deduped as (
    -- defensive re-dedup in case bronze receives late-arriving files
    select *,
        row_number() over (
            partition by account_id, report_date
            order by modified_date desc
        ) as rn
    from stg
),

enriched as (
    select
        {{ sfmc_surrogate_key(['account_id', 'report_date']) }} as account_summary_sk,
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
        -- derived rate metrics
        div0(emails_delivered, nullif(emails_sent, 0))   as delivery_rate,
        div0(emails_opened, nullif(emails_delivered, 0)) as open_rate,
        div0(emails_clicked, nullif(emails_opened, 0))   as click_through_rate,
        div0(emails_bounced, nullif(emails_sent, 0))     as bounce_rate,
        div0(unsubscribes, nullif(emails_delivered, 0))  as unsubscribe_rate,
        modified_date,
        current_timestamp() as dbt_enriched_at
    from deduped
    where rn = 1
      and emails_sent is not null
)

select * from enriched
