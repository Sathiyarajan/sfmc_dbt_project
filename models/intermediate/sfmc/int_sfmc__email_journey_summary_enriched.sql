with stg as (
    select * from {{ ref('stg_sfmc__email_journey_summary') }}
),

deduped as (
    select *,
        row_number() over (
            partition by journey_id, email_activity_id, report_date
            order by modified_date desc
        ) as rn
    from stg
),

-- enrich with the SCD2 current account name/business unit at time of report
account_dim as (
    select account_id, account_name, business_unit_id, effective_start_date, effective_end_date
    from {{ ref('int_sfmc__account_scd2') }}
),

enriched as (
    select
        {{ sfmc_surrogate_key(['j.journey_id', 'j.email_activity_id', 'j.report_date']) }} as journey_summary_sk,
        j.journey_id,
        j.journey_name,
        j.journey_version,
        j.account_id,
        a.account_name,
        a.business_unit_id,
        j.email_activity_id,
        j.report_date,
        j.emails_sent,
        j.emails_delivered,
        j.emails_opened,
        j.emails_clicked,
        j.goal_conversions,
        div0(j.emails_delivered, nullif(j.emails_sent, 0))   as delivery_rate,
        div0(j.emails_opened, nullif(j.emails_delivered, 0)) as open_rate,
        div0(j.emails_clicked, nullif(j.emails_opened, 0))   as click_through_rate,
        div0(j.goal_conversions, nullif(j.emails_clicked, 0)) as conversion_rate,
        j.modified_date,
        current_timestamp() as dbt_enriched_at
    from deduped j
    left join account_dim a
        on j.account_id = a.account_id
        and j.report_date >= a.effective_start_date
        and (j.report_date < a.effective_end_date or a.effective_end_date is null)
    where j.rn = 1
)

select * from enriched
