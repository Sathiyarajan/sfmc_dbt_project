-- Bronze layer: minimal transformation, flatten VARIANT -> typed columns, dedupe by file_name+key
with source as (
    select
        raw_payload,
        file_name,
        loaded_at
    from {{ source('sfmc_raw', 'raw_email_account_summary') }}
),

flattened as (
    select
        raw_payload:accountId::string          as account_id,
        raw_payload:accountName::string         as account_name,
        raw_payload:businessUnitId::string      as business_unit_id,
        raw_payload:emailsSent::number          as emails_sent,
        raw_payload:emailsDelivered::number     as emails_delivered,
        raw_payload:emailsBounced::number       as emails_bounced,
        raw_payload:emailsOpened::number        as emails_opened,
        raw_payload:emailsClicked::number       as emails_clicked,
        raw_payload:unsubscribes::number        as unsubscribes,
        raw_payload:reportDate::timestamp_ntz   as report_date,
        raw_payload:modifiedDate::timestamp_ntz as modified_date,
        file_name,
        loaded_at,
        row_number() over (
            partition by raw_payload:accountId::string, raw_payload:reportDate::timestamp_ntz
            order by loaded_at desc
        ) as rn
    from source
)

select
    account_id,
    account_name,
    business_unit_id,
    emails_sent,
    emails_delivered,
    emails_bounced,
    emails_opened,
    emails_clicked,
    unsubscribes,
    report_date,
    modified_date,
    file_name,
    loaded_at
from flattened
where rn = 1   -- keep latest file version per natural key (bronze-level dedupe of re-landed files)
