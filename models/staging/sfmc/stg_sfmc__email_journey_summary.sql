with source as (
    select
        raw_payload,
        file_name,
        loaded_at
    from {{ source('sfmc_raw', 'raw_email_journey_summary') }}
),

flattened as (
    select
        raw_payload:journeyId::string           as journey_id,
        raw_payload:journeyName::string          as journey_name,
        raw_payload:versionNumber::number        as journey_version,
        raw_payload:accountId::string            as account_id,
        raw_payload:emailActivityId::string      as email_activity_id,
        raw_payload:emailsSent::number           as emails_sent,
        raw_payload:emailsDelivered::number      as emails_delivered,
        raw_payload:emailsOpened::number         as emails_opened,
        raw_payload:emailsClicked::number        as emails_clicked,
        raw_payload:goalConversions::number      as goal_conversions,
        raw_payload:reportDate::timestamp_ntz    as report_date,
        raw_payload:modifiedDate::timestamp_ntz  as modified_date,
        file_name,
        loaded_at,
        row_number() over (
            partition by raw_payload:journeyId::string, raw_payload:emailActivityId::string,
                         raw_payload:reportDate::timestamp_ntz
            order by loaded_at desc
        ) as rn
    from source
)

select
    journey_id,
    journey_name,
    journey_version,
    account_id,
    email_activity_id,
    emails_sent,
    emails_delivered,
    emails_opened,
    emails_clicked,
    goal_conversions,
    report_date,
    modified_date,
    file_name,
    loaded_at
from flattened
where rn = 1
