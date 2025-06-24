{{ config(
    materialized='table',
    unique_key='case_id',
    tags=['sl', 'dignity_kit']
) }}

with combined as (

    select
        case_id,
        lower(nullif(experienced_days_where_you_had_nothing, '')) as experienced_days,
        lower(nullif(miss_school_due_to_lack_hygiene_products, '')) as missed_school,
        lower(nullif(challenges_accessing_personal_hygiene_items, '')) as hygiene_challenges
    from {{ ref('upskilling') }}

    union all

    select
        case_id,
        lower(nullif(dignity_experienced_days_nothing, '')) as experienced_days,
        lower(nullif(dignity_miss_school, '')) as missed_school,
        lower(nullif(dignity_challenges_hygiene, '')) as hygiene_challenges
    from {{ ref('service_enrollment') }}

),

grouped as (
    select
        case_id,

        -- "yes" if ANY form says yes
        case
            when bool_or(experienced_days = 'yes') then 'yes'
            when bool_or(experienced_days = 'no') then 'no'
            else null
        end as experienced_days_where_you_had_nothing,

        case
            when bool_or(missed_school = 'yes') then 'yes'
            when bool_or(missed_school = 'no') then 'no'
            else null
        end as miss_school_due_to_lack_hygiene_products,

        case
            when bool_or(hygiene_challenges = 'yes') then 'yes'
            when bool_or(hygiene_challenges = 'no') then 'no'
            else null
        end as challenges_accessing_personal_hygiene_items

    from combined
    group by case_id
)

select * from grouped