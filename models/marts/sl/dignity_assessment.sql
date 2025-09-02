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
        lower(nullif(challenges_accessing_personal_hygiene_items, '')) as hygiene_challenges,
        nullif(dignity_kit_support, '') as dignity_kit_support,
        lower(nullif(head_gender, '')) as head_gender,
        lower(nullif(receiving_aid, '')) as receiving_aid,
        nullif(household_head, '') as household_head,
        nullif(hh_engaged_in_work, '') as hh_engaged_in_work,
        nullif(forego_basic_essentials, '') as forego_basic_essentials,
        nullif(number_of_meals_reduced, '') as number_of_meals_reduced
    from {{ ref('upskilling') }}

    union all

    select
        case_id,
        lower(nullif(dignity_experienced_days_nothing, '')) as experienced_days,
        lower(nullif(dignity_miss_school, '')) as missed_school,
        lower(nullif(dignity_challenges_hygiene, '')) as hygiene_challenges,
        nullif(dignity_kit_support, '') as dignity_kit_support,
        null as head_gender,  -- not available in enrollment
        null as receiving_aid,  -- not available in enrollment
        null as household_head,  -- not available in enrollment
        null as hh_engaged_in_work,  -- not available in enrollment
        null as forego_basic_essentials,  -- not available in enrollment
        null as number_of_meals_reduced  -- not available in enrollment
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
        end as challenges_accessing_personal_hygiene_items,

        -- Support details: combine non-null values
        string_agg(distinct dignity_kit_support, '; ') as dignity_kit_support,

        -- Categorical fields: take first non-null value or use boolean logic for yes/no
        max(head_gender) as head_gender,
        case
            when bool_or(receiving_aid = 'yes') then 'yes'
            when bool_or(receiving_aid = 'no') then 'no'
            else null
        end as receiving_aid,
        max(household_head) as household_head,
        max(hh_engaged_in_work) as hh_engaged_in_work,
        max(forego_basic_essentials) as forego_basic_essentials,
        max(number_of_meals_reduced) as number_of_meals_reduced

    from combined
    group by case_id
)

select * from grouped