{{ config(
    materialized='table',
    unique_key='case_id',
    tags=['sl', 'daycare']
) }}

with combined as (

    select
        case_id,
        nullif(children_in_daycare, '')::int as number_of_children,
        lower(nullif(children_under_4, '')) as children_under_4,
        lower(nullif(children_with_disabilities, '')) as children_with_disabilities
    from {{ ref('upskilling') }}
    where has_children = 'yes'

    union all

    select
        case_id,
        nullif(daycare_number_of_children, '')::int as number_of_children,
        lower(nullif(daycare_children_under_4, '')) as children_under_4,
        lower(nullif(daycare_children_with_disabilities, '')) as children_with_disabilities
    from {{ ref('service_enrollment') }}
    where daycare_do_you_have_children = 'yes'
),

final as (
    select
        case_id,

        -- Numeric: pick highest number reported
        max(number_of_children) as number_of_children,

        -- Yes/No fields: treat as boolean flags
        case
            when bool_or(children_under_4 = 'yes') then 'yes'
            when bool_or(children_under_4 = 'no') then 'no'
            else null
        end as children_under_4,

        case
            when bool_or(children_with_disabilities = 'yes') then 'yes'
            when bool_or(children_with_disabilities = 'no') then 'no'
            else null
        end as children_with_disabilities

    from combined
    group by case_id
)

select * from final