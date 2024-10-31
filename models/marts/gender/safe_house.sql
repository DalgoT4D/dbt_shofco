{{ config(
  materialized='table'
) }}

with
    case_occurrences_data as (
        select
            case_id,
            assigned_to,
            {{ validate_date("date_of_safehouse_onboarding") }}
            as date_of_safe_house_onboarding,
            {{ validate_date("date_of_discharge") }} as date_of_safe_house_discharge
        from {{ ref("staging_gender_safe_house_commcare") }}
        where {{ validate_date("date_of_safehouse_onboarding") }} IS NOT NULL
    ),

    case_dates as (
        -- Extract the minimum and maximum dates from the cases table if start_date or
        -- end_date is null
        select
            min(date_of_safe_house_onboarding) as earliest_open_date,
            max(
                coalesce(date_of_safe_house_discharge, current_date)
            ) as latest_close_date
        from case_occurrences_data
    ),

    date_range as (
        -- Generate the date range using either the provided start/end dates or the
        -- full range in the dataset if null
        select
            date_trunc(
                'month',
                generate_series(

                    cd.earliest_open_date, cd.latest_close_date, interval '1 month'
                )
            ) as month
        from case_dates cd
    ),

    people_in_safe_houses_per_month as (
        select dr.month, c.case_id, c.assigned_to
        from date_range dr
        left join
            case_occurrences_data c
            on dr.month
            between date_trunc('month', c.date_of_safe_house_onboarding) and coalesce(
                date_trunc('month', c.date_of_safe_house_discharge), '2999-12-31'::date
            )
    )

select assigned_to,month, count(distinct case_id) as people_in_safe_house
from people_in_safe_houses_per_month
group by assigned_to,month
order by assigned_to, month
