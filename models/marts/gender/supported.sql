{{ config(
  materialized='table',
  tags= ["commcare_extraction", "gender_cases", "gender"]
) }}

with
case_occurrences_data as (
    select
        parent_case_id,
        case_name,
        case_id,
        assigned_to,
        reported_by,
        {{ validate_date("date_of_case_reporting") }} as date_of_case_reporting,
        {{ validate_date("date_of_case_closure") }} as date_of_case_closure,
        county,
        site
    from {{ ref("case_occurence_pii") }}
),

date_range as (
    -- Generate a series of months covering the period of interest
    select
        date_trunc(
            'month',
            generate_series(
                (select min(date_of_case_reporting) from case_occurrences_data),  -- Earliest date_of_reporting
                coalesce(
                    (select max(date_of_case_closure) from case_occurrences_data),  -- Latest date_of_case_closure
                    current_date
                ),
                interval '1 month'
            )
        ) as month
),

cases_expanded as (
    select
        c.parent_case_id,
        c.case_name,
        c.assigned_to,
        c.reported_by,
        c.date_of_case_reporting,
        c.date_of_case_closure,
        c.county,
        c.site,
        dr.month
    from case_occurrences_data as c
    cross join date_range as dr
    where
        -- Check if the case is open during the given month
        c.date_of_case_reporting <= (dr.month + interval '1 month' - interval '1 day')
        and (c.date_of_case_closure >= dr.month or c.date_of_case_closure is null)
)

select
    c.month,
    c.parent_case_id,
    c.case_name,
    c.assigned_to,
    c.reported_by,
    c.county,
    c.site,
    case
        when
            c.date_of_case_reporting <= (c.month + interval '1 month' - interval '1 day')
            and (c.date_of_case_closure >= c.month or c.date_of_case_closure is null)
            then 1
        else 0
    end as is_case_open
from cases_expanded as c
order by c.parent_case_id, c.month
