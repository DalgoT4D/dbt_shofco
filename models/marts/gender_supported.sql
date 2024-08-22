with
    case_occurrences_data as (
        select
            parent_case_id,
            case_id,
            {{ validate_date("date_of_reporting") }} as date_of_reporting,
            {{ validate_date("date_of_case_closure") }} as date_of_case_closure

        from {{ ref("stg_gender_case_occurrences_commcare") }}
    ),

    date_range as (
        -- Generate a series of months covering the period of interest
        select
            date_trunc(
                'month',
                generate_series(
                    (select min(date_of_reporting) from case_occurrences_data),  -- Earliest date_of_reporting
                    coalesce(
                        (select max(date_of_case_closure) from case_occurrences_data),  -- Latest date_of_case_closure
                        current_date
                    ),
                    interval '1 month'
                )
            ) as month
    ),

    cases_expanded as (
        select c.parent_case_id, c.date_of_reporting, c.date_of_case_closure, dr.month
        from case_occurrences_data c
        cross join date_range dr
        where
            -- Check if the case is open during the given month
            c.date_of_reporting <= (dr.month + interval '1 month' - interval '1 day')
            and (c.date_of_case_closure >= dr.month or c.date_of_case_closure is null)
    )

select
    month,
    parent_case_id,
    case
        when
            c.date_of_reporting <= (month + interval '1 month' - interval '1 day')
            and (c.date_of_case_closure >= month or c.date_of_case_closure is null)
        then 1
        else 0
    end as is_case_open
from cases_expanded c
order by parent_case_id, month
