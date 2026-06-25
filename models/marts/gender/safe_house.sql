{{ config(
  materialized='table',
  tags=["commcare_extraction", "gender_safe_house", "gender"]
) }}

with
case_occurrences_data as (
    select
        case_id,        CASE 
            WHEN LOWER(assigned_to) = 'wilson.onyango'     THEN 'wilson.obiero'
            WHEN LOWER(assigned_to) = 'zena.khassim'       THEN 'zena.kassim'
            WHEN LOWER(assigned_to) = 'damaris.walengwa'   THEN 'damaris.walegwa'
            WHEN LOWER(assigned_to) = 'elphas.mtekwa'      THEN 'elphus.mtekwa'
            WHEN LOWER(assigned_to) = 'triza.njeri'        THEN 'trizah.njeri'
            WHEN LOWER(assigned_to) = 'wmwita'             THEN 'w.mwita'
            WHEN LOWER(assigned_to) = 'eanas.makokha'      THEN 'esnas.makokha'
            WHEN LOWER(assigned_to) = 'amina.katata'       THEN 'amina.katana'
            WHEN LOWER(assigned_to) = 'emmaculate.achieng' THEN 'emma.achieng'
            ELSE assigned_to
        END as assigned_to,
        {{ validate_date("date_of_safehouse_onboarding") }}
        as date_of_safe_house_onboarding,
        {{ validate_date("date_of_discharge") }} as date_of_safe_house_discharge
    from {{ ref("staging_gender_safe_house_commcare") }}
    where {{ validate_date("date_of_safehouse_onboarding") }} is not NULL
),

case_dates as (
    select
        min(date_of_safe_house_onboarding) as earliest_open_date,
        max(
            coalesce(date_of_safe_house_discharge, current_date)
        ) as latest_close_date
    from case_occurrences_data
),

date_range as (
    select
        date_trunc(
            'month',
            generate_series(
                cd.earliest_open_date, cd.latest_close_date, interval '1 month'
            )
        ) as month
    from case_dates as cd
),

people_in_safe_houses_per_month as (
    select
        dr.month,
        c.case_id,
        c.assigned_to
    from date_range as dr
    left join
        case_occurrences_data as c
        on
            dr.month
            between date_trunc('month', c.date_of_safe_house_onboarding) and coalesce(
                date_trunc('month', c.date_of_safe_house_discharge), '2999-12-31'::date
            )
)

select
    assigned_to,
    month,
    count(distinct case_id) as people_in_safe_house
from people_in_safe_houses_per_month
group by assigned_to, month
order by assigned_to, month