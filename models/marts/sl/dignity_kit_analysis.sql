{{ config(materialized='table', tags=['sl', 'dignity_kit']) }}

with src as (
    select * from {{ ref('staging_sl_dignity_kit_criteria') }}
),
scored as (
    select
        case_id,
        beneficiary_name,
        case when nullif(trim(beneficiary_name), '') is not null then beneficiary_name else case_name end as display_name,
        gender,
        county,
        subcounty,
        ward,
        telephone_number,
        national_id_number,
        date_opened,
        -- derive age: prefer numeric text, else compute from dob
        case
            when age_text ~ '^[0-9]+(\\.[0-9]+)?$' then age_text::numeric
            else null
        end as age_reported,
        dob,
        case
            when dob is not null then date_part('year', age(current_date, dob))
            else
                case
                    when age_text ~ '^[0-9]+(\\.[0-9]+)?$' then age_text::numeric
                    else null
                end
        end as age_years,
        parent_flag,
        pwd_flag,
        head_of_household,
        hh_members_iga,
        meals_reduction_freq,
        forego_basic_essentials,
        support_from_shofco,
        coalesce(refugee_type, '') as refugee_type,
        activity,
        intervention,
        name_of_tvet,
        total_score_reported,
        -- scoring
        case when lower(trim(pwd_flag)) = 'yes' then 6 else 0 end as disability_score,
        case when lower(trim(head_of_household)) = 'self' then 1 else 0 end as head_of_household_score,
        case when lower(trim(gender)) = 'female' then 1 else 0 end as gender_score,
        case when lower(trim(hh_members_iga)) in ('all', 'all_or_most', 'all or most') then 0 else 1 end as household_iga_score,
        case when lower(trim(meals_reduction_freq)) in ('every_day','everyday','pretty_often','everyday_or_pretty_often','everyday or pretty often') then 1 else 0 end as reduce_meals_score,
        case when lower(trim(forego_basic_essentials)) in ('every_day','everyday','pretty_often','everyday_or_pretty_often','everyday or pretty often') then 1 else 0 end as forego_essentials_score,
        case when lower(trim(support_from_shofco)) = 'no' then 1 else 0 end as support_from_shofco_score,
        case when lower(trim(refugee_type)) in ('yes','refugee') or refugee_type is not null then 1 else 0 end as refugee_score,
        case when lower(trim(parent_flag)) = 'yes' then 1 else 0 end as children_score
    from src
),
age_filtered as (
    select *
    from scored
    where age_years between 18 and 35
),
deduped as (
    select
        *,
        row_number() over (
            partition by
                coalesce(lower(trim(display_name)), ''),
                coalesce(national_id_number, ''),
                coalesce(telephone_number, '')
            order by date_opened desc nulls last, case_id desc
        ) as rn
    from age_filtered
    where coalesce(trim(display_name), '') <> ''
)

select
    case_id,
    beneficiary_name,
    display_name,
    gender,
    county,
    subcounty,
    ward,
    telephone_number,
    national_id_number,
    date_opened,
    age_reported,
    dob,
    age_years,
    parent_flag,
    pwd_flag,
    head_of_household,
    hh_members_iga,
    meals_reduction_freq,
    forego_basic_essentials,
    support_from_shofco,
    refugee_type,
    null::text as type_of_disability_dir,
    activity,
    intervention,
    name_of_tvet,
    disability_score,
    head_of_household_score,
    gender_score,
    household_iga_score,
    reduce_meals_score,
    forego_essentials_score,
    support_from_shofco_score,
    refugee_score,
    children_score,
    total_score_reported,
    disability_score
    + head_of_household_score
    + gender_score
    + household_iga_score
    + reduce_meals_score
    + forego_essentials_score
    + support_from_shofco_score
    + refugee_score
    + children_score as total_score_computed,
    coalesce(total_score_reported,
             disability_score
             + head_of_household_score
             + gender_score
             + household_iga_score
             + reduce_meals_score
             + forego_essentials_score
             + support_from_shofco_score
             + refugee_score
             + children_score) as total_score
from deduped
where rn = 1
