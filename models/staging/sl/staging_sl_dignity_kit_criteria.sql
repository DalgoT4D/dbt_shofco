{{ 
    config(
        materialized='table',
        tags=['staging', 'sl_staging', 'dignity_kit'],
        unique_key='case_id'
    ) 
}}

with raw_cases as (
    select
        data ->> 'case_id' as case_id,
        data -> 'properties' ->> 'date_opened' as date_opened_raw,
        data -> 'properties' ->> 'case_name' as case_name,
        data -> 'properties' ->> 'Beneficiary_Name' as beneficiary_name,
        data -> 'properties' ->> 'Age' as age_text,
        data -> 'properties' ->> 'DOB' as dob_text,
        data -> 'properties' ->> 'gender' as gender,
        data -> 'properties' ->> 'county' as county,
        data -> 'properties' ->> 'Sub-county' as subcounty,
        data -> 'properties' ->> 'ward' as ward,
        data -> 'properties' ->> 'telephone_number' as telephone_number,
        data -> 'properties' ->> 'national_id_number' as national_id_number,
        data -> 'properties' ->> 'Parent' as parent_flag,
        data -> 'properties' ->> 'PWD' as pwd_flag,
        data -> 'properties' ->> 'Head_of_the_HH' as head_of_household,
        data -> 'properties' ->> 'HH_members_engaged_in_income_generating_activity' as hh_members_iga,
        data -> 'properties' ->> 'Oftenness_in_the_reduction_of_meals_in_the_last_3_months' as meals_reduction_freq,
        data -> 'properties' ->> 'Forego_basic_essentials' as forego_basic_essentials,
        data -> 'properties' ->> 'Last_3_months_received_or_receiving_support_from_SHOFCO' as support_from_shofco,
        data -> 'properties' ->> 'Intervention_they_are_under' as intervention,
        data -> 'properties' ->> 'Activity' as activity,
        data -> 'properties' ->> 'Name_of_TVET' as name_of_tvet,
        data -> 'properties' ->> 'gender_of_hh_head' as gender_of_hh_head,
        data -> 'properties' ->> 'participants_refugee_type_dir' as participants_refugee_type_dir,
        data -> 'properties' ->> 'refugee_type_dir' as refugee_type_dir,
        data -> 'properties' ->> 'overal_total_score_analysis' as total_score_raw
    from {{ source('staging_sl', 'zzz_case') }}
    where data -> 'properties' ->> 'case_type' = 'Dignity_Kit_Criteria'
),
prepared as (
    select
        case_id,
        {{ validate_date('date_opened_raw') }} as date_opened,
        case_name,
        beneficiary_name,
        age_text,
        {{ validate_date('dob_text') }} as dob,
        gender,
        county,
        subcounty,
        ward,
        telephone_number,
        national_id_number,
        parent_flag,
        pwd_flag,
        head_of_household,
        hh_members_iga,
        meals_reduction_freq,
        forego_basic_essentials,
        support_from_shofco,
        intervention,
        activity,
        name_of_tvet,
        gender_of_hh_head,
        coalesce(participants_refugee_type_dir, refugee_type_dir) as refugee_type,
        case
            when total_score_raw ~ '^[0-9]+(\\.[0-9]+)?$' then total_score_raw::numeric
            else null
        end as total_score_reported
    from raw_cases
)

select * from prepared
