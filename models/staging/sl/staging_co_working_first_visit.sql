{{
    config(
        materialized="table", 
        unique_key="case_id", 
        tags=["commcare_extraction","co_working_first_visit","sl"]
    )
}}

with source_data as (
    select
        id,
        data
    from {{ source('staging_sl', 'co_working') }}
)

select
    id as case_id,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'sex_csf', '') as sex_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'enumerator', '') as enumerator,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'is_pwd_csf', '') as is_pwd_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'time_in_csf', '') as time_in_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'time_out_csf', '') as time_out_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'id_number_csf', '') as id_number_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'is_mother_csf', '') as is_mother_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'first_name_csf', '') as first_name_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'nationality_csf', '') as nationality_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'second_name_csf', '') as second_name_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'age_in_years_csf', '') as age_in_years_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'date_of_birth_csf', '') as date_of_birth_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'date_of_visit_csf', '') as date_of_visit_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'enumerators_first', '') as enumerators_first,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'users_fullname_csf', '') as users_fullname_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'shofco_training_csf', '') as shofco_training_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'enumerator_last-name', '') as enumerator_last,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'ward_coworking_site_csf', '') as ward_coworking_site_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'primary_phone_number_csf', '') as primary_phone_number_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'county_coworking_site_csf', '') as county_coworking_site_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'what_visitor_is_using_csf', '') as what_visitor_is_using_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'alternative_phone_number_csf', '') as alternative_phone_number_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'coworking_space_facility_cwf', '') as coworking_space_facility_cwf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'subcounty_coworking_site_csf', '') as subcounty_coworking_site_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'specify_training_or_service_csf', '') as specify_training_or_service_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'update' ->> 'using_coworking_to_earn_a_living_csf', '') as using_coworking_to_earn_a_living_csf,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'meta' ->> 'username', '') as username,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'meta' ->> 'timeStart', '') as timeStart
from source_data