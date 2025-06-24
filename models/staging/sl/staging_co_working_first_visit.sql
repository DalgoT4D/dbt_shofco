{{
    config(
        materialized="table", 
        unique_key="case_id", 
        tags=["co_working_first_visit", "coworking", "sl"]
    )
}}

with source_data as (
    select
        id,
        data
    from {{ source('staging_sl', 'co_working') }}
    WHERE
        data::jsonb ->> 'archived' = 'false' OR data::jsonb ->> 'archived' IS NULL
)

select
    id as case_id,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'sex_csf', '') as sex,
    NULLIF((data::jsonb) -> 'form' -> 'meta_data' ->> 'enumerator', '') as enumerator_full_name,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'is_pwd_csf', '') as is_pwd,

    NULLIF((data::jsonb) -> 'form' ->> 'time_in_csf', '') as time_in,
    NULLIF((data::jsonb) -> 'form' ->>'time_out_csf', '') as time_out,
    
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'id_number_csf', '') as id_number,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'disability_type_csf', '') as disability_type,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'how_many_csf', '') as number_of_visitors,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'is_mother_csf', '') as is_mother,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' -> 'name_of_user_csf' ->> 'first_name_csf', '') as first_name,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'nationality_csf', '') as nationality,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' -> 'name_of_user_csf' ->> 'second_name_csf', '') as second_name,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'age_in_years_csf', '') as age_in_years,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'date_of_birth_csf', '') as date_of_birth,
    NULLIF((data::jsonb) -> 'form' ->> 'date_of_visit_csf', '') as date_of_visit,
    NULLIF((data::jsonb) -> 'form' -> 'meta_data' ->> 'enumerators_first', '') as enumerator_first_name,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' -> 'name_of_user_csf' ->> 'users_fullname_csf', '') as users_fullname,
    NULLIF((data::jsonb) -> 'form' -> 'meta_data' ->> 'enumerator_last-name', '') as enumerator_last_name,
    NULLIF((data::jsonb) -> 'form' ->> 'ward_coworking_site_csf', '') as ward,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'primary_phone_number_csf', '') as primary_phone_number,
    NULLIF((data::jsonb) -> 'form' ->> 'county_coworking_site_csf', '') as county,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'alternative_phone_number_csf', '') as alternative_phone_number,
    NULLIF((data::jsonb) -> 'form' ->> 'coworking_space_facility_cwf', '') as coworking_space_facility,
    NULLIF((data::jsonb) -> 'form' ->> 'subcounty_coworking_site_csf', '') as subcounty,
    NULLIF((data::jsonb) -> 'form' ->> 'specify_training_or_service_csf', '') as training_or_service,
    NULLIF((data::jsonb) -> 'form' -> 'visit_purpose_csf' ->> 'using_coworking_to_earn_a_living_csf', '') as using_coworking_to_earn_a_living,
    NULLIF((data::jsonb) -> 'form' -> 'visit_purpose_csf' ->> 'what_visitor_is_using_csf', '') as purpose_for_using_csf,
    NULLIF((data::jsonb) -> 'form' -> 'visit_purpose_csf' ->> 'specify_other_csf', '') as other_purpose_for_using_csf,
    NULLIF((data::jsonb) -> 'form' ->> 'shofco_training_csf', '') as shofco_training,
    NULLIF((data::jsonb) -> 'form' -> 'case' -> 'meta' ->> 'username', '') as enumerator_username,

    NULLIF((data::jsonb) -> 'form' ->> 'facilitator_csf', '') as facilitator,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'visitors_nationality_csf', '') as visitors_nationality,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'registration_document_csf', '') as registration_document,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' ->> 'asylum_pass_number_csf', '') as asylum_pass_number,
    NULLIF((data::jsonb) -> 'form' -> 'visitor_details_csf' -> 'name_of_user_csf' ->> 'visitors_fullname_label_csf', '') as visitors_fullname_label,
    NULLIF((data::jsonb) -> 'form' -> 'visit_purpose_csf' ->> 'purpose_of_visit_csf', '') as purpose_of_visit

from source_data
