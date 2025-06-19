{{
    config(
        materialized="table",
        unique_key="case_id",
        tags=["co_working_return_visit", "coworking", "sl"]
    )
}}

with source_data as (
    select
        id,
        data
    from {{ source('staging_sl', 'co_working_returning') }}
)

select
    id as case_id,

    -- visitor info
    NULLIF(data::jsonb -> 'form' ->> 'time_in_csr', '') as time_in,
    NULLIF(data::jsonb -> 'form' ->> 'time_out_csr', '') as time_out,
    NULLIF(data::jsonb -> 'form' ->> 'date_of_visit_csr', '') as date_of_visit,
    NULLIF(data::jsonb -> 'form' ->> 'facilitator_csr', '') as facilitator,
    NULLIF(data::jsonb -> 'form' ->> 'this_form_will_capture_data_on_co-working_space_attendance', '') as form_capture_flag,

    -- visit purpose block
    NULLIF(data::jsonb -> 'form' -> 'visit_purpose_csr' ->> 'purpose_of_visit_csr', '') as purpose_of_visit,
    NULLIF(data::jsonb -> 'form' -> 'visit_purpose_csr' ->> 'what_visitor_is_using_csr', '') as what_visitor_is_using,
    NULLIF(data::jsonb -> 'form' -> 'visit_purpose_csr' ->> 'using_coworking_for_a_living_csr', '') as using_coworking_for_living,
    NULLIF(data::jsonb -> 'form' -> 'visit_purpose_csr' ->> 'specific_reason_for_accessing_csr', '') as specific_reason_for_accessing,
    NULLIF(data::jsonb -> 'form' -> 'visit_purpose_csr' ->> 'other_reason_csr', '') as other_reason,

    -- meta data
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'enumerator', '') as enumerator_full_name,
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'enumerators_first', '') as enumerator_first_name,
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'enumerator_last-name', '') as enumerator_last_name,
    NULLIF(data::jsonb -> 'form' -> 'meta' ->> 'username', '') as enumerator_username,

    -- personal profile fields (from meta_data)
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'pp_sex', '') as sex,
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'pp_age', '') as age,
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'pp_fullname', '') as fullname,
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'pp_unique_id', '') as unique_id,
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'pp_ahofco_ward', '') as ward,
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'pp_shofco_subcounty', '') as subcounty,
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'pp_shofco_county', '') as county,
    NULLIF(data::jsonb -> 'form' -> 'meta_data' ->> 'user_location_id', '') as user_location_id,

    -- case
    NULLIF(data::jsonb -> 'form' -> 'case' ->> '@case_id', '') as commcare_case_id

from source_data