{{ config(
    materialized='table',
    unique_key='case_id',
    tags=['co_working', 'prod', 'individual_profile', "sl", "sl_marts"]
) }}

SELECT
    case_id,
    first_name AS first_name,
    second_name AS second_name,
    pp_fullname AS users_fullname,
    visitor_fullname_csf AS visitors_fullname_label,
    sex AS sex,  -- Using coalesced field
    CASE WHEN age ~ '^[0-9]+$' THEN age::INT ELSE NULL END AS age_in_years,  -- Using coalesced field
    date_of_birth AS date_of_birth,
    is_pwd AS is_pwd,
    disability_type_csf AS disability_type,
    is_mother_csf AS is_mother,
    nationality AS nationality,
    visitors_nationality_csf AS visitors_nationality,
    registration_document AS registration_document,
    id_number_csf AS id_number,
    asylum_pass_number_csf AS asylum_pass_number,
    primary_phone_number AS primary_phone_number,
    NULL AS alternative_phone_number, -- alternative_phone_number_csf is empty in source

    -- Enumerator metadata
    enumerator AS enumerator_full_name,
    enumerator_first_Name AS enumerator_first_name,  -- Using coalesced field
    enumerator_last_name AS enumerator_last_name,  -- Using coalesced field
    user_id AS enumerator_username,

    -- Location info
    ward_coworking_site_csf AS ward,
    subcounty_coworking_site_csf AS subcounty,
    county_coworking_site_csf AS county,
    NULL AS coworking_space_facility,

    -- Visit metadata (for profile alignment)
    {{ validate_date('date_of_visit_csf') }} AS date_of_visit,
    time_in_csf AS time_in,
    time_out_csf AS time_out

FROM {{ ref('staging_sl_case_table') }}
WHERE date_of_visit_csf IS NOT NULL