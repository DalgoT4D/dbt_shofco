{{ config(
    materialized='table',
    unique_key='case_id',
    tags=['co_working', 'prod', 'individual_profile']
) }}

SELECT
    case_id,
    first_name,
    second_name,
    users_fullname,
    visitors_fullname_label,
    sex,
    age_in_years,
    date_of_birth,
    is_pwd,
    disability_type,
    is_mother,
    nationality,
    visitors_nationality,
    registration_document,
    id_number,
    asylum_pass_number,
    primary_phone_number,
    alternative_phone_number,

    -- Enumerator metadata
    enumerator_full_name,
    enumerator_first_name,
    enumerator_last_name,
    enumerator_username,

    -- Location info
    ward,
    subcounty,
    county,
    coworking_space_facility,

    -- Visit metadata (for profile alignment)
    date_of_visit,
    time_in,
    time_out

FROM {{ ref('staging_co_working_first_visit') }}