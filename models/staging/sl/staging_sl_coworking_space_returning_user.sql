{{ config(
    materialized='table',
    tags=['sl', 'co_working_space_returning_user']
) }}

WITH raw_data AS (
    SELECT
        data::jsonb AS form_data,

        -- Meta information
        data::jsonb -> 'form' -> 'meta' ->> 'instanceID' AS form_id,
        data::jsonb -> 'form' -> 'meta' ->> 'username' AS enumerator_username,
        data::jsonb ->> 'received_on' AS submission_date,

        -- Participant metadata
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_fullname' AS participant_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_unique_id' AS unique_id,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_sex' AS sex,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_county' AS county,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_shofco_subcounty' AS subcounty,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_ahofco_ward' AS ward,

        -- Safe cast for age
        CASE
            WHEN NULLIF(data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age', '') ~ '^[0-9]*\.?[0-9]+$'
            THEN FLOOR((data::jsonb -> 'form' -> 'case' -> 'update' ->> 'pp_age')::FLOAT)::INT
            ELSE NULL
        END AS age,

        -- Visit details
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'date_of_visit_csr' AS date_of_visit,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'time_in_csr' AS time_in,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'time_out_csr' AS time_out,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerator' AS enumerator_full_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerators_first' AS enumerator_first_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerator_last-name' AS enumerator_last_name,

        -- Coworking space usage
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'what_visitor_is_using_csr' AS usage_category,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'specific_reason_for_accessing_csr' AS specific_reason,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'using_coworking_for_a_living_csr' AS using_for_living,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'other_reason_csr' AS other_reason

    FROM {{ source('staging_sl', 'Co_working_space__Returning_User_') }}
    WHERE (data::jsonb ->> 'archived') IS NULL OR (data::jsonb ->> 'archived') = 'false'
)

SELECT
    form_id,
    enumerator_username,
    submission_date::timestamp AS submission_date,

    -- Participant info
    participant_name,
    unique_id,
    age,
    sex,
    county,
    subcounty,
    ward,

    -- Visit info
    date_of_visit::date AS date_of_visit,
    time_in,
    time_out,
    enumerator_full_name,
    enumerator_first_name,
    enumerator_last_name,

    -- Coworking usage
    usage_category,
    specific_reason,
    using_for_living,
    other_reason

FROM raw_data
