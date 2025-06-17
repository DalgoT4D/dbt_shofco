{{ config(
  materialized = 'table',
  tags = ['sl', 'registration']
) }}

WITH register_data AS (
    SELECT
        id,
        data::jsonb -> 'form' -> 'case' ->> '@case_id' AS case_id,
        data::jsonb -> 'form' -> 'case' ->> '@user_id' AS user_id,
        data::jsonb ->> 'received_on' AS received_on,
        data::jsonb ->> 'initial_processing_complete' AS initial_processing_complete,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'date_of_birth_dir' AS date_of_birth,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'date_of_registration_dir' AS registration_date,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'participant_full-name_dir' AS full_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'first_name_dir' AS first_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'second_name_dir' AS last_name,
        (data::jsonb -> 'form' -> 'case' -> 'update' ->> 'age_in_years_dir')::int AS age,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'sex_dir' AS sex,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'citizenship_dir' AS citizenship,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'service_of_interest_dir' AS service_of_interest,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'primary_phone_number_dir' AS phone_number,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'national_id_number_dir' AS id_number,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerator' AS enumerator_full_name,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerators_first' AS enumerator_first,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'enumerator_last-name' AS enumerator_last,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'ward_dir' AS ward,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'subcounty_shofco_site_dir' AS subcounty,
        data::jsonb -> 'form' -> 'case' -> 'update' ->> 'county_dir' AS county,
        data::jsonb -> 'metadata' ->> 'username' AS username
    FROM {{ source('staging_sl', 'Register') }}
    WHERE data::jsonb ->> 'archived' = 'false' OR data::jsonb ->> 'archived' IS NULL
)

SELECT DISTINCT
    id,
    case_id,
    user_id,
    received_on,
    initial_processing_complete,
    date_of_birth,
    registration_date,
    full_name,
    first_name,
    last_name,
    age,
    sex,
    citizenship,
    service_of_interest,
    phone_number,
    id_number,
    enumerator_full_name,
    enumerator_first,
    enumerator_last,
    ward,
    subcounty,
    county,
    username
FROM register_data
