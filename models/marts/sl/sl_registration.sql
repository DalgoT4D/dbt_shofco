{{ config(
    materialized = 'table',
    tags = ['sl', 'registration']
) }}

WITH base AS (
    SELECT
        id,
        case_id,
        user_id,

        -- Safe casting for timestamp (PostgreSQL doesn't support TRY_CAST)
        CASE 
            WHEN received_on ~ '^\d{4}-\d{2}-\d{2}' THEN received_on::TIMESTAMP 
            ELSE NULL 
        END AS received_on,

        -- Safe casting for boolean
        CASE 
            WHEN LOWER(initial_processing_complete) IN ('true', 'yes', '1') THEN TRUE
            WHEN LOWER(initial_processing_complete) IN ('false', 'no', '0') THEN FALSE
            ELSE NULL
        END AS initial_processing_complete,

        -- Dates and personal details
        NULLIF(date_of_birth, '')::DATE AS date_of_birth,
        NULLIF(registration_date, '')::DATE AS registration_date,
        INITCAP(NULLIF(full_name, '')) AS full_name,
        INITCAP(NULLIF(first_name, '')) AS first_name,
        INITCAP(NULLIF(last_name, '')) AS last_name,

        -- Numeric and categorical values
        CASE
            WHEN NULLIF(age::TEXT, '') IS NULL THEN NULL
            WHEN age::TEXT ~ '^\d+$' THEN age::INT
            ELSE NULL
        END AS age,
        INITCAP(NULLIF(sex, '')) AS sex,
        INITCAP(NULLIF(citizenship, '')) AS citizenship,
        INITCAP(NULLIF(service_of_interest, '')) AS service_of_interest,
        NULLIF(phone_number, '') AS phone_number,
        NULLIF(id_number, '') AS id_number,

        -- Enumerator info
        INITCAP(NULLIF(enumerator_full_name, '')) AS enumerator_full_name,
        INITCAP(NULLIF(enumerator_first, '')) AS enumerator_first,
        INITCAP(NULLIF(enumerator_last, '')) AS enumerator_last,

        -- Geographic (ensure blanks become NULL)
        INITCAP(NULLIF(ward, '')) AS ward,
        INITCAP(NULLIF(subcounty, '')) AS subcounty,
        INITCAP(NULLIF(county, '')) AS county,

        -- PWD & metadata
        CASE
            WHEN LOWER(is_pwd) IN ('yes', 'true', 'y') THEN 'Yes'
            WHEN LOWER(is_pwd) IN ('no', 'false', 'n') THEN 'No'
            ELSE NULL
        END AS is_pwd,

        username

    FROM {{ ref('staging_register') }}
)

SELECT * FROM base
