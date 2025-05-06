{{
    config(
        materialized='table',
        schema='intermediate_sl',
        alias='sl_service_registration_analytics',
        tags=['sl', 'service_registration', 'analytics']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('staging_sl_service_registration') }}
),

enriched AS (
    SELECT
        *,
        -- Age group logic if age or date_of_birth is valid
        CASE 
            WHEN date_of_birth IS NOT NULL THEN
                DATE_PART('year', AGE(date_of_birth::date))::int
            ELSE NULL
        END AS age,

        CASE 
            WHEN date_of_birth IS NOT NULL THEN
                CASE 
                    WHEN DATE_PART('year', AGE(date_of_birth::date)) < 18 THEN 'Under 18'
                    WHEN DATE_PART('year', AGE(date_of_birth::date)) BETWEEN 18 AND 24 THEN '18-24'
                    WHEN DATE_PART('year', AGE(date_of_birth::date)) BETWEEN 25 AND 35 THEN '25-35'
                    ELSE '36+'
                END
            ELSE 'Unknown'
        END AS age_group,

        -- Gender category
        CASE 
            WHEN gender ILIKE 'female' THEN 'Female'
            WHEN gender ILIKE 'male' THEN 'Male'
            ELSE 'Other/Unspecified'
        END AS gender_category,

        -- Employment status simplification
        CASE
            WHEN is_employed = 'yes' THEN 'Employed'
            WHEN is_employed = 'no' THEN 'Unemployed'
            ELSE 'Unknown'
        END AS employment_status,

        -- Refugee status
        CASE
            WHEN is_refugee ILIKE '%refugee%' THEN 'Refugee'
            WHEN is_refugee ILIKE '%host%' THEN 'Host Community'
            ELSE 'Unspecified'
        END AS refugee_classification
    FROM source
)

SELECT * FROM enriched
