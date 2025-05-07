{{
    config(
        materialized='table',
        alias='sl_swep_production_analytics',
        tags=['sl', 'swep', 'analytics']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('staging_sl_swep_production') }}
),

enriched_data AS (
    SELECT
        case_id,
        case_name,
        case_type,
        date_opened,
        first_name,
        middle_name,
        last_name,
        gender,
        age::int,
        date_of_birth::date,
        national_id_number,
        non_citizen_id_number,
        registration_document,
        nationality,
        nationality_if_other,
        non_citizen_nationality,
        is_refugee,
        refugee_type,
        asylum_pass_number,
        phone_number,
        alt_phone_number,
        education_level,
        is_mother,
        baby_below_four_years,
        referral,
        referral_source,
        village,
        ward,
        sub_county,
        county,
        shofco_ward,
        shofco_subcounty,
        shofco_county,
        registration_date::date,

        -- Age Group
        CASE 
            WHEN age::int < 18 THEN 'Under 18'
            WHEN age::int BETWEEN 18 AND 24 THEN '18-24'
            WHEN age::int BETWEEN 25 AND 35 THEN '25-35'
            WHEN age::int > 35 THEN '36+'
            ELSE 'Unknown'
        END AS age_group,

        -- Gender Category
        CASE 
            WHEN gender ILIKE 'female' THEN 'Female'
            WHEN gender ILIKE 'male' THEN 'Male'
            ELSE 'Other/Unspecified'
        END AS gender_category,

        -- Refugee Classification
        CASE
            WHEN is_refugee ILIKE '%refugee%' THEN 'Refugee'
            WHEN is_refugee ILIKE '%host%' THEN 'Host Community'
            ELSE 'Unspecified'
        END AS refugee_classification
    FROM source
)

SELECT * FROM enriched_data
