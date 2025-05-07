{{ 
    config(
        materialized='table',
        schema='intermediate_sl',
        alias='sl_coworking_space_registration_analytics',
        tags=['sl', 'coworking_space', 'registration', 'analytics']
    ) 
}}

WITH source AS (
    SELECT * FROM {{ ref('staging_sl_coworking_space_registration') }}
),

processed AS (
    SELECT
        case_id,
        case_name,
        case_type,
        date_opened,
        owner_id,
        external_id,

        -- Personal Information
        gender,
        is_plwd = 'yes' AS is_plwd,
        is_mother = 'yes' AS is_mother,
        id_number,
        first_name,
        middle_name,
        last_name_surname,
        nationality,
        officer_name,
        date_of_birth,

        -- Contact Info
        primary_phone_number,
        alternative_phone_number,

        -- Derived Fields
        EXTRACT(YEAR FROM date_opened::timestamp) AS year_opened,
        EXTRACT(MONTH FROM date_opened::timestamp) AS month_opened
    FROM source
)

SELECT * FROM processed
