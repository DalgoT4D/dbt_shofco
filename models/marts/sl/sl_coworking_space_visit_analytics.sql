{{ 
    config(
        materialized='table',
        alias='sl_coworking_space_visit_analytics',
        tags=['sl', 'coworking_space', 'analytics']
    ) 
}}

WITH source AS (
    SELECT * FROM {{ ref('staging_sl_coworking_space_visit') }}
),

processed AS (
    SELECT
        case_id,
        case_name,
        case_type,
        owner_id,
        external_id,
        officer_name,
        date_opened,
        date_of_visit,
        time_in,
        time_out,
        purpose_of_visit,
        specific_reason_for_accessing_space,
        ward_shofco_site,
        subcounty_shofco_site,
        county_shofco_site,

        -- Duration in hours (if time fields are valid)
        CASE 
            WHEN time_in IS NOT NULL AND time_out IS NOT NULL 
            THEN DATE_PART('hour', time_out::time - time_in::time)
                 + DATE_PART('minute', time_out::time - time_in::time)/60.0
            ELSE NULL 
        END AS duration_hours,

        -- Extract year, month, quarter from date_of_visit
        EXTRACT(YEAR FROM date_of_visit::date) AS year_of_visit,
        EXTRACT(MONTH FROM date_of_visit::date) AS month_of_visit,
        EXTRACT(QUARTER FROM date_of_visit::date) AS quarter_of_visit
    FROM source
)

SELECT * FROM processed
