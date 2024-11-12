{{ config(
  materialized='table'
) }}

SELECT
    _airbyte_raw_id AS id, -- Include the primary key or unique identifier for each row if needed
    "Cohort",
    COALESCE(
        NULLIF(
            CONCAT_WS(', ',
                CASE WHEN "Teen_Mom_" = 'Yes' THEN 'Teen Mom' END,
                CASE WHEN "Orphan_" = 'Yes' THEN 'Orphan' END,
                CASE WHEN "Special_needs_" = 'Yes' THEN 'Special Needs' END,
                CASE WHEN "GBV_survivor_" = 'Yes' THEN 'GBV Survivor' END
            ),
            ''
        ),
        'None'
    ) AS categories
FROM {{ source('staging_education', 'Scholarships') }}