{{ config(
  materialized='table'
) }}

SELECT
    "Term" as "term",
    "Year" as "year",
    "Cohort" as "cohort",
    "Home_County" as "county",
    "Home_Sub_County" as "subcounty",
    "Form__current_" as "form",
    "Boarding_OR_Day",
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