{{ config(
  materialized='table',
  tags=["education_expansion", "education"]
) }}

SELECT
    year,
    cohort,
    county,
    subcounty,
    form,
    term,
    boarding_or_day,
    COALESCE(
        NULLIF(
            CONCAT_WS(
                ', ',
                CASE WHEN teen_mom = 'Yes' THEN 'Teen Mom' END,
                CASE WHEN orphan = 'Yes' THEN 'Orphan' END,
                CASE WHEN special_needs = 'Yes' THEN 'Special Needs' END,
                CASE WHEN gbv_survivor = 'Yes' THEN 'GBV Survivor' END
            ),
            ''
        ),
        'None'
    ) AS categories
FROM {{ ref('staging_scholarships') }}
