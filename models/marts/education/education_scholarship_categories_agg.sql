{{ config(
  materialized='table',
  tags=["education_expansion", "education"]
) }}

WITH cleaned_staging_scholarships as (
SELECT
    year,
    cohort,
    initcap(trim(' ' from county)) as county,
    translate(initcap(trim(' ' from subcounty)),'_,-', ' ') as subcounty,
    form,
    term,
    initcap(trim(' ' from boarding_or_day)) as boarding_or_day,
    teen_mom,
    orphan,
    special_needs,
    gbv_survivor
FROM {{ ref('staging_scholarships') }} 

)

SELECT
    year,
    cohort,
    COALESCE(NULLIF(county, ''), 'Unknown') AS county,
    COALESCE(NULLIF(subcounty, ''), 'Unknown') AS subcounty,
    form,
    term,
    COALESCE(NULLIF(boarding_or_day, ''), 'Unknown') AS boarding_or_day,
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
FROM cleaned_staging_scholarships
