{{ config(
  materialized='table',
  tags=["gender_gbv_leaders_and_champions","gender"]
) }}

SELECT
    national_id,
    active,
    county,
    gender,
    trained,
    identified,
    {{ validate_date("date_identified") }} AS date_identified,
    {{ validate_date ("date_trained") }} AS date_trained,
    CASE
        -- Both are Yes
        WHEN
            "trained" = 'Yes' AND "identified" = 'Yes'
            THEN 'Trained and Identified'

        -- Only one is Yes
        WHEN "trained" = 'yes' THEN 'Trained'
        WHEN "identified" = 'yes' THEN 'Identified'

        -- Default: none are Yes
        ELSE 'None'
    END AS status,

    CASE
        WHEN
            "date_trained" IS NOT NULL
            THEN 'Quarter ' || EXTRACT(QUARTER FROM {{validate_date("date_trained") }} )
        ELSE 'Unknown'
    END AS "quarter_trained",
    
    CASE
        WHEN
            "date_identified" IS NOT NULL
            THEN 'Quarter ' || EXTRACT(QUARTER FROM {{validate_date("date_identified") }} )
        ELSE 'Unknown'
    END AS quarter_identified

FROM {{ ref('staging_gbv_leaders') }}
