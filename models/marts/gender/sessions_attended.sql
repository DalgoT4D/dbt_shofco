{{ config(
  materialized='table',
  tags=['gender_mental_health_assesment', "gender"]
) }}

WITH final_assessment_data AS (
    SELECT
        case_id,
        sessions_attended,
        date_of_final_assessment,
        session_id
    FROM {{ ref('staging_gender_final_mental_health_assesment') }}
    WHERE sessions_attended IS NOT NULL
),

initial_assessment_data AS (
    SELECT
        case_id,
        county,
        village,
        gender_site_code
    FROM {{ ref('staging_gender_initial_mental_health_assesment') }}
)

SELECT
    f.case_id,
    f.sessions_attended,
    {{ validate_date("f.date_of_final_assessment") }} AS date_of_final_assessment,
    f.session_id,
    i.county,
    i.village,
    i.gender_site_code
FROM final_assessment_data AS f
LEFT JOIN initial_assessment_data AS i
    ON f.case_id = i.case_id
