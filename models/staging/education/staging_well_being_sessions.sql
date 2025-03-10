{{ config(
  materialized='table',
  tags=["education_well_being_sessions", "education"]
) }}

SELECT
    "Date" AS date, 
    "Grade" AS grade, 
    "Topic" AS topic, 
    "School" AS school, 
    "Stream" AS stream, 
    '1' AS number_of_students_trained,
    'Individual' AS session_type
FROM {{ source('staging_education', 'Individual_Sessions') }}
UNION ALL
SELECT
    "Date" AS date,
    "Grade" AS grade,
    "Topic" AS topic,
    "School" AS school,
    "Stream" AS stream,
    "Number_of_stdents_trained" AS number_of_students_trained,
    'Group' AS session_type
FROM {{ source('staging_education', 'Group_Sessions') }}
