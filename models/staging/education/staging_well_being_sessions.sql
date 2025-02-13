{{ config(
  materialized='table',
  tags=["education_well_being_sessions", "education"]
) }}

SELECT
    "Date" as date, 
    "Grade" as grade, 
    "Topic" as topic, 
    "School" as school, 
    "Stream" as stream, 
    '1' AS number_of_students_trained,
    'Individual' AS session_type
FROM {{ source('staging_education', 'Individual_Sessions') }}
UNION ALL
SELECT
    "Date" as date,
    "Grade" as grade,
    "Topic" as topic,
    "School" as school,
    "Stream" as stream,
    "Number_of_stdents_trained" as number_of_students_trained,
    'Group' AS session_type
FROM {{ source('staging_education', 'Group_Sessions') }}
