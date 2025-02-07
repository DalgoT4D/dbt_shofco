{{ config(
  materialized='table',
    tags=["education_well_being_sessions", "education"]
) }}

SELECT
    "Grade" AS grade,
    "Topic" AS topic,
    "Stream" AS stream,
    "Session_Type" AS session_type,
    TO_DATE("Date", 'DD/MM/YYYY') AS date,
    LOWER("School") AS school_type,
    CAST(
        "Number_of_stdents_trained" AS INTEGER
    ) AS number_of_students_trained
FROM {{ ref("staging_well_being_sessions") }}
