{{ config(
  materialized='table',
  tags="education_well_being_sessions"
) }}

SELECT
    "Date",
    "Grade",
    "Topic",
    "School",
    "Stream", 
    '1' AS "Number_of_stdents_trained",
    'Individual' AS "Session_Type"
FROM {{ source('staging_education', 'Individual_Sessions') }}
UNION ALL
SELECT
    "Date",
    "Grade",
    "Topic",
    "School",
    "Stream",
    "Number_of_stdents_trained",
    'Group' AS "Session_Type"
FROM {{ source('staging_education', 'Group_Sessions') }}
