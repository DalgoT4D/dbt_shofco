{{ config(
  materialized='table',
    tags="education_well_being_sessions"
) }}

SELECT  
    TO_DATE("Date", 'DD/MM/YYYY') AS date,           
    "Grade" AS grade,          
    "Topic" AS topic,               
    LOWER("School") AS school_type,             
    "Stream" AS stream,              
    CAST("Number_of_stdents_trained" AS INTEGER) AS number_of_students_trained, 
    "Session_Type" AS session_type   
FROM {{ ref("staging_well_being_sessions") }}
