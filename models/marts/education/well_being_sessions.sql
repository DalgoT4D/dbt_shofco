{{ config(
  materialized='table',
    tags="education_well_being_sessions"
) }}

SELECT  
    TO_DATE("Date", 'DD/MM/YYYY') AS "Date",           
    "Grade" AS grade,          
    "Topic",               
    LOWER("School") AS school_type,             
    "Stream",              
    CAST("Number_of_stdents_trained" AS INTEGER) AS "Number_of_stdents_trained", 
    "Session_Type"    
FROM {{ ref("staging_well_being_sessions") }}
