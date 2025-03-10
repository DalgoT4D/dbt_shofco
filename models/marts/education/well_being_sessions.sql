{{ config(
  materialized='table',
    tags=["education_well_being_sessions", "education"]
) }}

SELECT  
    {{ validate_date("date") }} AS date,           
    grade,          
    topic,               
    LOWER(school) AS school_type,             
    stream,              
    CAST(number_of_students_trained AS INTEGER) AS number_of_students_trained, 
    session_type
FROM {{ ref("staging_well_being_sessions") }}
