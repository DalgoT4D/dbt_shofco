{{ config(
  materialized='table'
) }}

SELECT
      "County",
      "Gender", 
      "National_ID",
      "Site",
      "Trained",
      "Identified",
      "Date_identified",
      "Date_trained",
      "Active",
    CASE
        -- Both are Yes
        WHEN "Trained" = 'Yes' AND "Identified" = 'Yes' THEN 'Trained and Identified'
        
        -- Only one is Yes
        WHEN "Trained" = 'Yes' THEN 'Trained'
        WHEN "Identified" = 'Yes' THEN 'Identified'
        
        -- Default: none are Yes
        ELSE 'None'
    END AS status,
    
    CASE
        WHEN "Date_trained" IS NOT NULL THEN 'Quarter ' || EXTRACT(QUARTER FROM "Date_trained")
        ELSE 'Unknown'
    END AS "quarter_trained",
    
    CASE
        WHEN "Date_identified" IS NOT NULL THEN 'Quarter ' || EXTRACT(QUARTER FROM "Date_identified")
        ELSE 'Unknown'
    END AS "quarter_identified"

FROM {{ ref('staging_champions') }}
