WITH champions_data AS (
    SELECT
        "Site",
        INITCAP(TRIM("County")) AS "County",
        INITCAP(TRIM("Gender")) AS "Gender",  -- Normalizes gender
        "Mobile",
        "Engaged",
        "Trained",
        "Identified",
        INITCAP(TRIM("Sub_County")) AS "Sub_County",
        INITCAP(TRIM("Sub_County_1")) AS "Sub_County_1",
        "National_ID",
        INITCAP(TRIM("Champions_Name")) AS "Champions_Name",
        INITCAP(TRIM("Community_Role")) AS "Community_Role"
    FROM {{ ref('stg_champions') }}  -- Refers to the previous model
)

SELECT
    *,
    CASE
        -- All three are Yes
        WHEN "Engaged" = 'Yes' AND "Trained" = 'Yes' AND "Identified" = 'Yes' THEN 'Identified, Trained and Engaged'
        
        -- Combinations of two
        WHEN "Engaged" = 'Yes' AND "Trained" = 'Yes' THEN 'Trained and Engaged'
        WHEN "Engaged" = 'Yes' AND "Identified" = 'Yes' THEN 'Identified and Engaged'
        WHEN "Trained" = 'Yes' AND "Identified" = 'Yes' THEN 'Trained and Identified'
        
        -- Only one is Yes
        WHEN "Engaged" = 'Yes' THEN 'Engaged'
        WHEN "Trained" = 'Yes' THEN 'Trained'
        WHEN "Identified" = 'Yes' THEN 'Identified'
        
        -- Default: none are Yes
        ELSE 'None'
    END AS status
FROM champions_data