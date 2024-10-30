WITH champions_data AS (
    SELECT
        "Site",
        INITCAP(TRIM("County")) AS "County",
        INITCAP(TRIM("Gender")) AS "Gender",  -- Normalizes gender
        "Mobile",
        "Trained",
        "Identified",
        INITCAP(TRIM("Sub_County")) AS "Sub_County",
        INITCAP(TRIM("Sub_County_1")) AS "Sub_County_1",
        "National_ID",
        INITCAP(TRIM("Champions_Name")) AS "Champions_Name",
        INITCAP(TRIM("Community_Role")) AS "Community_Role"
    FROM {{ ref('staging_champions') }}  -- Refers to the previous model
)

SELECT
    *,
    CASE
        -- Both are Yes
        WHEN "Trained" = 'Yes' AND "Identified" = 'Yes' THEN 'Trained and Identified'
        
        -- Only one is Yes
        WHEN "Trained" = 'Yes' THEN 'Trained'
        WHEN "Identified" = 'Yes' THEN 'Identified'
        
        -- Default: none are Yes
        ELSE 'None'
    END AS status
FROM champions_data