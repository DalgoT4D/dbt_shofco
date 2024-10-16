SELECT
    INITCAP(TRIM("Name")) AS "Name",
    INITCAP(TRIM("County")) AS "County",
    CASE
        WHEN TRIM(LOWER("Gender")) = 'm' THEN 'Male'
        ELSE INITCAP(TRIM("Gender"))
    END AS "Gender",
    "Mobile",
    INITCAP(TRIM("Sub_county")) AS "Sub_county",
    "National_ID",
    INITCAP(TRIM("Community_Role")) AS "Community_Role"

FROM {{ ref('stg_gbv_leaders') }}  -- Refers to the previous model