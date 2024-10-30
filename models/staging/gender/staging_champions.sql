SELECT
    "Site",
    "County",
    "Gender",
    "Mobile",
    "Trained",
    "Identified",
    "Sub_County",
    "Sub_County_1",
    "National_ID",
    "Champions_Name",
    "Community_Role"
FROM {{ source('staging_gender', 'Champions_') }}