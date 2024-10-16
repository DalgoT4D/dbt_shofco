SELECT
    "Site",
    "County",
    "Gender",
    "Mobile",
    "Engaged",
    "Trained",
    "Identified",
    "Sub_County",
    "Sub_County_1",
    "National_ID",
    "Champions_Name",
    "Community_Role"
FROM {{ source('source_commcare', 'Champions_') }}