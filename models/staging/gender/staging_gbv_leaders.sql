SELECT
    "GBV_Leader_Name",
    "County",
    "Gender",
    "Mobile",
    "Sub_county",
    "National_ID",
    "Community_Role"
FROM {{ source('staging_gender', 'GBV_Community_Leaders') }}