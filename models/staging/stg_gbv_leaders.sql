SELECT
    "Name",
    "County",
    "Gender",
    "Mobile",
    "Sub_county",
    "National_ID",
    "Community_Role"
FROM {{ source('source_commcare', 'GBV_Community_Leaders') }}