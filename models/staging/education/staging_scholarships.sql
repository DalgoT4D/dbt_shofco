SELECT
    *
FROM {{ source('staging_education', 'Scholarships') }}