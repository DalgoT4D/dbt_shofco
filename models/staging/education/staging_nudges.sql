SELECT
    *
FROM {{ source('staging_education', 'Nudges') }}