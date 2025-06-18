{{ config(
  materialized='table',
  tags=['upskilling', 'sl', 'certificates']
) }}

WITH base AS (
  SELECT
    case_id,
    TRIM(educational_certificates) AS certificate_string
  FROM {{ ref('upskilling') }}
  WHERE educational_certificates IS NOT NULL AND TRIM(educational_certificates) <> ''
)

SELECT
  case_id,
  TRIM(certificate) AS certificate
FROM base,
LATERAL unnest(string_to_array(certificate_string, ' ')) AS certificate

