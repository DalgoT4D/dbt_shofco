{{ config(
  materialized='table',
  tags=['placement_followup', 'sl', 'documents']
) }}

WITH base AS (
  SELECT
    case_id,
    TRIM(documents_held_pl) AS documents_string
  FROM {{ ref('staging_sl_placement_followup') }}
  WHERE documents_held_pl IS NOT NULL
    AND TRIM(documents_held_pl) <> ''
)

SELECT
  case_id,
  TRIM(document) AS document
FROM base,
LATERAL unnest(string_to_array(documents_string, ' ')) AS document
