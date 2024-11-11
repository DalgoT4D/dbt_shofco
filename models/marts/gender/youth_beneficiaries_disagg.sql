{{ config(
  materialized='table'
) }}

WITH initial_data AS (
    SELECT
        case_id,
        ward,
        county,
        constituency,
        date_of_registration,
        beneficiary_gender,
        name_of_the_beneficiary,
        beneficiary_phone_number,
        registered_by,
        beneficiary_categories,
        case_name,
        date_opened,
        NULL AS training_type,  -- Placeholder for training_type, specific to subsequent data
        indexed_on,
        NULL AS parent_case_id,  -- Placeholder for parent_case_id, specific to subsequent data
        unnest(string_to_array(beneficiary_categories, ' ')) AS category
    FROM {{ ref('staging_youth_beneficiaries') }}
    WHERE training_type IS NULL
),

subsequent_data AS (
    SELECT
        s.case_id,
        COALESCE(s.ward, i.ward) AS ward,
        COALESCE(s.county, i.county) AS county,
        COALESCE(s.constituency, i.constituency) AS constituency,
        COALESCE(s.date_of_registration, i.date_of_registration) AS date_of_registration,
        COALESCE(s.beneficiary_gender, i.beneficiary_gender) AS beneficiary_gender,
        COALESCE(s.name_of_the_beneficiary, i.name_of_the_beneficiary) AS name_of_the_beneficiary,
        COALESCE(s.beneficiary_phone_number, i.beneficiary_phone_number) AS beneficiary_phone_number,
        COALESCE(s.registered_by, i.registered_by) AS registered_by,
        COALESCE(s.beneficiary_categories, i.beneficiary_categories) AS beneficiary_categories,
        COALESCE(s.case_name, i.case_name) AS case_name,
        COALESCE(s.date_opened, i.date_opened) AS date_opened,
        s.training_type,  -- Training type specifically from subsequent data
        s.indexed_on,
        s.parent_case_id,  -- Keep parent_case_id from subsequent cases
        unnest(string_to_array(COALESCE(s.beneficiary_categories, i.beneficiary_categories), ' ')) AS category
    FROM {{ ref('staging_youth_beneficiaries') }} s
    LEFT JOIN initial_data i ON s.parent_case_id = i.case_id
    WHERE s.training_type IS NOT NULL
)

SELECT * FROM initial_data
UNION ALL
SELECT * FROM subsequent_data