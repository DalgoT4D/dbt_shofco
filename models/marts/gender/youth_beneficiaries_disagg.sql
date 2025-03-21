{{ config(
  materialized='table',
  tags=['gender_youth_beneficiaries', "gender"]
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
        -- Placeholder for training_type, specific to subsequent data
        NULL AS training_type,
        indexed_on,
        -- Placeholder for parent_case_id, specific to subsequent data
        NULL AS parent_case_id,
        unnest(string_to_array(beneficiary_categories, ' ')) AS category
    FROM {{ ref('staging_youth_beneficiaries') }}
    WHERE training_type IS NULL
),

subsequent_data AS (
    SELECT
        s.case_id,
        coalesce(s.ward, i.ward) AS ward,
        coalesce(s.county, i.county) AS county,
        coalesce(s.constituency, i.constituency) AS constituency,
        coalesce(s.date_of_registration, i.date_of_registration) AS date_of_registration,
        coalesce(s.beneficiary_gender, i.beneficiary_gender) AS beneficiary_gender,
        coalesce(s.name_of_the_beneficiary, i.name_of_the_beneficiary) AS name_of_the_beneficiary,
        coalesce(s.beneficiary_phone_number, i.beneficiary_phone_number) AS beneficiary_phone_number,
        coalesce(s.registered_by, i.registered_by) AS registered_by,
        coalesce(s.beneficiary_categories, i.beneficiary_categories) AS beneficiary_categories,
        coalesce(s.case_name, i.case_name) AS case_name,
        coalesce(s.date_opened, i.date_opened) AS date_opened,
        s.training_type,  -- Training type specifically from subsequent data
        s.indexed_on,
        s.parent_case_id,  -- Keep parent_case_id from subsequent cases
        unnest(string_to_array(coalesce(s.beneficiary_categories, i.beneficiary_categories), ' ')) AS category
    FROM {{ ref('staging_youth_beneficiaries') }} AS s
    LEFT JOIN initial_data AS i ON s.parent_case_id = i.case_id
    WHERE s.training_type IS NOT NULL
)

SELECT 
    case_id,
    ward,
    county,
    constituency,
    {{ validate_date("date_of_registration") }} AS date_of_registration,
    beneficiary_gender,
    name_of_the_beneficiary,
    beneficiary_phone_number,
    registered_by,
    beneficiary_categories,
    case_name,
    {{ validate_date("date_opened") }} AS date_opened,
    training_type,
    {{ validate_date("indexed_on") }} AS indexed_on,
    parent_case_id,
    category
FROM initial_data
UNION ALL
SELECT 
    case_id,
    ward,
    county,
    constituency,
    {{ validate_date("date_of_registration") }} AS date_of_registration,
    beneficiary_gender,
    name_of_the_beneficiary,
    beneficiary_phone_number,
    registered_by,
    beneficiary_categories,
    case_name,
    {{ validate_date("date_opened") }} AS date_opened,
    training_type,
    {{ validate_date("indexed_on") }} AS indexed_on,
    parent_case_id,
    category
FROM subsequent_data
