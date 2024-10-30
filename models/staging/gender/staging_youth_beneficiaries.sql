SELECT
    -- Extract fields from the JSON 'properties' key in the 'data' column
    (data::json->>'case_id') AS case_id,
    (data::json->'properties'->>'ward') AS ward,
    (data::json->'properties'->>'county') AS county,
    (data::json->'properties'->>'constituency') AS constituency,
    (data::json->'properties'->>'date_of_registration') AS date_of_registration,
    (data::json->'properties'->>'beneficiary_gender') AS beneficiary_gender,
    (data::json->'properties'->>'name_of_the_beneficiary') AS name_of_the_beneficiary,
    (data::json->'properties'->>'beneficiary_phone_number') AS beneficiary_phone_number,
    (data::json->'properties'->>'registered_by') AS registered_by,
    (data::json->'properties'->>'beneficiary_categories') AS beneficiary_categories,
    (data::json->'properties'->>'case_name') AS case_name,
    (data::json->'properties'->>'date_opened') AS date_opened,
    -- Reference indexed_on directly
    indexed_on
FROM {{ source('staging_youth', 'zzz_case') }}