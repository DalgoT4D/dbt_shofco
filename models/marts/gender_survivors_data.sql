with survivors_data as
(
    SELECT
    case_id,
    CASE 
        WHEN date_of_birth IS NULL THEN
            CASE 
                WHEN what_is_the_age_provided IS NOT NULL 
                    THEN what_is_the_age_provided::int
                ELSE
                    CASE 
                        WHEN (what_is_the_year_of_birth_of_survivor IS NOT NULL AND LENGTH(what_is_the_year_of_birth_of_survivor)>=4)
                            THEN EXTRACT(year FROM age(TO_DATE(what_is_the_year_of_birth_of_survivor, 'YYYY')))::int
                        ELSE NULL
                    END
            END
        ELSE EXTRACT(year FROM age(current_date,date_of_birth::date)) :: int 
    END AS age,

    {{ validate_date('date_of_registration') }} as date_of_registration,
    gender, 
    gender_site_code_of_registration,
    county AS county_code, 
    constituency as constituency_code,
    village as village_name,
    ward as ward_code, 
    case_type, 
    {{ validate_date('date_opened') }} as date_opened,  
    indexed_on, 
    closed, 
    {{ validate_date('created_at') }} as created_at
    FROM {{ ref('stg_gender_survivors_commcare')}}
)

SELECT a.*,
COALESCE(b.county_name, a.county_code) as county_name,
COALESCE(b.constituency_name,a.constituency_code) as constituency_name,
COALESCE(b.ward_name,a.ward_code) as ward_name,
c.site_name as gender_site_name_of_registration
FROM survivors_data a
LEFT JOIN {{ source('transformed_schema_db','dim_location_administrative_units') }} b
ON a.county_code = b.county_code
AND a.constituency_code = b.constituency_id
AND a.ward_code = b.ward_id
LEFT JOIN {{ source('transformed_schema_db','dim_gender_sites') }} c
ON a.gender_site_code_of_registration = c.site_code