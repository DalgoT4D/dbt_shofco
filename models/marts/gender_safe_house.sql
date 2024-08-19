

with case_occurrences_data as (
    SELECT
	case_id,
	{{ validate_date('date_of_safehouse_onboarding') }} as date_of_safe_house_onboarding, 
	{{ validate_date('date_of_discharge') }} as date_of_safe_house_discharge 
    
FROM {{ref('stg_gender_safe_house_commcare')}}
),

 case_dates AS (
    -- Extract the minimum and maximum dates from the cases table if start_date or end_date is null
    SELECT
        MIN(date_of_safe_house_onboarding) AS earliest_open_date,
        MAX(COALESCE(date_of_safe_house_discharge, CURRENT_DATE)) AS latest_close_date
    FROM
        case_occurrences_data
),

date_range AS (
    -- Generate the date range using either the provided start/end dates or the full range in the dataset if null
    SELECT
        date_trunc('month', generate_series(
              
                cd.earliest_open_date,
            
                cd.latest_close_date
            ,
            interval '1 month'
        )) AS month
    FROM
        case_dates cd
),


people_in_safe_houses_per_month AS (
    SELECT
        dr.month,
        c.case_id
    FROM 
        date_range dr
    LEFT JOIN 
       case_occurrences_data c
    ON 
        dr.month BETWEEN date_trunc('month', c.date_of_safe_house_onboarding) AND COALESCE(date_trunc('month', c.date_of_safe_house_discharge), '2999-12-31'::date)
)


SELECT
    month:date,
    COUNT(DISTINCT case_id) AS people_in_safe_house
FROM
    people_in_safe_houses_per_month
GROUP BY
    month
ORDER BY
    month