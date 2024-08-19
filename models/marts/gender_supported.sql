

with case_occurrences_data as (
    SELECT
	parent_case_id,
    case_id,
	{{ validate_date('date_of_reporting') }} as date_of_reporting, 
	{{ validate_date('date_of_case_closure') }} as date_of_case_closure 
    
FROM {{ref('stg_gender_case_occurrences_commcare')}}
),

 date_range AS (
    -- Generate a series of months covering the period of interest
    SELECT
        date_trunc('month', generate_series(
            (SELECT MIN(date_of_reporting) FROM case_occurrences_data), -- Earliest date_of_reporting
            COALESCE(
                (SELECT MAX(date_of_case_closure) FROM case_occurrences_data), -- Latest date_of_case_closure
                CURRENT_DATE
            ),
            interval '1 month'
        )) AS month
),

cases_expanded AS (
    SELECT
        c.parent_case_id,
        c.date_of_reporting,
        c.date_of_case_closure,
        dr.month
    FROM
        case_occurrences_data c
    CROSS JOIN date_range dr
    WHERE
        -- Check if the case is open during the given month
        c.date_of_reporting <= (dr.month + interval '1 month' - interval '1 day')
        AND (c.date_of_case_closure >= dr.month OR c.date_of_case_closure IS NULL)
)

SELECT
    month,
    parent_case_id,
    CASE
        WHEN c.date_of_reporting <= (month + interval '1 month' - interval '1 day')
             AND (c.date_of_case_closure >= month OR c.date_of_case_closure IS NULL)
        THEN 1
        ELSE 0
    END AS is_case_open
FROM
    cases_expanded c
ORDER BY
    parent_case_id,
    month