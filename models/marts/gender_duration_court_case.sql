SELECT
        case_id,
        -- Explicitly casting to DATE
        CAST({{ validate_date("case_intake_date") }} AS DATE) AS case_intake_date,
        CAST({{ validate_date("date_of_case_closure") }} AS DATE) AS date_of_case_closure,
        CAST({{ validate_date("date_of_court_followup") }} AS DATE) AS date_of_court_followup,
        is_the_case_proceeding_to_court,
        CASE 
            -- For closed cases, calculate duration between intake and closure with explicit date casting
            WHEN {{ validate_date("date_of_case_closure") }} IS NOT NULL THEN 
                (CAST({{ validate_date("date_of_case_closure") }} AS DATE) - CAST({{ validate_date("case_intake_date") }} AS DATE))
            -- For ongoing cases, calculate duration from intake to today with explicit date casting
            ELSE 
                (current_date - CAST({{ validate_date("case_intake_date") }} AS DATE))
        END AS case_duration_in_days
    FROM {{ ref('stg_gender_case_occurrences_commcare') }}
    WHERE 
        {{ validate_date("case_intake_date") }} IS NOT NULL