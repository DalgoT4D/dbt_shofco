{{ config(
    materialized='table',
    tags=['sustainable_livelihoods', 'service_completion']
) }}

WITH base AS (
    SELECT
        form_id,
        enumerator_username,
        submission_date::DATE,
        training_activity,
        completed_training,
        helpfulness_rating,
        would_recommend,
        confident_to_find_employment,

        participant_name,
        unique_id,
        age,
        sex,
        county,
        subcounty,
        ward,

        completed_courses,
        dropped_courses,

        -- Safe extraction from text prefix like "2__entrepreneurship"
        CASE 
            WHEN total_courses_completed ~ '^\d+__' THEN CAST(SPLIT_PART(total_courses_completed, '__', 1) AS INTEGER)
            WHEN total_courses_completed ~ '^\d+$' THEN CAST(total_courses_completed AS INTEGER)
            ELSE NULL
        END AS total_courses_completed,

        CASE 
            WHEN total_courses_dropped ~ '^\d+__' THEN CAST(SPLIT_PART(total_courses_dropped, '__', 1) AS INTEGER)
            WHEN total_courses_dropped ~ '^\d+$' THEN CAST(total_courses_dropped AS INTEGER)
            ELSE NULL
        END AS total_courses_dropped,

        -- Age group buckets
        CASE 
            WHEN age <= 17 THEN '<18'
            WHEN age BETWEEN 18 AND 24 THEN '18-24'
            WHEN age BETWEEN 25 AND 30 THEN '25-30'
            WHEN age BETWEEN 31 AND 40 THEN '31-40'
            WHEN age BETWEEN 41 AND 50 THEN '41-50'
            ELSE '51+'
        END AS age_group,

        DATE_TRUNC('month', submission_date) AS training_month,

        -- KPIs
        CASE 
            WHEN (
                (CASE 
                    WHEN total_courses_dropped ~ '^\d+__' THEN CAST(SPLIT_PART(total_courses_dropped, '__', 1) AS NUMERIC)
                    WHEN total_courses_dropped ~ '^\d+$' THEN CAST(total_courses_dropped AS NUMERIC)
                    ELSE 0
                END +
                CASE 
                    WHEN total_courses_completed ~ '^\d+__' THEN CAST(SPLIT_PART(total_courses_completed, '__', 1) AS NUMERIC)
                    WHEN total_courses_completed ~ '^\d+$' THEN CAST(total_courses_completed AS NUMERIC)
                    ELSE 0
                END) > 0
            )
            THEN ROUND(
                1.0 * 
                CASE 
                    WHEN total_courses_dropped ~ '^\d+__' THEN CAST(SPLIT_PART(total_courses_dropped, '__', 1) AS NUMERIC)
                    WHEN total_courses_dropped ~ '^\d+$' THEN CAST(total_courses_dropped AS NUMERIC)
                    ELSE 0
                END /
                (
                    CASE 
                        WHEN total_courses_dropped ~ '^\d+__' THEN CAST(SPLIT_PART(total_courses_dropped, '__', 1) AS NUMERIC)
                        WHEN total_courses_dropped ~ '^\d+$' THEN CAST(total_courses_dropped AS NUMERIC)
                        ELSE 0
                    END +
                    CASE 
                        WHEN total_courses_completed ~ '^\d+__' THEN CAST(SPLIT_PART(total_courses_completed, '__', 1) AS NUMERIC)
                        WHEN total_courses_completed ~ '^\d+$' THEN CAST(total_courses_completed AS NUMERIC)
                        ELSE 0
                    END
                ), 2
            )
            ELSE NULL
        END AS dropout_rate,

        CASE 
            WHEN (
                CASE 
                    WHEN total_courses_dropped ~ '^\d+__' THEN CAST(SPLIT_PART(total_courses_dropped, '__', 1) AS NUMERIC)
                    WHEN total_courses_dropped ~ '^\d+$' THEN CAST(total_courses_dropped AS NUMERIC)
                    ELSE 0
                END
            ) > 0
            THEN ROUND(
                1.0 * 
                CASE 
                    WHEN total_courses_completed ~ '^\d+__' THEN CAST(SPLIT_PART(total_courses_completed, '__', 1) AS NUMERIC)
                    WHEN total_courses_completed ~ '^\d+$' THEN CAST(total_courses_completed AS NUMERIC)
                    ELSE 0
                END /
                CASE 
                    WHEN total_courses_dropped ~ '^\d+__' THEN CAST(SPLIT_PART(total_courses_dropped, '__', 1) AS NUMERIC)
                    WHEN total_courses_dropped ~ '^\d+$' THEN CAST(total_courses_dropped AS NUMERIC)
                    ELSE 0
                END
            , 2)
            ELSE NULL
        END AS completion_to_dropout_ratio

    FROM {{ ref('staging_sl_service_completion_follow_up') }}
)

SELECT *
FROM base
