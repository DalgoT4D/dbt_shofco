-- Calculate average satisfaction per person, considering only non-null scores
WITH avg_satisfaction_per_person AS (
    SELECT
        date_of_visit,
        overall_satisfaction -- Now using the overall satisfaction score from the new JSON
    FROM {{ ref('stg_gender_satisfaction_scores') }}
    WHERE overall_satisfaction IS NOT NULL
),

-- Calculate average satisfaction per month
monthly_avg_satisfaction AS (
    SELECT
        TO_CHAR(TO_DATE(date_of_visit, 'YYYY-MM-DD'), 'YYYY-MM') AS month,
        ROUND(AVG(overall_satisfaction), 2) AS avg_monthly_satisfaction
    FROM avg_satisfaction_per_person
    GROUP BY TO_CHAR(TO_DATE(date_of_visit, 'YYYY-MM-DD'), 'YYYY-MM')
)

-- Final output for Superset visualization
SELECT
    month,
    avg_monthly_satisfaction
FROM monthly_avg_satisfaction
ORDER BY month