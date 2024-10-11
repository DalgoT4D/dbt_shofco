-- Calculate average satisfaction per respondent, considering only non-null scores
WITH avg_satisfaction_per_person AS (
    SELECT
        date_of_visit,
        ROUND(
            (COALESCE(being_treated_with_respect, 0) + COALESCE(staff_sensitivity_to_your_privacy, 0) + COALESCE(friendliness_and_courtesy_of_staff, 0) +
             COALESCE(staff_willingness_to_listen_and_offer_assistance, 0) + COALESCE(staff_conduct_themselves_in_a_professional_manner, 0) +
             COALESCE(time_taken_to_offer_services, 0) + COALESCE(service_staff_were_welcoming, 0))::NUMERIC
            /
            (7 - (CASE WHEN being_treated_with_respect IS NULL THEN 1 ELSE 0 END
                + CASE WHEN staff_sensitivity_to_your_privacy IS NULL THEN 1 ELSE 0 END
                + CASE WHEN friendliness_and_courtesy_of_staff IS NULL THEN 1 ELSE 0 END
                + CASE WHEN staff_willingness_to_listen_and_offer_assistance IS NULL THEN 1 ELSE 0 END
                + CASE WHEN staff_conduct_themselves_in_a_professional_manner IS NULL THEN 1 ELSE 0 END
                + CASE WHEN time_taken_to_offer_services IS NULL THEN 1 ELSE 0 END
                + CASE WHEN service_staff_were_welcoming IS NULL THEN 1 ELSE 0 END)), 2) AS avg_satisfaction
    FROM {{ ref('stg_gender_satisfaction_scores') }}
    WHERE (being_treated_with_respect IS NOT NULL
        OR staff_sensitivity_to_your_privacy IS NOT NULL
        OR friendliness_and_courtesy_of_staff IS NOT NULL
        OR staff_willingness_to_listen_and_offer_assistance IS NOT NULL
        OR staff_conduct_themselves_in_a_professional_manner IS NOT NULL
        OR time_taken_to_offer_services IS NOT NULL
        OR service_staff_were_welcoming IS NOT NULL)
),

-- Calculate average satisfaction per month
monthly_avg_satisfaction AS (
    SELECT
        TO_CHAR(TO_DATE(date_of_visit, 'YYYY-MM-DD'), 'YYYY-MM') AS month,
        ROUND(AVG(avg_satisfaction), 2) AS avg_monthly_satisfaction
    FROM avg_satisfaction_per_person
    GROUP BY TO_CHAR(TO_DATE(date_of_visit, 'YYYY-MM-DD'), 'YYYY-MM')
)

-- Final output for Superset visualization
SELECT
    month,
    avg_monthly_satisfaction
FROM monthly_avg_satisfaction
ORDER BY month