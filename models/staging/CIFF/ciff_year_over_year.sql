{{ config(
  materialized='table',
  tags=["ciff"]
) }}

WITH base_data AS (
  SELECT * FROM {{ ref('ciff_dashboard') }}
),

unpivoted AS (
  SELECT indicators, 1 as year_num, 'Year 1' as year_label, y1_annual_target as annual_target, y1_ytd_achievement as ytd_achievement, y1_percentage_achievement as percentage_achievement
  FROM base_data
  UNION ALL
  SELECT indicators, 2, 'Year 2', y2_annual_target, y2_ytd_achievement, y2_percentage_achievement
  FROM base_data
  UNION ALL
  SELECT indicators, 3, 'Year 3', y3_annual_targets, y3_ytd_achievement, y3_percentage_achievement
  FROM base_data
  UNION ALL
  SELECT indicators, 4, 'Year 4', y4_annual_targets, y4_ytd_achievement, y4_percentage_achievement
  FROM base_data
  UNION ALL
  SELECT indicators, 5, 'Year 5', y5_annual_targets, y5_ytd_achievement, y5_percentage_achievement
  FROM base_data
  UNION ALL
  SELECT indicators, 6, 'Year 6', y6_annual_targets, y6_ytd_achievement, y6_percentage_achievement
  FROM base_data
),

final AS (
  SELECT 
    indicators,
    year_num,
    year_label,
    annual_target,
    ytd_achievement,
    percentage_achievement
  FROM unpivoted
)

SELECT 
  indicators,
  year_num,
  year_label,
  annual_target,
  ytd_achievement,
  percentage_achievement
FROM final
ORDER BY indicators, year_num