{{ config(
  materialized='table',
  tags=["ciff"]
) }}

SELECT
    _indicators_ AS indicators,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_cummulative_targets_y_1_to_y_5_, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_cummulative_targets_y_1_to_y_5_, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_cummulative_targets_y_1_to_y_5_, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS cumulative_targets_y1_to_y5,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_percentage_achievement, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) = '' THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(_percentage_achievement, '$', ''), '%', ''), ' ', ''), '-', '')) = '-' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) AS NUMERIC) / 100.0
    END AS percentage_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_cumulative_ytd_achievement_, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_cumulative_ytd_achievement_, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_cumulative_ytd_achievement_, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS cumulative_ytd_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_1_annual_target, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_1_annual_target, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_1_annual_target, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS y1_annual_target,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_1_ytd_achievement, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_1_ytd_achievement, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_1_ytd_achievement, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS y1_ytd_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_1_percentage_achievement, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_1_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) = '' THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(_y_1_percentage_achievement, '$', ''), '%', ''), ' ', ''), '-', '')) = '-' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_1_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) AS NUMERIC) / 100.0
    END AS y1_percentage_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_2_annual_target, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_2_annual_target, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_2_annual_target, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS y2_annual_target,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_2_ytd_achievement_, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_2_ytd_achievement_, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_2_ytd_achievement_, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS y2_ytd_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_2_percentage_achievement, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_2_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) = '' THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(_y_2_percentage_achievement, '$', ''), '%', ''), ' ', ''), '-', '')) = '-' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_2_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) AS NUMERIC) / 100.0
    END AS y2_percentage_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_3_annual_targets, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_3_annual_targets, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_3_annual_targets, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS y3_annual_targets,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_3_ytd_achievement, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_3_ytd_achievement, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_3_ytd_achievement, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS y3_ytd_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_3_percentage_achievement, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_3_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) = '' THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(_y_3_percentage_achievement, '$', ''), '%', ''), ' ', ''), '-', '')) = '-' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_3_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) AS NUMERIC) / 100.0
    END AS y3_percentage_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_4_annual_targets, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_4_annual_targets, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_4_annual_targets, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS y4_annual_targets,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_4_ytd_achievement, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_4_ytd_achievement, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_4_ytd_achievement, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS y4_ytd_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_4_percentage_achievement, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_4_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) = '' THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(_y_4_percentage_achievement, '$', ''), '%', ''), ' ', ''), '-', '')) = '-' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_4_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) AS NUMERIC) / 100.0
    END AS y4_percentage_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_5_annual_targets, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_5_annual_targets, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_5_annual_targets, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS y5_annual_targets,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_5_ytd_achievement, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_5_ytd_achievement, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_5_ytd_achievement, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS y5_ytd_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_5_percentage_achievement, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_5_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) = '' THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(_y_5_percentage_achievement, '$', ''), '%', ''), ' ', ''), '-', '')) = '-' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_5_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) AS NUMERIC) / 100.0
    END AS y5_percentage_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_6_annual_targets, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_6_annual_targets, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_6_annual_targets, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS y6_annual_targets,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_6_ytd_achievement, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_6_ytd_achievement, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_6_ytd_achievement, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS y6_ytd_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_y_6_percentage_achievement, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_6_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) = '' THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(_y_6_percentage_achievement, '$', ''), '%', ''), ' ', ''), '-', '')) = '-' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_y_6_percentage_achievement, '$', ''), '%', ''), ',', ''), ' ', ''), '-', '')) AS NUMERIC) / 100.0
    END AS y6_percentage_achievement,
    CASE 
        WHEN NULLIF(NULLIF(NULLIF(_total_targets, '#DIV/0!'), ''), '-') IS NULL THEN NULL
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_total_targets, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) = '' THEN NULL
        ELSE CAST(TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(_total_targets, '$', ''), ',', ''), ' ', ''), '#DIV/0!', ''), '-', '')) AS NUMERIC)
    END AS total_targets
FROM {{ source('staging_ciff', 'Ciff') }}
