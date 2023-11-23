SELECT
    year_month,
    year,
    year_quarter,
    min(date) start_date,
    max(date) end_date,
    CAST(format_timestamp('%Y%m', DATE_SUB(MIN(date), INTERVAL 1 DAY)) AS INT64) previous_year_month,
    CAST(format_timestamp('%Y%m', DATE_ADD(MAX(date), INTERVAL 1 DAY)) AS INT64) next_year_month
FROM {{ ref('util_calendar') }} a
GROUP BY year_month, year, year_quarter
ORDER BY year_month ASC