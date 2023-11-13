SELECT
    year_quarter,
    year,
    min(date) start_date,
    max(date) end_date,
    CONCAT(
        CAST(EXTRACT(YEAR FROM DATE_SUB(MIN(date), INTERVAL 1 DAY)) AS STRING),
        'Q',
        CAST(EXTRACT(QUARTER FROM DATE_SUB(MIN(date), INTERVAL 1 DAY)) AS STRING)
    ) previous_year_quarter,
    CONCAT(
        CAST(EXTRACT(YEAR FROM DATE_ADD(MAX(date), INTERVAL 1 DAY)) AS STRING),
        'Q',
        CAST(EXTRACT(QUARTER FROM DATE_ADD(MAX(date), INTERVAL 1 DAY)) AS STRING)
    ) next_year_quarter
FROM {{ ref('util_calendar') }} a
GROUP BY year_quarter, year
ORDER BY year_quarter ASC