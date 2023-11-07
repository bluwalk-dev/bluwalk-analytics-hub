SELECT
    year_month,
    min(date) start_date,
    max(date) end_date
FROM {{ ref('util_calendar') }}
GROUP BY year_month
ORDER BY year_month ASC