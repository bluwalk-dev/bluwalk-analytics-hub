SELECT
    year_week,
    min(date) start_date,
    max(date) end_date
FROM {{ ref('util_calendar') }}
GROUP BY year_week
ORDER BY year_week ASC