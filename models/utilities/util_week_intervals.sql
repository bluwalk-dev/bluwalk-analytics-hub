SELECT
    year_week,
    max(date) start_date,
    min(date) end_date
FROM {{ ref('util_calendar') }}
GROUP BY year_week
ORDER BY year_week ASC