SELECT 
    year_week,
    ROUND(AVG(DATETIME_DIFF(statement_close, date, HOUR) - 24),2) release_delay
FROM (
    select
        c.year_week,
        c.year_month,
        c.date,
        max(last_update_localtime) as statement_close
    FROM {{ ref('stg_log__statement_close') }} cr
    LEFT JOIN {{ ref('util_week_intervals') }} we ON cr.statement = we.year_week
    LEFT JOIN {{ ref('util_calendar') }} c ON we.end_date = c.date
    GROUP BY date, year_week, year_month
    ORDER BY year_week DESC
    )
GROUP BY year_week
ORDER BY year_week DESC