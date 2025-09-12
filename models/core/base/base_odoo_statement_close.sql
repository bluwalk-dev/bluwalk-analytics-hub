WITH statement_log AS (
    SELECT
        CAST(date AS DATE) date,
        CAST(period AS INT64) statement,
        CAST(lastUpdate AS TIMESTAMP) last_update_timestamp,
        DATETIME(TIMESTAMP(lastUpdate), 'Europe/Lisbon') as last_update_localtime
    FROM {{ source('generic', 'close_period_log') }}

    UNION ALL

    SELECT
        CAST(dbt_valid_from AS DATE) date,
        period statement,
        CAST(write_date AS TIMESTAMP) last_update_timestamp,
        DATETIME(TIMESTAMP(write_date), 'Europe/Lisbon') last_update_localtime
    FROM bluwalk-analytics-hub.core.core_finances_payment_cycle_close
)


select
    c.year_week,
    c.year_month,
    c.date,
    max(last_update_localtime) as statement_close
FROM statement_log cr
LEFT JOIN bluwalk-analytics-hub.core.ref_calendar_weeks we ON cr.statement = we.year_week
LEFT JOIN bluwalk-analytics-hub.core.ref_calendar c ON we.end_date = c.date
GROUP BY date, year_week, year_month
ORDER BY year_week DESC