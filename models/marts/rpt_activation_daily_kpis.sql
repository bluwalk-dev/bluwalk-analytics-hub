SELECT 
    a.date,
    a.year_week,
    a.year_month,
    b.nr_missed_calls,
    b.nr_inbound_calls,
    b.missed_call_ratio
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('agg_activation_daily_missed_call_ratio') }} b ON a.date = b.date
WHERE a.date <= current_date
ORDER BY a.date DESC