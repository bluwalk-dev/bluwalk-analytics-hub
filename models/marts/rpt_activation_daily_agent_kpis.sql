WITH date_agents AS (
    SELECT DISTINCT a.date, b.agent_name
    FROM {{ ref('util_calendar') }} a
    CROSS JOIN {{ ref('agg_activation_daily_agent_activation_points') }} b
)

SELECT 
    a.date,
    a.year_week,
    a.year_month,
    b.nr_missed_calls,
    b.nr_inbound_calls,
    b.missed_call_ratio
FROM date_agents a
LEFT JOIN {{ ref('agg_activation_daily_agent_activation_points') }} b ON a.date = b.date AND a.agent_name = b.agent_name
WHERE a.date <= current_date
ORDER BY a.date DESC