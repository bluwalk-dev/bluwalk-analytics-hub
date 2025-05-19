SELECT
    a.year_month,
    b.team,
    SUM(COALESCE(total_inbound,0)) total_inbound,
    SUM(COALESCE(b.total_valid_inbound, 0)) AS total_valid_inbound,
    SUM(COALESCE(b.missed_inbound, 0)) AS missed_inbound
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('agg_operations_daily_team_inbound_calls') }} b ON a.date = b.date
WHERE a.date <= CURRENT_DATE()
GROUP BY year_month, team
ORDER BY year_month DESC