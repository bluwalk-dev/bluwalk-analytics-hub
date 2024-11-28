SELECT
    a.year_month,
    agent_name,
    ROUND(AVG(resolution_time),2) resolution_time,
    COUNT(*) nr_tickets_resolution_time
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('fct_tickets') }} b ON a.date = CAST(b.create_date AS DATE)
WHERE agent_team = 'Customer Service'
GROUP BY year_month, agent_name