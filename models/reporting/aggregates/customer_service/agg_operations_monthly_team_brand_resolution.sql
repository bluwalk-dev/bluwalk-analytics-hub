SELECT
    a.year_month,
    b.brand,
    b.agent_team,
    ROUND(AVG(resolution_time),2) resolution_time,
    COUNT(*) nr_tickets_resolution_time
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('fct_tickets') }} b ON a.date = CAST(b.create_date AS DATE)
WHERE year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
GROUP BY year_month, brand, agent_team
ORDER BY year_month DESC