SELECT
    CAST(create_date AS DATE) date,
    ROUND(AVG(resolution_time),2) resolution_time,
    COUNT(*) nr_tickets_resolution_time
FROM {{ ref('fct_tickets') }}
WHERE agent_team = 'Customer Service'
GROUP BY date
ORDER BY date DESC