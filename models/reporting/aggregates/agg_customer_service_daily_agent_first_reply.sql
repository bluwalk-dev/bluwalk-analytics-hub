SELECT
    CAST(create_date AS DATE) date,
    agent_name,
    AVG(first_reply_time) first_reply_time,
    COUNT(*) nr_tickets
FROM {{ ref('fct_tickets') }}
WHERE agent_team = 'Customer Service'
GROUP BY date, agent_name
ORDER BY date DESC