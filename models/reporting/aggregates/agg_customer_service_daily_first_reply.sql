SELECT
    CAST(create_date AS DATE) date,
    AVG(first_reply_time) first_reply_time,
    COUNT(*) nr_tickets_first_reply_time
FROM {{ ref('fct_tickets') }}
WHERE agent_team = 'Customer Service'
GROUP BY date
ORDER BY date DESC