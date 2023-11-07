SELECT
    a.year_month,
    ROUND(AVG(first_reply_time),2) first_reply_time,
    COUNT(*) nr_tickets_first_reply_time
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('fct_tickets') }} b ON a.date = CAST(b.create_date AS DATE)
WHERE agent_team = 'Customer Service'
GROUP BY year_month
ORDER BY year_month DESC