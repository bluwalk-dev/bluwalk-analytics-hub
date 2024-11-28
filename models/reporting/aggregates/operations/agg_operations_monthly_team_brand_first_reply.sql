SELECT
    a.year_month,
    b.brand,
    b.agent_team,
    ROUND(AVG(first_reply_time),2) first_reply_time,
    COUNT(*) nr_tickets_first_reply_time
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('fct_tickets') }} b ON a.date = CAST(b.create_date AS DATE)
WHERE year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
GROUP BY year_month, brand, agent_team