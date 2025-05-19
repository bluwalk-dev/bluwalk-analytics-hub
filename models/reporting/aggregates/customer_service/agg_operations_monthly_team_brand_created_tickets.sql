SELECT
    a.year_month,
    b.brand,
    b.agent_team,
    COUNT(*) number_of_tickets
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('fct_tickets') }} b ON a.date = CAST(b.create_date AS DATE)
WHERE year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
GROUP BY year_month, agent_team, brand