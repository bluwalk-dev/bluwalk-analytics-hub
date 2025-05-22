SELECT
    a.date,
    COUNT(b.user_id) AS nr_signups
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('dim_users') }} b ON a.date = CAST(b.create_date AS DATE)
WHERE a.date <= CURRENT_DATE
GROUP BY a.date
ORDER BY a.date DESC