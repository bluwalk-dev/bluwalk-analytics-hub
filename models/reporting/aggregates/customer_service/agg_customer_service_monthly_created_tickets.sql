SELECT
    a.year_month,
    COUNT(*) number_of_tickets
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('fct_tickets') }} b ON a.date = CAST(b.create_date AS DATE)
WHERE year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
GROUP BY year_month
ORDER BY year_month DESC