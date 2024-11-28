SELECT
    year_month,
    ROUND(AVG(normalized_score),4) average_rating,
    COUNT(*) number_of_ratings
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('fct_tickets_ratings') }} b ON a.date = CAST(b.date_time AS DATE)
WHERE year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
GROUP BY year_month
ORDER BY year_month DESC