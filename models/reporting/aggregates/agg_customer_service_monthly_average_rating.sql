SELECT
    year_month,
    AVG(normalized_score) average_rating,
    COUNT(*) number_of_ratings
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('fct_tickets_ratings') }} b ON a.date = CAST(b.date_time AS DATE)
GROUP BY year_month