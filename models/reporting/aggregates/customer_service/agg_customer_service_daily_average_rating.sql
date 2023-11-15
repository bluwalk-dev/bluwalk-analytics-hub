SELECT
    CAST(date_time AS DATE) date,
    ROUND(AVG(normalized_score),4) average_rating,
    COUNT(*) number_of_ratings
FROM {{ ref('fct_tickets_ratings') }} l
GROUP BY date
ORDER BY date DESC