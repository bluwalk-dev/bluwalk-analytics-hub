SELECT
    CAST(date_time AS DATE) date,
    agent_name,
    AVG(normalized_score) average_rating,
    COUNT(*) number_of_ratings
FROM {{ ref('fct_tickets_ratings') }} l
GROUP BY date, agent_name
ORDER BY date DESC