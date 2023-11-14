SELECT
    b.date,
    a.owner_name agent_name,
    SUM(activation_point_score) activation_points
FROM {{ ref("fct_deals_all") }} a
LEFT JOIN {{ ref("util_calendar") }} b ON CAST(a.close_date AS DATE) = b.date
WHERE 
    is_closed_won IS TRUE AND 
    a.close_date IS NOT NULL
GROUP BY b.date, a.owner_name
ORDER BY b.date DESC