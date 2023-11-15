SELECT
    b.date,
    a.owner_name sales_agent_name,
    a.owner_team sales_agent_team,
    SUM(activation_point_score) activation_points
FROM {{ ref("fct_deals_all") }} a
LEFT JOIN {{ ref("util_calendar") }} b ON CAST(a.close_date AS DATE) = b.date
WHERE 
    is_closed_won IS TRUE AND 
    a.close_date IS NOT NULL
GROUP BY b.date, sales_agent_name, sales_agent_team
ORDER BY b.date DESC