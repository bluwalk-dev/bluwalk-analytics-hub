WITH date_agents AS (
    SELECT DISTINCT a.date, a.year_week, a.year_month, b.sales_agent_name, b.sales_agent_team
    FROM {{ ref('util_calendar') }} a
    CROSS JOIN {{ ref('agg_activation_daily_agent_activation_points') }} b
)

SELECT 
    a.date,
    a.year_week,
    a.year_month,
    a.sales_agent_name agent_name,
    a.sales_agent_team agent_team,
    IFNULL(b.activation_points,0) activation_points
FROM date_agents a
LEFT JOIN {{ ref('agg_activation_daily_agent_activation_points') }} b ON a.date = b.date AND a.sales_agent_name = b.sales_agent_name AND a.sales_agent_team = b.sales_agent_team
WHERE a.date <= current_date
ORDER BY a.date DESC, agent_name ASC