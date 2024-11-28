SELECT 
    a.year_month,
    d.brand,
    d.team,
    b.number_of_ratings,
    b.average_rating,
    g.number_of_tickets nr_created_tickets,
    c.nr_tickets_first_reply_time,
    c.first_reply_time,
    h.missed_inbound,
    h.total_inbound,
    d.missed_call_ratio,
    f.resolution_time
FROM {{ ref('util_month_intervals') }} a
LEFT JOIN {{ ref('agg_operations_monthly_team_brand_missed_call_ratio') }} d ON a.year_month = d.year_month
LEFT JOIN {{ ref('agg_operations_monthly_team_brand_average_rating') }} b ON a.year_month = b.year_month AND d.brand = b.brand AND d.team = b.agent_team
LEFT JOIN {{ ref('agg_operations_monthly_team_brand_first_reply') }} c ON a.year_month = c.year_month AND d.brand = c.brand AND d.team = c.agent_team
LEFT JOIN {{ ref('agg_operations_monthly_team_brand_resolution') }} f ON a.year_month = f.year_month AND d.brand = f.brand AND d.team = f.agent_team
LEFT JOIN {{ ref('agg_operations_monthly_team_brand_created_tickets') }} g ON a.year_month = g.year_month AND d.brand = g.brand AND d.team = g.agent_team
LEFT JOIN {{ ref('agg_operations_monthly_team_brand_inbound_calls') }} h ON a.year_month = h.year_month AND d.brand = h.brand AND d.team = h.team
WHERE 
    a.year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
ORDER BY a.year_month DESC