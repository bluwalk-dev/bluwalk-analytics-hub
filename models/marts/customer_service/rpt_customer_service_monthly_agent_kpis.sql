WITH month_agents AS (
    SELECT DISTINCT year_month, agent_name
    FROM {{ ref('agg_customer_service_monthly_agent_first_reply') }}
)

SELECT
    a.year_month,
    a.agent_name,
    b.number_of_ratings,
    b.average_rating,
    c.nr_tickets_first_reply_time,
    c.first_reply_time,
    d.nr_missed_calls,
    d.nr_inbound_calls,
    d.missed_call_ratio,
    f.resolution_time
FROM month_agents a
LEFT JOIN {{ ref('agg_customer_service_monthly_agent_average_rating') }} b ON a.year_month = b.year_month AND a.agent_name = b.agent_name
LEFT JOIN {{ ref('agg_customer_service_monthly_agent_first_reply') }} c ON a.year_month = c.year_month AND a.agent_name = c.agent_name
LEFT JOIN {{ ref('agg_customer_service_monthly_missed_call_ratio') }} d ON a.year_month = d.year_month
LEFT JOIN {{ ref('agg_customer_service_monthly_agent_resolution') }} f ON a.year_month = f.year_month AND a.agent_name = f.agent_name
WHERE 
    a.year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64) AND
    (b.number_of_ratings > 0 OR
    c.nr_tickets_first_reply_time > 0 OR
    b.average_rating > 0
    )
    
ORDER BY a.year_month DESC