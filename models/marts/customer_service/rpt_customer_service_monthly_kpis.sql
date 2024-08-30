SELECT 
    a.year_month,
    b.number_of_ratings,
    b.average_rating,
    g.number_of_tickets nr_created_tickets,
    c.nr_tickets_first_reply_time,
    c.first_reply_time,
    d.nr_missed_calls,
    d.nr_inbound_calls,
    d.missed_call_ratio,
    e.retention_success,
    e.retention_attempts,
    f.resolution_time
FROM {{ ref('util_month_intervals') }} a
LEFT JOIN {{ ref('agg_customer_service_monthly_average_rating') }} b ON a.year_month = b.year_month
LEFT JOIN {{ ref('agg_customer_service_monthly_first_reply') }} c ON a.year_month = c.year_month
LEFT JOIN {{ ref('agg_customer_service_monthly_missed_call_ratio') }} d ON a.year_month = d.year_month
LEFT JOIN {{ ref('agg_customer_service_monthly_churn_prevention') }} e ON a.year_month = e.year_month
LEFT JOIN {{ ref('agg_customer_service_monthly_resolution') }} f ON a.year_month = f.year_month
LEFT JOIN {{ ref('agg_customer_service_monthly_created_tickets') }} g ON a.year_month = g.year_month
WHERE a.year_month <= CAST(FORMAT_DATE('%Y%m', CURRENT_DATE()) AS INT64)
ORDER BY a.year_month DESC