{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

SELECT 
    a.date,
    a.year_week,
    a.year_month,

    -- Customer Service
    b.number_of_ratings,
    b.average_rating,
    c.nr_tickets_first_reply_time,
    c.first_reply_time,
    d.nr_missed_calls,
    d.nr_inbound_calls,
    d.missed_call_ratio,
    e.nr_tickets_resolution_time,
    e.resolution_time
    
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('agg_customer_service_daily_average_rating') }} b ON a.date = b.date
LEFT JOIN {{ ref('agg_customer_service_daily_first_reply') }} c ON a.date = c.date
LEFT JOIN {{ ref('agg_customer_service_daily_missed_call_ratio') }} d ON a.date = d.date
LEFT JOIN {{ ref('agg_customer_service_daily_resolution') }} e ON a.date = e.date
WHERE 
  a.date > '2023-01-01' AND
  a.date < current_date()
ORDER BY a.date DESC