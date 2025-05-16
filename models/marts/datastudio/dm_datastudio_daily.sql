{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

SELECT 
    -- Table keys
    a.date,
    a.year_week,
    a.year_month,
    a.nr_missed_calls act_nr_missed_calls,
    a.nr_inbound_calls act_nr_inbound_calls,
    a.missed_call_ratio act_missed_call_ratio,
    -- Table values Customer Service
    b.number_of_ratings,
    b.average_rating,
    b.nr_tickets_first_reply_time,
    b.first_reply_time,
    b.nr_missed_calls,
    b.nr_inbound_calls,
    b.missed_call_ratio,
    b.retention_success,
    b.retention_attempts,
    b.nr_tickets_resolution_time,
    b.resolution_time
FROM {{ ref('rpt_activation_daily_kpis') }} a
LEFT JOIN {{ ref('rpt_customer_service_daily_kpis') }} b ON a.date = b.date