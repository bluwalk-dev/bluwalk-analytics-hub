{{ config(materialized='table') }}

SELECT 
    -- Table keys
    a.date,
    a.year_week,
    a.year_month,
    -- Table values Marketing
    a.marketing_points,
    a.nr_signups,
    a.new_food_accounts,
    a.new_rideshare_accounts,
    a.new_shopping_accounts,
    a.new_parcel_accounts,
    a.new_total_accounts,
    -- Table values Activation
    b.nr_missed_calls act_nr_missed_calls,
    b.nr_inbound_calls act_nr_inbound_calls,
    b.missed_call_ratio act_missed_call_ratio,
    -- Table values Customer Service
    c.number_of_ratings,
    c.average_rating,
    c.nr_tickets_first_reply_time,
    c.first_reply_time,
    c.nr_missed_calls,
    c.nr_inbound_calls,
    c.missed_call_ratio,
    c.retention_success,
    c.retention_attempts
FROM {{ ref('rpt_marketing_daily_kpis') }} a
LEFT JOIN {{ ref('rpt_activation_daily_kpis') }} b ON a.date = b.date
LEFT JOIN {{ ref('rpt_customer_service_daily_kpis') }} c ON a.date = c.date