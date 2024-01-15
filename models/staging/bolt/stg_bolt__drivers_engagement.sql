SELECT
    CAST(company_id AS INT64) as bolt_company_id,
    CAST(date AS date) as date,
    CAST(driver_id AS STRING) as partner_account_uuid,
    CAST(first_name AS STRING) driver_first_name,
    CAST(last_name AS STRING) driver_last_name,
    CAST(full_name AS STRING) driver_name,
    CAST(status AS STRING) status,
    CAST(accepts_cash AS BOOLEAN) accepts_cash,
    CAST(driver_score AS INTEGER) driver_score,
    CAST(acceptance_rate AS NUMERIC)/100 acceptance_rate,
    CAST(acceptance_rate_ex_optional AS NUMERIC)/100 acceptance_rate_ex_optional,
    CAST(online_minutes AS INTEGER) online_minutes,
    CAST(utilization_percent AS NUMERIC)/100 utilization_percent,
    CAST(completed_orders AS INTEGER) completed_orders,
    CAST(completion_rate_percent AS NUMERIC)/100 completion_rate_percent,
    CAST(finished_rate_percent AS NUMERIC)/100 finished_rate_percent, 
    CAST(average_ride_distance_meters AS NUMERIC) average_ride_distance_meters,
    CAST(average_driver_rating AS NUMERIC) average_driver_rating,
    CAST(has_minimum_order_minutes AS BOOLEAN) has_minimum_order_minutes,
    CAST(waiting_for_orders_minutes AS INTEGER) waiting_for_orders_minutes,
    TIMESTAMP_MILLIS(load_timestamp) load_timestamp
FROM {{ source('bolt', 'driver_engagement') }}
WHERE 
    online_minutes > 0 AND
    completed_orders > 0