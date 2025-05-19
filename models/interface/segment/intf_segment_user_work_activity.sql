SELECT
  a.user_id,
  a.user_email,
  b.vehicle_contract_type,
  b.ridesharing_last_activity,
  c.groceries_last_activity,
  d.parcel_last_activity,
  e.food_delivery_last_activity,
  IFNULL(f.ridesharing_yesterday_net_earnings, 0) AS ridesharing_yesterday_net_earnings, -- Yesterday's net earnings from ridesharing
  IFNULL(f.ridesharing_yesterday_nr_trips, 0) AS ridesharing_yesterday_nr_trips, -- Number of ridesharing trips from yesterday
  IFNULL(f.ridesharing_yesterday_acceptance_rate, 0) AS ridesharing_yesterday_acceptance_rate, -- Acceptance rate for ridesharing trips from yesterday
  IFNULL(f.ridesharing_yesterday_online_hours, 0) AS ridesharing_yesterday_online_hours
FROM {{ ref("dim_users") }} a
LEFT JOIN {{ ref("int_user_ridesharing_last_15_days_activity") }} b ON a.user_id = b.user_id -- Left joining with ridesharing activity table for the last 15 days based on user ID
LEFT JOIN {{ ref("int_user_groceries_last_15_days_activity") }} c ON a.user_id = c.user_id -- Left joining with groceries activity table for the last 15 days based on user ID
LEFT JOIN {{ ref("int_user_parcel_last_15_days_activity") }} d ON a.user_id = d.user_id -- Left joining with parcel service activity table for the last 15 days based on user ID
LEFT JOIN {{ ref("int_user_food_delivery_last_15_days_activity") }} e ON a.user_id = e.user_id -- Left joining with food delivery activity table for the last 15 days based on user ID
LEFT JOIN {{ ref("int_user_ridesharing_yesterday_performance") }} f ON a.user_id = f.user_id -- Left joining with ridesharing performance table for yesterday based on user ID
WHERE (
    b.ridesharing_last_activity IS NOT NULL OR
    c.groceries_last_activity IS NOT NULL OR 
    d.parcel_last_activity IS NOT NULL OR
    e.food_delivery_last_activity IS NOT NULL
)