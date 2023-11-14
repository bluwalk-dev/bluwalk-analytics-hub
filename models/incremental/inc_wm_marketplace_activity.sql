SELECT
  a.user_id,
  a.email user_email,
  b.vehicle_contract_type,
  b.ridesharing_last_activity,
  c.groceries_last_activity,
  d.parcel_last_activity,
  e.food_delivery_last_activity,
  IFNULL(f.ridesharing_yesterday_net_earnings, 0) AS ridesharing_yesterday_net_earnings,
  IFNULL(f.ridesharing_yesterday_nr_trips, 0) AS ridesharing_yesterday_nr_trips,
  IFNULL(f.ridesharing_yesterday_acceptance_rate, 0) AS ridesharing_yesterday_acceptance_rate
FROM {{ ref("dim_users") }} a
LEFT JOIN {{ ref("int_user_ridesharing_last_15_days_activity") }} b ON a.user_id = b.user_id
LEFT JOIN {{ ref("int_user_groceries_last_15_days_activity") }} c ON a.user_id = c.user_id
LEFT JOIN {{ ref("int_user_parcel_last_15_days_activity") }} d ON a.user_id = d.user_id
LEFT JOIN {{ ref("int_user_food_delivery_last_15_days_activity") }} e ON a.user_id = e.user_id
LEFT JOIN {{ ref("int_user_ridesharing_yesterday_performance") }} f ON a.user_id = f.user_id
WHERE (
    b.ridesharing_last_activity IS NOT NULL OR
    c.groceries_last_activity IS NOT NULL OR 
    d.parcel_last_activity IS NOT NULL OR
    e.food_delivery_last_activity IS NOT NULL
    )