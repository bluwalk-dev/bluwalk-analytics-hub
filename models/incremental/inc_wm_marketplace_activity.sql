-- This query selects various user activities and performance metrics from related tables
SELECT
  a.user_id, -- Unique identifier for the user from the dim_users table
  a.user_email, -- User's email address
  b.vehicle_contract_type, -- Type of vehicle contract the user has (from ridesharing activity data)
  b.ridesharing_last_activity, -- The last activity date for ridesharing
  c.groceries_last_activity, -- The last activity date for grocery services
  d.parcel_last_activity, -- The last activity date for parcel services
  e.food_delivery_last_activity, -- The last activity date for food delivery services
  -- Using IFNULL to ensure that there are no NULL values, replacing them with 0 if necessary:
  IFNULL(f.ridesharing_yesterday_net_earnings, 0) AS ridesharing_yesterday_net_earnings, -- Yesterday's net earnings from ridesharing
  IFNULL(f.ridesharing_yesterday_nr_trips, 0) AS ridesharing_yesterday_nr_trips, -- Number of ridesharing trips from yesterday
  IFNULL(f.ridesharing_yesterday_acceptance_rate, 0) AS ridesharing_yesterday_acceptance_rate, -- Acceptance rate for ridesharing trips from yesterday
  IFNULL(f.ridesharing_yesterday_online_hours, 0) AS ridesharing_yesterday_online_hours
FROM {{ ref("dim_users") }} a -- Reference to the 'dim_users' table which holds user dimension data
LEFT JOIN {{ ref("int_user_ridesharing_last_15_days_activity") }} b ON a.user_id = b.user_id -- Left joining with ridesharing activity table for the last 15 days based on user ID
LEFT JOIN {{ ref("int_user_groceries_last_15_days_activity") }} c ON a.user_id = c.user_id -- Left joining with groceries activity table for the last 15 days based on user ID
LEFT JOIN {{ ref("int_user_parcel_last_15_days_activity") }} d ON a.user_id = d.user_id -- Left joining with parcel service activity table for the last 15 days based on user ID
LEFT JOIN {{ ref("int_user_food_delivery_last_15_days_activity") }} e ON a.user_id = e.user_id -- Left joining with food delivery activity table for the last 15 days based on user ID
LEFT JOIN {{ ref("int_user_ridesharing_yesterday_performance") }} f ON a.user_id = f.user_id -- Left joining with ridesharing performance table for yesterday based on user ID
WHERE (
    -- Filtering out users who have not had any activity in the last 15 days across all services
    b.ridesharing_last_activity IS NOT NULL OR
    c.groceries_last_activity IS NOT NULL OR 
    d.parcel_last_activity IS NOT NULL OR
    e.food_delivery_last_activity IS NOT NULL
)