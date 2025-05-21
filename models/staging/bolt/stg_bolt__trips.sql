SELECT
  CAST(company_id   AS INT64)   AS bolt_company_id,
  CAST(date         AS DATE)    AS date,
  CAST(order_id     AS INT64)   AS order_id,
  driver_first_name,
  driver_last_name,
  CONCAT(driver_first_name, ' ', driver_last_name) AS driver_name,
  CAST(driver_id    AS STRING)  AS partner_account_uuid,
  pickup_address,
  pickup_arrived_at,
  pickup_departed_at,
  payment_method,
  ride_distance,
  ride_distance_unit,
  tip,
  total_price,
  currency,
  driver_assigned_time,
  accepted_time,
  DATETIME(accepted_time, 'Europe/Lisbon') AS accepted_time_local,
  order_state,
  driver_rating,
  drop_off_address,
  drop_off_time,
  DATETIME(drop_off_time, 'Europe/Lisbon') AS drop_off_time_local,
  search_category,
  REPLACE(car_reg_number, '-', '') AS vehicle_plate,
  TIMESTAMP_MILLIS(load_timestamp) AS load_timestamp,
  driver_phone
FROM {{ source('bolt','trips_history') }}
QUALIFY
  ROW_NUMBER()
    OVER (
      PARTITION BY company_id, DATE, order_id
      ORDER BY load_timestamp DESC
    ) = 1