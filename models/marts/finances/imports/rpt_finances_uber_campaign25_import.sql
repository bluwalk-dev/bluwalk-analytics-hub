WITH

-- 1) one tiny calendar lookup
calendar AS (
  SELECT
    date,
    year_week
  FROM {{ ref("util_calendar") }}
),

-- 2) trips, with peak/off-peak counted per (contact, week)
trips AS (
  SELECT
    u.contact_id,
    c.year_week,
    SUM(
      CASE
        WHEN (
          (EXTRACT(DAYOFWEEK   FROM u.dropoff_local_time) BETWEEN 2 AND 6
           AND EXTRACT(HOUR     FROM u.dropoff_local_time) BETWEEN 7  AND 9 )
          OR (EXTRACT(DAYOFWEEK FROM u.dropoff_local_time) BETWEEN 2 AND 6
           AND EXTRACT(HOUR     FROM u.dropoff_local_time) BETWEEN 16 AND 18)
          OR (EXTRACT(DAYOFWEEK FROM u.dropoff_local_time) IN (1,7))
          OR (EXTRACT(DAYOFWEEK FROM u.dropoff_local_time) = 6
           AND EXTRACT(HOUR     FROM u.dropoff_local_time) BETWEEN 19 AND 23)
        ) THEN 1 ELSE 0
      END
    ) AS trips_peak,
    SUM(
      CASE
        WHEN (
          (EXTRACT(DAYOFWEEK   FROM u.dropoff_local_time) BETWEEN 2 AND 6
           AND EXTRACT(HOUR     FROM u.dropoff_local_time) BETWEEN 7  AND 9 )
          OR (EXTRACT(DAYOFWEEK FROM u.dropoff_local_time) BETWEEN 2 AND 6
           AND EXTRACT(HOUR     FROM u.dropoff_local_time) BETWEEN 16 AND 18)
          OR (EXTRACT(DAYOFWEEK FROM u.dropoff_local_time) IN (1,7))
          OR (EXTRACT(DAYOFWEEK FROM u.dropoff_local_time) = 6
           AND EXTRACT(HOUR     FROM u.dropoff_local_time) BETWEEN 19 AND 23)
        ) THEN 0 ELSE 1
      END
    ) AS trips_off_peak
  FROM {{ ref("base_uber_trips") }} AS u
  LEFT JOIN {{ ref("dim_vehicles") }} AS v ON u.vehicle_plate = v.vehicle_plate
  JOIN calendar AS c ON CAST(u.dropoff_local_time AS DATE) = c.date
  WHERE v.vehicle_fuel_type = 'electric'
  GROUP BY u.contact_id, c.year_week
),

-- 3) vehicle contract type, grouped once per week/contact
vehicle_contract_type AS (
  SELECT
    cal.year_week,
    s.contact_id,
    MAX(s.vehicle_contract_type) AS vehicle_contract_type
  FROM {{ ref("int_user_service_fee_per_day") }} AS s
  JOIN calendar AS cal ON s.date = cal.date
  GROUP BY cal.year_week, s.contact_id
),

-- 4) all your “user‐paid” fees in one sweep
financials AS (
  SELECT
    COALESCE(w.contact_id, a.contact_id)   AS contact_id,
    a.payment_cycle                       AS year_week,
    - SUM(CASE WHEN a.group_id = 3 AND a.account_type = 'user' THEN a.amount ELSE 0 END) AS service_fee_spending,
    - SUM(CASE WHEN a.category_id = 30 AND a.account_type = 'user' THEN a.amount ELSE 0 END) AS rental_spending
  FROM {{ ref("fct_financial_user_transaction_lines") }} AS a
  LEFT JOIN {{ ref("fct_work_orders") }} AS w ON a.trips_id = w.work_order_id
  WHERE 
    (a.group_id = 3 AND a.account_type = 'user') OR 
    (a.category_id = 30 AND a.account_type = 'user')
  GROUP BY COALESCE(w.contact_id, a.contact_id), a.payment_cycle
)

SELECT
  t.contact_id,
  t.year_week,
  v.vehicle_contract_type,
  t.trips_peak,
  t.trips_off_peak,
  COALESCE(f.service_fee_spending, 0) AS service_fee_spending,
  COALESCE(f.rental_spending,    0) AS rental_spending

FROM trips AS t
LEFT JOIN vehicle_contract_type AS v ON t.contact_id = v.contact_id AND t.year_week   = v.year_week
LEFT JOIN financials AS f ON t.contact_id = f.contact_id AND t.year_week   = f.year_week

ORDER BY t.year_week, t.contact_id