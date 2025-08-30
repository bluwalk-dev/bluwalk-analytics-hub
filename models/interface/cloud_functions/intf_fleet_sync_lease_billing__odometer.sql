WITH weekly_conditions AS (
  SELECT
    c.lease_contract_id,
    c.lease_contract_name,
    c.customer_id,
    c.vehicle_id,
    c.start_date AS contract_start_date,
    c.end_date   AS contract_end_date,
    c.start_kms  AS contract_start_kms,
    c.end_kms    AS contract_end_kms,
    cond.id AS conditions_id,
    cond.effective_date,
    COALESCE(cond.termination_date, CURRENT_DATE("Europe/Lisbon")) AS termination_date,
    cond.rate_mileage_wk_limit,
    rm.mileage_excess AS extra_mileage_price,
    w.year_week,
    GREATEST(w.start_date, cond.effective_date, c.start_date) AS period_start,
    LEAST(
      w.end_date,
      COALESCE(cond.termination_date, CURRENT_DATE("Europe/Lisbon")),
      COALESCE(c.end_date, CURRENT_DATE("Europe/Lisbon"))
    ) AS period_end
  FROM {{ ref('base_fleet_lease_contracts') }} c
  JOIN {{ ref('stg_odoo_drivfit__lease_contract_conditions') }} cond
    ON c.lease_contract_id = cond.lease_contract_id
  LEFT JOIN {{ ref('stg_odoo_drivfit__rate_mileages') }} rm
    ON cond.rate_mileage_id = rm.id
  JOIN {{ ref('util_week_intervals') }} w
    ON w.start_date <= COALESCE(cond.termination_date, CURRENT_DATE("Europe/Lisbon"))
   AND w.end_date   >= cond.effective_date
),
start_reading AS (
  SELECT
    w.year_week,
    o.vehicle_id,
    o.odometer AS start_odometer
  FROM {{ ref('util_week_intervals') }} w
  JOIN {{ ref('fct_fleet_vehicle_odometers') }} o
    ON o.date < w.start_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY w.year_week, o.vehicle_id
    ORDER BY o.date DESC
  ) = 1
),
end_reading AS (
  SELECT
    w.year_week,
    o.vehicle_id,
    o.odometer AS end_odometer
  FROM {{ ref('util_week_intervals') }} w
  JOIN {{ ref('fct_fleet_vehicle_odometers') }} o
    ON o.date <= w.end_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY w.year_week, o.vehicle_id
    ORDER BY o.date DESC
  ) = 1
),
odometer_weekly AS (
  SELECT
    COALESCE(s.year_week, e.year_week) AS year_week,
    COALESCE(s.vehicle_id, e.vehicle_id) AS vehicle_id,
    s.start_odometer,
    e.end_odometer
  FROM start_reading s
  FULL JOIN end_reading e USING (year_week, vehicle_id)
)
SELECT
  wc.lease_contract_id,
  wc.lease_contract_name,
  wc.customer_id,
  wc.vehicle_id,
  wc.conditions_id,
  wc.period_start,
  wc.period_end,
  wc.year_week,
  wc.extra_mileage_price,

  DATE_DIFF(wc.period_end, wc.period_start, DAY) + 1 AS rental_days,

  CASE 
    WHEN wc.period_start = wc.contract_start_date THEN wc.contract_start_kms
    ELSE od.start_odometer
  END AS start_kms,

  CASE 
    WHEN wc.contract_end_date IS NOT NULL AND wc.period_end = wc.contract_end_date THEN wc.contract_end_kms
    ELSE od.end_odometer
  END AS end_kms,

  -- distância arredondada por defeito (base para extras e receita)
  FLOOR(GREATEST(
    (CASE 
       WHEN wc.contract_end_date IS NOT NULL AND wc.period_end = wc.contract_end_date THEN wc.contract_end_kms
       ELSE od.end_odometer
     END)
    -
    (CASE 
       WHEN wc.period_start = wc.contract_start_date THEN wc.contract_start_kms
       ELSE od.start_odometer
     END),
  0)) AS distance,

  -- kms incluídos pró‑rata, arredondados por defeito
  FLOOR((wc.rate_mileage_wk_limit / 7.0) * (DATE_DIFF(wc.period_end, wc.period_start, DAY) + 1)) AS included_kms,

  -- extras com base nos valores arredondados
  GREATEST(
    FLOOR(GREATEST(
      (CASE 
         WHEN wc.contract_end_date IS NOT NULL AND wc.period_end = wc.contract_end_date THEN wc.contract_end_kms
         ELSE od.end_odometer
       END)
      -
      (CASE 
         WHEN wc.period_start = wc.contract_start_date THEN wc.contract_start_kms
         ELSE od.start_odometer
       END),
    0)) - FLOOR((wc.rate_mileage_wk_limit / 7.0) * (DATE_DIFF(wc.period_end, wc.period_start, DAY) + 1)),
    0
  ) AS extra_km,

  -- receita baseada nos kms arredondados
  ROUND(
    wc.extra_mileage_price *
    GREATEST(
      FLOOR(GREATEST(
        (CASE 
           WHEN wc.contract_end_date IS NOT NULL AND wc.period_end = wc.contract_end_date THEN wc.contract_end_kms
           ELSE od.end_odometer
         END)
        -
        (CASE 
           WHEN wc.period_start = wc.contract_start_date THEN wc.contract_start_kms
           ELSE od.start_odometer
         END),
      0)) - FLOOR((wc.rate_mileage_wk_limit / 7.0) * (DATE_DIFF(wc.period_end, wc.period_start, DAY) + 1)),
      0
    ),
    2
  ) AS extra_km_revenue

FROM weekly_conditions wc
LEFT JOIN odometer_weekly od
  ON wc.vehicle_id = od.vehicle_id
 AND wc.year_week = od.year_week
ORDER BY wc.lease_contract_id, wc.year_week