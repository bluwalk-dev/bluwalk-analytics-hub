SELECT
  a.date,
  a.year_month,
  b.vehicle_id,
  b.vehicle_deal_name
FROM {{ ref('util_calendar') }} a
JOIN {{ ref('fct_fleet_vehicle_contracts') }} b ON a.date BETWEEN b.start_date AND b.end_date