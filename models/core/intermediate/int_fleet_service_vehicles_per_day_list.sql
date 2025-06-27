SELECT
  s.vehicle_id,
  s.vehicle_license_plate,
  d.date AS date,
  d.year_month
FROM {{ ref('int_fleet_service_intervals') }} AS s
JOIN {{ ref('util_calendar') }} AS d
  ON s.service_start <  DATETIME(DATE_ADD(d.date, INTERVAL 1 DAY), TIME '00:00:00') AND 
  s.service_end >= DATETIME(DATE_ADD(d.date, INTERVAL 1 DAY), TIME '00:00:00')
ORDER BY
  d.date desc,
  s.vehicle_id