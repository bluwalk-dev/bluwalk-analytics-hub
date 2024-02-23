{{ config(materialized='table') }}

WITH 
cs_dates_locations AS (
  SELECT DISTINCT
    a.location_id,
    b.date
  FROM {{ ref("base_predict_hq_events") }} a
  CROSS JOIN {{ ref("util_calendar") }} b
  WHERE date > current_date()
)

SELECT
  a.location_id,
  a.date,
  IFNULL(SUM(event_phq_attendance),0) expected_attendance
FROM cs_dates_locations a
LEFT JOIN {{ ref("base_predict_hq_events") }} b ON CAST(b.event_start_date AS DATE) = a.date AND a.location_id = b.location_id
GROUP BY location_id, date
ORDER BY date ASC