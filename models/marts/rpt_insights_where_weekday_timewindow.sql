WITH RankedTrips AS (
    SELECT
      location_id,
      zone_name,
      NULL zone_navigation_link,
      time_window,
      week_day_iso,
      nr_of_trips,
      ROW_NUMBER() OVER (PARTITION BY location_id, week_day_iso, time_window ORDER BY nr_of_trips DESC) AS rn,
      MAX(nr_of_trips) OVER (PARTITION BY location_id, week_day_iso, time_window) AS max_nr_of_trips
    FROM {{ ref('int_insights_trips_last_30_days') }}
)

SELECT 
  location_id,
  zone_name,
  zone_navigation_link,
  time_window,
  week_day_iso,
  nr_of_trips,
  ROUND((nr_of_trips / max_nr_of_trips)*100,0) as percentage
FROM RankedTrips
WHERE rn <= 10
ORDER BY location_id, week_day_iso, time_window, nr_of_trips DESC
