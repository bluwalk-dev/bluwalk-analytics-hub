WITH RankedTrips AS (
    SELECT
      location_id,
      zone_name,
      time_window,
      SUM(nr_of_trips) AS nr_of_trips,
      ROW_NUMBER() OVER (PARTITION BY location_id, time_window ORDER BY SUM(nr_of_trips) DESC) AS rn,
      MAX(SUM(nr_of_trips)) OVER (PARTITION BY location_id, time_window) AS max_nr_of_trips
    FROM {{ ref('int_insights_trips_last_30_days') }}
    GROUP BY location_id, zone_name, time_window
)
SELECT 
  location_id,
  zone_name,
  time_window,
  nr_of_trips,
  ROUND((nr_of_trips / max_nr_of_trips)*100,0) as percentage
FROM RankedTrips
WHERE rn <= 10
ORDER BY time_window, nr_of_trips DESC