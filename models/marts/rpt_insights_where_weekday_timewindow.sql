WITH RankedTrips AS (
    SELECT
      city,
      zone,
      time_window,
      week_day,
      nr_of_trips,
      ROW_NUMBER() OVER (PARTITION BY city, week_day, time_window ORDER BY nr_of_trips DESC) AS rn,
      MAX(nr_of_trips) OVER (PARTITION BY city, week_day, time_window) AS max_nr_of_trips
    FROM {{ ref('int_insights_trips_last_30_days') }}
)

SELECT 
  city,
  zone,
  time_window,
  week_day,
  nr_of_trips,
  ROUND((nr_of_trips / max_nr_of_trips)*100,0) as percentage
FROM RankedTrips
WHERE rn <= 10
ORDER BY city, week_day, time_window, nr_of_trips DESC
