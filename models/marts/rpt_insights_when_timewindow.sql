WITH RankedTrips AS (
    SELECT
      city,
      week_day,
      time_window,
      SUM(nr_of_trips) AS nr_of_trips,
      MAX(SUM(nr_of_trips)) OVER (PARTITION BY city, time_window) AS max_nr_of_trips
    FROM {{ ref('int_insights_trips_last_30_days') }}
    GROUP BY city, week_day, time_window
)
SELECT 
  city,
  week_day,
  time_window,
  nr_of_trips,
  ROUND((nr_of_trips / max_nr_of_trips)*100,0) as percentage
FROM RankedTrips
ORDER BY city, time_window, week_day, nr_of_trips DESC