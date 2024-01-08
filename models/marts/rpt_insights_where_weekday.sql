WITH RankedTrips AS (
    SELECT
      city,
      zone,
      week_day,
      SUM(nr_of_trips) AS nr_of_trips,
      ROW_NUMBER() OVER (PARTITION BY city, week_day ORDER BY SUM(nr_of_trips) DESC) AS rn,
      MAX(SUM(nr_of_trips)) OVER (PARTITION BY city, week_day) AS max_nr_of_trips
    FROM {{ ref('int_insights_trips_last_30_days') }}
    GROUP BY city, zone, week_day
)

SELECT 
  city, 
  zone, 
  week_day, 
  nr_of_trips,
  ROUND((nr_of_trips / max_nr_of_trips)*100, 0) as percentage
FROM RankedTrips
WHERE rn <= 10
ORDER BY week_day, nr_of_trips DESC