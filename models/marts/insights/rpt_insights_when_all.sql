WITH RankedTrips AS (
    SELECT
      location_id,
      week_day_iso,
      SUM(nr_of_trips) AS nr_of_trips,
      MAX(SUM(nr_of_trips)) OVER (PARTITION BY location_id) AS max_nr_of_trips
    FROM {{ ref('int_insights_trips_last_30_days') }}
    GROUP BY location_id, week_day_iso
)
SELECT 
  location_id,
  week_day_iso,
  nr_of_trips,
  ROUND((nr_of_trips / max_nr_of_trips)*100,0) as percentage
FROM RankedTrips
ORDER BY location_id ASC, week_day_iso ASC, nr_of_trips DESC