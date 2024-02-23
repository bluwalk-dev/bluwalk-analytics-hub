WITH RankedTrips AS (
    SELECT
      location_id,
      zone_name,
      zone_navigation_link,
      week_day_iso,
      SUM(nr_of_trips) AS nr_of_trips,
      ROW_NUMBER() OVER (PARTITION BY location_id, week_day_iso ORDER BY SUM(nr_of_trips) DESC) AS rn,
      MAX(SUM(nr_of_trips)) OVER (PARTITION BY location_id, week_day_iso) AS max_nr_of_trips
    FROM {{ ref('int_insights_trips_last_30_days') }}
    GROUP BY location_id, zone_name, week_day_iso, zone_navigation_link
)

SELECT 
  location_id, 
  zone_name,
  zone_navigation_link,
  week_day_iso, 
  nr_of_trips,
  ROUND((nr_of_trips / max_nr_of_trips)*100, 0) as percentage
FROM RankedTrips
WHERE rn <= 10
ORDER BY week_day_iso, nr_of_trips DESC