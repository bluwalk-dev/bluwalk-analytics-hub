SELECT
  location_id,
  zone_name,
  NULL zone_navigation_link,
  SUM(nr_of_trips) AS nr_of_trips,
  ROUND((SUM(nr_of_trips) / MAX(SUM(nr_of_trips)) OVER ())*100) as percentage
FROM {{ ref('int_insights_trips_last_30_days') }}
GROUP BY location_id, zone_name
ORDER BY nr_of_trips DESC
LIMIT 10