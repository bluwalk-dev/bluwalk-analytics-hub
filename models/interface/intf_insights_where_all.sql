WITH ranked_zones AS (
  SELECT
    location_id,
    zone_name,
    zone_navigation_link,
    SUM(nr_of_trips) AS nr_of_trips,
    ROW_NUMBER() OVER(PARTITION BY location_id ORDER BY SUM(nr_of_trips) DESC) as rank,
    MAX(SUM(nr_of_trips)) OVER(PARTITION BY location_id) as max_trips_per_location
  FROM {{ ref('int_insights_trips_last_30_days') }}
  GROUP BY location_id, zone_name, zone_navigation_link
)

SELECT
  location_id,
  zone_name,
  zone_navigation_link,
  nr_of_trips,
  ROUND((nr_of_trips / max_trips_per_location) * 100) as percentage
FROM ranked_zones
WHERE rank <= 10
ORDER BY location_id, rank