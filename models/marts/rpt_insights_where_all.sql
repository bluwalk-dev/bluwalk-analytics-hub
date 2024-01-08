SELECT
  city,
  zone,
  SUM(nr_of_trips) AS nr_of_trips,
  ROUND((SUM(nr_of_trips) / MAX(SUM(nr_of_trips)) OVER ())*100) as percentage
FROM {{ ref('int_insights_trips_last_30_days') }}
GROUP BY city, zone
ORDER BY nr_of_trips DESC
LIMIT 10