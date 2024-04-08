WITH WeekdayCounts AS (
    SELECT
      location_id,
      week_day_iso,
      COUNT(*) AS weekday_count -- Counts occurrences of each weekday
    FROM {{ ref('int_insights_trips_last_30_days') }}
    GROUP BY location_id, week_day_iso
),
NormalizedTrips AS (
    SELECT
      t.location_id,
      t.week_day_iso,
      SUM(t.nr_of_trips) AS nr_of_trips,
      wc.weekday_count,
      SUM(t.nr_of_trips) / wc.weekday_count AS normalized_trips
    FROM {{ ref('int_insights_trips_last_30_days') }} t
    JOIN WeekdayCounts wc ON t.location_id = wc.location_id AND t.week_day_iso = wc.week_day_iso
    GROUP BY t.location_id, t.week_day_iso, wc.weekday_count
),
MaxNormalized AS (
    SELECT
      location_id,
      MAX(normalized_trips) AS max_normalized_trips
    FROM NormalizedTrips
    GROUP BY location_id
)
SELECT 
  nt.location_id,
  nt.week_day_iso,
  nt.nr_of_trips,
  ROUND((nt.normalized_trips / mn.max_normalized_trips) * 100, 0) AS percentage
FROM NormalizedTrips nt
JOIN MaxNormalized mn ON nt.location_id = mn.location_id
ORDER BY nt.location_id ASC, nt.week_day_iso ASC, nt.nr_of_trips DESC