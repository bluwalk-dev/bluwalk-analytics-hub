WITH WeekdayTimeWindowCounts AS (
    SELECT
      location_id,
      week_day_iso,
      time_window,
      COUNT(*) AS weekday_time_window_count -- Counts occurrences of each weekday within each time window
    FROM {{ ref('int_insights_trips_last_30_days') }}
    GROUP BY location_id, week_day_iso, time_window
),
NormalizedTrips AS (
    SELECT
      t.location_id,
      t.week_day_iso,
      t.time_window,
      SUM(t.nr_of_trips) AS nr_of_trips,
      wtwc.weekday_time_window_count,
      SUM(t.nr_of_trips) / wtwc.weekday_time_window_count AS normalized_trips -- Normalizes the number of trips
    FROM {{ ref('int_insights_trips_last_30_days') }} t
    JOIN WeekdayTimeWindowCounts wtwc ON t.location_id = wtwc.location_id AND t.week_day_iso = wtwc.week_day_iso AND t.time_window = wtwc.time_window
    GROUP BY t.location_id, t.week_day_iso, t.time_window, wtwc.weekday_time_window_count
),
MaxNormalized AS (
    SELECT
      location_id,
      time_window,
      MAX(normalized_trips) AS max_normalized_trips -- Finds the maximum normalized trips for each location and time window
    FROM NormalizedTrips
    GROUP BY location_id, time_window
)
SELECT 
  nt.location_id,
  nt.week_day_iso,
  nt.time_window,
  nt.nr_of_trips,
  ROUND((nt.normalized_trips / mn.max_normalized_trips) * 100, 0) AS percentage -- Calculates the adjusted percentage
FROM NormalizedTrips nt
JOIN MaxNormalized mn ON nt.location_id = mn.location_id AND nt.time_window = mn.time_window
ORDER BY nt.location_id, nt.time_window, nt.week_day_iso, nt.nr_of_trips DESC