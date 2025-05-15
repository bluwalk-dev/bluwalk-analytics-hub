{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

WITH

trips AS (
    SELECT
        c.week_day_iso,
        CASE
            WHEN CAST(a.request_local_time AS TIME) BETWEEN '00:00:00' AND '05:59:59' THEN 'night'
            WHEN CAST(a.request_local_time AS TIME) BETWEEN '06:00:00' AND '11:59:59' THEN 'morning'
            WHEN CAST(a.request_local_time AS TIME) BETWEEN '12:00:00' AND '17:59:59' THEN 'afternoon'
            WHEN CAST(a.request_local_time AS TIME) BETWEEN '18:00:0' AND '23:59:59' THEN 'evening'
        END AS time_window,
        a.location_id,
        a.zone_name,
        a.zone_navigation_link
    FROM {{ ref('int_user_rideshare_trips_zoned') }} a
    LEFT JOIN {{ ref('util_calendar') }} c ON CAST(a.request_local_time AS DATE) = c.date
    WHERE
        a.zone_name IS NOT NULL AND
        CAST(a.request_local_time AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
)

SELECT 
    location_id,
    zone_name,
    zone_navigation_link,
    week_day_iso,
    time_window,
    nr_of_trips
FROM (
    SELECT
        *,
        RANK() OVER(PARTITION BY location_id, week_day_iso, time_window ORDER BY nr_of_trips DESC) as rank
    FROM (
        SELECT
            location_id,
            zone_name,
            zone_navigation_link,
            week_day_iso,
            time_window,
            count(*) nr_of_trips,
        FROM trips
        GROUP BY week_day_iso, time_window, location_id, zone_name, zone_navigation_link
    )
)
WHERE rank <= 10
ORDER BY location_id ASC, week_day_iso ASC, time_window, nr_of_trips DESC