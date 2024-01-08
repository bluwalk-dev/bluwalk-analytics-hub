{{ config(materialized='table') }}

WITH 

zip_codes as (
    SELECT * FROM {{ source('generic', 'zip_codes') }}
),

trips AS (
    SELECT
        c.week_day,
        CASE
            WHEN CAST(a.request_local_time AS TIME) BETWEEN '00:00:00' AND '05:59:59' THEN 'night'
            WHEN CAST(a.request_local_time AS TIME) BETWEEN '06:00:00' AND '11:59:59' THEN 'morning'
            WHEN CAST(a.request_local_time AS TIME) BETWEEN '12:00:00' AND '17:59:59' THEN 'afternoon'
            WHEN CAST(a.request_local_time AS TIME) BETWEEN '18:00:0' AND '23:59:59' THEN 'evening'
        END AS time_window,
        'Porto' city,
        b.zone
    FROM {{ ref('fct_user_rideshare_trips') }} a
    LEFT JOIN zip_codes b on a.address_pickup_zip = b.zip_code
    LEFT JOIN {{ ref('util_calendar') }} c ON CAST(a.request_local_time AS DATE) = c.date
    WHERE
        b.zone is not null and
        CAST(a.request_local_time AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
)

SELECT 
    city,
    zone,
    week_day,
    time_window,
    nr_of_trips
FROM (
    SELECT
        *,
        RANK() OVER(PARTITION BY week_day, time_window ORDER BY nr_of_trips DESC) as rank
    FROM (
        SELECT
            city,
            zone,
            week_day,
            time_window,
            count(*) nr_of_trips,
        FROM trips
        GROUP BY week_day, time_window, city, zone
    )
)
WHERE rank <= 10
ORDER BY week_day ASC, time_window, nr_of_trips DESC