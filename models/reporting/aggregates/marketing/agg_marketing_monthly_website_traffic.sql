-- A Common Table Expression (CTE) named 'traffic_per_day' that calculates daily traffic metrics
WITH traffic_per_day AS (
    SELECT
        a.date, -- The date of the traffic
        COUNT(*) AS user_count, -- Count of all users on that date
        SUM(CASE 
                WHEN is_new_user IS TRUE THEN 1 -- If the user is flagged as a new user, count 1
                ELSE 0 -- Otherwise, count 0
            END) new_user_count -- The total count of new users for the day
    FROM {{ ref('base_website_traffic') }} a -- Referencing the base website traffic model which contains raw traffic data
    GROUP BY date -- Grouping the data by the date to get the daily metrics
)

-- The main query that aggregates the daily traffic data into monthly metrics
SELECT
    cal.year_month, -- The year and month from the calendar table
    sum(trf.user_count) as user_count, -- Sum of all daily user counts for the month
    sum(trf.new_user_count) as new_user_count -- Sum of all daily new user counts for the month
FROM {{ ref('util_calendar') }} cal -- Referencing a utility calendar table that contains all dates within the desired range
LEFT JOIN traffic_per_day trf ON cal.date = trf.date -- Joining the CTE with the calendar on the date column
WHERE user_count IS NOT NULL -- Filtering out dates with no user data; this is likely meant to be 'trf.user_count' to reference the joined CTE
GROUP BY cal.year_month -- Grouping the results by year and month to aggregate the counts
ORDER BY year_month DESC -- Ordering the results by year and month in descending order to start with the most recent data