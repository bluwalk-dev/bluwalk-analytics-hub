-- Selecting the year_month and the number of signups that occurred in each month
SELECT
    a.date, -- Extracting the date from the calendar table
    COUNT(b.user_id) AS nr_signups -- Counting the number of user signups for each date
FROM 
    {{ ref('util_calendar') }} a -- Joining with a utility calendar table, which likely contains a row for each day
LEFT JOIN 
    {{ ref('dim_users') }} b -- Left joining with the user dimension table
    ON a.date = CAST(b.create_date AS DATE) -- Matching the calendar date with the user's creation date
WHERE a.date <= CURRENT_DATE -- Filtering to include only dates up to the current day to avoid future signups
GROUP BY a.date -- Grouping the results by date to aggregate signups per day
ORDER BY a.date DESC -- Sorting the results by date in descending order for recent dates first