-- Select year_month and the number of signups that occurred in that month
SELECT
    a.year_month, -- Selecting the year and month from the calendar table
    count(b.user_id) as nr_signups -- Counting the number of users who signed up in each year_month
FROM
    {{ ref('util_calendar') }} a -- Referencing a utility calendar table which likely has a row for each day
LEFT JOIN 
    {{ ref('dim_users') }} b -- Left joining the dimension table for users
    ON a.date = CAST(b.create_date as DATE) -- Joining on the condition that the calendar date matches the user's creation date
-- Only considering dates up to the current day to avoid counting future signups
WHERE a.date <= current_date
-- Grouping the results by year_month to get the count of signups for each month
GROUP BY a.year_month
-- Ordering the results by year_month in descending order so the most recent months appear first
ORDER BY a.year_month DESC
