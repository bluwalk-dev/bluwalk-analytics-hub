-- Selecting key business metrics for each month
SELECT
    cal.year_month,                 -- The year and month from the calendar table
    cal.end_date,                   -- The end date of the month from the calendar table
    -- Coalescing the number of active users with 0 in case there's no matching record
    IFNULL(activeUsers.nr_active_users, 0) as nr_active_users,
    -- Coalescing the total number of activations with 0 in case of no matching record
    IFNULL(userActivations.nr_activations, 0) as nr_activations,
    -- Coalescing the number of new activations with 0
    IFNULL(userActivations.new_activations, 0) as new_activations,
    -- Coalescing the number of reactivations with 0
    IFNULL(userActivations.re_activations, 0) as re_activations,
    -- Coalescing the number of churns with 0
    IFNULL(userChurns.nr_churns, 0) as nr_churns,
    -- Coalescing the website visitor count with 0
    IFNULL(website.user_count, 0) as website_visitors,
    -- Coalescing the number of signups with 0
    IFNULL(signup.nr_signups, 0) as nr_signups,
    -- Coalescing the churn rate with 0
    IFNULL(churnedUsers.churn_rate, 0) as churn_rate,
    -- Calculating the retention rate as 1 minus the churn rate, defaulting to 1 (100%) if no churn data
    1-IFNULL(churnedUsers.churn_rate, 0) as retention_rate,
    -- Coalescing the growth rate with 0
    IFNULL(growthUsers.growth_rate, 0) as growth_rate
-- The FROM clause begins with the calendar utility table, which likely includes all months within the dataset
FROM {{ ref('util_month_intervals') }} cal
-- Each of the following LEFT JOINs connects a monthly aggregate table to the calendar on year_month
LEFT JOIN {{ ref('agg_wm_monthly_users_active') }} activeUsers ON cal.year_month = activeUsers.year_month
LEFT JOIN {{ ref('agg_wm_monthly_users_activation') }} userActivations ON cal.year_month = userActivations.year_month 
LEFT JOIN {{ ref('agg_wm_monthly_users_churned') }} userChurns ON cal.year_month = userChurns.year_month
LEFT JOIN {{ ref('agg_wm_monthly_churn_rate') }} churnedUsers ON cal.year_month = churnedUsers.year_month
LEFT JOIN {{ ref('agg_wm_monthly_growth_rate') }} growthUsers ON cal.year_month = growthUsers.year_month
LEFT JOIN {{ ref('agg_marketing_monthly_website_traffic') }} website ON cal.year_month = website.year_month
LEFT JOIN {{ ref('agg_wm_monthly_signups') }} signup ON cal.year_month = signup.year_month
-- The WHERE clause filters for months after 2020 and ensures the end_date is not in the future
WHERE cal.year > 2020 AND cal.end_date <= current_date()
-- Ordering results by year_month descending to get the most recent data first
ORDER BY cal.year_month DESC