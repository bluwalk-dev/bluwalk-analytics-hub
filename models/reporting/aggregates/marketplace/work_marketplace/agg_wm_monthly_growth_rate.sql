-- Main selection of monthly user activations data with a calculation of growth rate
SELECT 
    au.year_month, -- The year and month for which the data is aggregated
    au2.nr_active_users as start_active_users, -- The number of active users at the start of the period from a previous month
    au.nr_activations, -- The number of activations during the month
    CASE 
        -- A case to handle division by zero or when there are no active users at the start of the period
        WHEN au2.nr_active_users = 0 THEN null -- If there are no start active users, growth rate is not defined (null)
        ELSE au.nr_activations/au2.nr_active_users -- Otherwise, calculate the growth rate as the ratio of new activations to start active users
    END as growth_rate -- The calculated growth rate for the month
-- From the monthly users activation aggregate table
FROM {{ ref('agg_wm_monthly_users_activation') }} au
-- Left joining the calendar utility table to ensure all months are included and matched correctly
LEFT JOIN {{ ref('util_month_intervals') }} ymk ON au.year_month = ymk.year_month
-- Left joining the monthly active users aggregate table to get the number of active users from the previous month
LEFT JOIN {{ ref('agg_wm_monthly_users_active') }} au2 ON ymk.previous_year_month = au2.year_month
-- Order the results by year_month in descending order to get the most recent data first
ORDER BY au.year_month DESC