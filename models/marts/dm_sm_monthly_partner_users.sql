WITH month_partner AS (

    SELECT * FROM 
    {{ ref('util_month_intervals') }}
    CROSS JOIN
    {{ ref('fct_quarter_params') }}
    WHERE
        year > 2020 AND 
        start_date <= current_date() AND
        partner_marketplace = 'Service'

)

-- Selecting key business metrics for each month
SELECT
    cal.year_month,                 -- The year and month from the calendar table
    cal.start_date,                 -- The start date of the month from the calendar table
    cal.end_date,                  -- The end date of the month from the calendar table
    cal.partner_name,
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
    -- Coalescing the churn rate with 0
    IFNULL(ROUND(churnedUsers.churn_rate, 4), 0) as churn_rate,
    IFNULL(ROUND(cal.monthly_revenue_per_user * userChurns.nr_churns, 2), 0) churned_mrr,
    -- Calculating the retention rate as 1 minus the churn rate, defaulting to 1 (100%) if no churn data
    1-IFNULL(ROUND(churnedUsers.churn_rate, 4), 0) as retention_rate,
    CASE 
        WHEN churnedUsers.churn_rate IS NULL OR churnedUsers.churn_rate = 0 THEN NULL
        ELSE ROUND(1 / churnedUsers.churn_rate, 4)
    END lifespan
-- The FROM clause begins with the calendar utility table, which likely includes all months within the dataset*/
FROM month_partner cal
-- Each of the following LEFT JOINs connects a monthly aggregate table to the calendar on year_month
LEFT JOIN {{ ref('agg_sm_monthly_partner_users_active') }} activeUsers ON cal.year_month = activeUsers.year_month AND cal.partner_name = activeUsers.partner_name
LEFT JOIN {{ ref('agg_sm_monthly_partner_users_activation') }} userActivations ON cal.year_month = userActivations.year_month AND cal.partner_name = userActivations.partner_name
LEFT JOIN {{ ref('agg_sm_monthly_partner_users_churned') }} userChurns ON cal.year_month = userChurns.year_month AND cal.partner_name = userChurns.partner_name
LEFT JOIN {{ ref('agg_sm_monthly_partner_churn_rate') }} churnedUsers ON cal.year_month = churnedUsers.year_month  AND cal.partner_name = churnedUsers.partner_name
-- The WHERE clause filters for months after 2020 and ensures the start_date is not in the future
ORDER BY cal.year_month DESC