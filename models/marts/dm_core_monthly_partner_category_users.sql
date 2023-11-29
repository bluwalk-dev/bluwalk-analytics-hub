WITH month_partner AS (

    SELECT DISTINCT 
        year_month,
        start_date,
        end_date,
        partner_marketplace,
        partner_category
    FROM 
    {{ ref('util_month_intervals') }}
    CROSS JOIN
    {{ ref('dim_partners') }}
    WHERE
        year > 2020 AND 
        start_date <= current_date()
)

-- Selecting key business metrics for each month
SELECT
    a.year_month,                 -- The year and month from the calendar table
    a.start_date,                 -- The start date of the month from the calendar table
    a.end_date,                  -- The end date of the month from the calendar table
    a.partner_marketplace,
    a.partner_category,
    -- Coalescing the number of active users with 0 in case there's no matching record
    IFNULL(b.nr_active_users, 0) as nr_active_users,
    -- Coalescing the total number of activations with 0 in case of no matching record
    IFNULL(c.nr_activations, 0) as nr_activations,
    -- Coalescing the number of new activations with 0
    IFNULL(c.new_activations, 0) as new_activations,
    -- Coalescing the number of reactivations with 0
    IFNULL(c.re_activations, 0) as re_activations,
    -- Coalescing the number of churns with 0
    IFNULL(d.nr_churns, 0) as nr_churns,
    -- Coalescing the churn rate with 0
    IFNULL(ROUND(e.churn_rate, 4), 0) as churn_rate,
    --IFNULL(ROUND(cal.monthly_revenue_per_user * userChurns.nr_churns, 2), 0) churned_mrr,
    -- Calculating the retention rate as 1 minus the churn rate, defaulting to 1 (100%) if no churn data
    ROUND(1-IFNULL(e.churn_rate, 0),4) as retention_rate,
    CASE 
        WHEN e.churn_rate IS NULL OR e.churn_rate = 0 THEN NULL
        ELSE ROUND(1 / e.churn_rate, 2)
    END lifespan
-- The FROM clause begins with the calendar utility table, which likely includes all months within the dataset*/
FROM month_partner a
-- Each of the following LEFT JOINs connects a monthly aggregate table to the calendar on year_month
LEFT JOIN {{ ref('agg_cm_monthly_partner_category_users_active') }} b ON a.year_month = b.year_month AND a.partner_category = b.partner_category
LEFT JOIN {{ ref('agg_cm_monthly_partner_category_users_activation') }} c ON a.year_month = c.year_month AND a.partner_category = C.partner_category
LEFT JOIN {{ ref('agg_cm_monthly_partner_category_users_churned') }} d ON a.year_month = d.year_month AND a.partner_category = d.partner_category
LEFT JOIN {{ ref('agg_cm_monthly_partner_category_churn_rate') }} e ON a.year_month = e.year_month AND a.partner_category = e.partner_category
WHERE (nr_active_users > 0 OR nr_activations > 0)
ORDER BY a.year_month DESC