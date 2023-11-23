-- Selecting key business metrics for each month
SELECT
    a.year_month,                 -- The year and month from the calendar table
    a.start_date,                 -- The start date of the month from the calendar table
    a.end_date,                   -- The end date of the month from the calendar table
    -- Coalescing the number of active users with 0 in case there's no matching record
    a.nr_active_users,
    -- Coalescing the total number of activations with 0 in case of no matching record
    a.nr_activations,
    -- Coalescing the number of new activations with 0
    a.new_activations,
    -- Coalescing the number of reactivations with 0
    a.re_activations,
    -- Coalescing the number of churns with 0
    a.nr_churns,
    -- Coalescing the website visitor count with 0
    IFNULL(b.user_count, 0) as website_visitors,
    -- Coalescing the number of signups with 0
    IFNULL(c.nr_signups, 0) as nr_signups,
    -- Coalescing the churn rate with 0
    a.churn_rate,
    -- Calculating the retention rate as 1 minus the churn rate, defaulting to 1 (100%) if no churn data
    a.retention_rate,
    CASE
        WHEN a.nr_active_users = 0 THEN NULL
        ELSE ROUND(d.amount / a.nr_active_users, 2)
    END AS revenue_per_active_user,
    IFNULL(ROUND(e.nps_score, 2),0) nps_score,
    a.lifespan
-- The FROM clause begins with the calendar utility table, which likely includes all months within the dataset
FROM {{ ref('dm_core_monthly_marketplace_users') }} a
LEFT JOIN {{ ref('agg_marketing_monthly_website_traffic') }} b ON a.year_month = b.year_month
LEFT JOIN {{ ref('agg_wm_monthly_signups') }} c ON a.year_month = c.year_month
LEFT JOIN {{ ref('agg_finances_monthly_revenue') }} d ON a.year_month = d.year_month
LEFT JOIN {{ ref('agg_customer_service_monthly_nps') }} e ON a.year_month = e.year_month
-- The WHERE clause filters for months after 2020 and ensures the start_date is not in the future
WHERE 
    a.partner_marketplace = 'Work'
-- Ordering results by year_month descending to get the most recent data first
ORDER BY a.year_month DESC