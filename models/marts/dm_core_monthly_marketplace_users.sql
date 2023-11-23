WITH month_partner AS (
    SELECT DISTINCT year_month, partner_marketplace, start_date, end_date FROM 
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
    a.end_date,                   -- The end date of the month from the calendar table
    a.partner_marketplace,
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
    -- Calculating the retention rate as 1 minus the churn rate, defaulting to 1 (100%) if no churn data
    1-IFNULL(ROUND(e.churn_rate, 4), 0) as retention_rate,
    CASE 
        WHEN e.churn_rate IS NULL OR e.churn_rate = 0 THEN NULL
        ELSE ROUND(1 / e.churn_rate, 4)
    END lifespan
-- The FROM clause begins with the calendar utility table, which likely includes all months within the dataset
FROM month_partner a
-- Each of the following LEFT JOINs connects a monthly aggregate table to the calendar on year_month
LEFT JOIN {{ ref('agg_cm_monthly_marketplace_users_active') }} b ON a.year_month = b.year_month AND a.partner_marketplace = b.partner_marketplace
LEFT JOIN {{ ref('agg_cm_monthly_marketplace_users_activation') }} c ON a.year_month = c.year_month AND a.partner_marketplace = c.partner_marketplace
LEFT JOIN {{ ref('agg_cm_monthly_marketplace_users_churned') }} d ON a.year_month = d.year_month AND a.partner_marketplace = d.partner_marketplace
LEFT JOIN {{ ref('agg_cm_monthly_marketplace_churn_rate') }} e ON a.year_month = e.year_month AND a.partner_marketplace = e.partner_marketplace
-- Ordering results by year_month descending to get the most recent data first
ORDER BY 
    a.year_month DESC,
    a.partner_marketplace DESC