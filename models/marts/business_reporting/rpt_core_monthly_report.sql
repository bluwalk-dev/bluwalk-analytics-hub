-- Selecting key business metrics for each month
SELECT
    a.year_month,                 -- The year and month from the calendar table
    a.start_date,                 -- The start date of the month from the calendar table
    a.end_date,                   -- The end date of the month from the calendar table
    a.nr_active_users,
    a.nr_activations,
    a.new_activations,
    a.re_activations,
    a.nr_churns,
    NULL as website_visitors,
    IFNULL(c.nr_signups, 0) as nr_signups,
    a.churn_rate,
    a.retention_rate,
    CASE
        WHEN a.nr_active_users = 0 THEN NULL
        ELSE ROUND(d.amount / a.nr_active_users, 2)
    END AS revenue_per_active_user,
    IFNULL(ROUND(e.nps_score, 2),0) nps_score,
    a.lifespan
FROM {{ ref('dm_core_monthly_marketplace_users') }} a
LEFT JOIN {{ ref('agg_wm_monthly_signups') }} c ON a.year_month = c.year_month
LEFT JOIN {{ ref('agg_finances_monthly_revenue') }} d ON a.year_month = d.year_month
LEFT JOIN {{ ref('agg_customer_service_monthly_nps') }} e ON a.year_month = e.year_month
WHERE 
    a.partner_marketplace = 'Work'
ORDER BY a.year_month DESC