SELECT 
    a.year_month,
    b.nr_active_users as start_active_users,
    a.nr_churns as nr_churns,
    CASE 
        WHEN b.nr_active_users = 0 THEN null 
        ELSE a.nr_churns/b.nr_active_users
    END as churn_rate
FROM {{ ref('agg_wm_monthly_users_churned') }} a
LEFT JOIN {{ ref('util_month_intervals') }} c ON a.year_month = c.year_month
LEFT JOIN {{ ref('agg_wm_monthly_users_active') }} b ON
    c.previous_year_month = b.year_month
ORDER BY a.year_month DESC