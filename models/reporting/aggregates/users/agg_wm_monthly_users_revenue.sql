WITH revenue AS (
    SELECT
        b.year_month,
        ROUND(SUM(a.amount), 2) total_revenue
    FROM {{ ref('fct_accounting_revenue') }} a
    LEFT JOIN {{ ref('util_calendar') }} b ON a.date = b.date
    WHERE
        a.date <= current_date
    GROUP BY b.year_month
)

SELECT
    a.year_month,
    b.nr_active_users,
    a.total_revenue,
    CASE
        WHEN b.nr_active_users = 0 THEN NULL
        ELSE ROUND(a.total_revenue / b.nr_active_users, 2)
    END AS revenue_per_active_user
FROM {{ ref('util_month_intervals') }} a
LEFT JOIN {{ ref('agg_wm_monthly_users_active')}} b ON a.year_month = b.year_month
LEFT JOIN revenue c ON a.year_month = c.year_month
WHERE
    a.end_date <= current_date
GROUP BY b.year_month
ORDER BY b.year_month DESC