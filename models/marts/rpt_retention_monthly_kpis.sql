SELECT
    a.year_month,                 -- The year and month from the calendar table
    a.start_date,                 -- The start date of the month from the calendar table
    a.end_date,                  -- The end date of the month from the calendar table
    b.retention_segment,
    IFNULL(c.nr_active_users, 0) as nr_active_users,
    IFNULL(b.nr_churns,0) nr_churns,
    IFNULL(ROUND(d.churn_rate, 4), 0) as churn_rate,
FROM {{ ref('util_month_intervals') }} a
LEFT JOIN {{ ref('agg_retention_monthly_retention_segment_users_churned') }} b ON a.year_month = b.year_month
LEFT JOIN {{ ref('agg_retention_monthly_retention_segment_users_active') }} c ON a.year_month = c.year_month AND b.retention_segment = c.retention_segment
LEFT JOIN {{ ref('agg_retention_monthly_retention_segment_churn_rate') }} d ON a.year_month = d.year_month AND b.retention_segment = d.retention_segment
WHERE 
    year > 2020 AND 
    start_date <= current_date()
ORDER BY a.year_month DESC