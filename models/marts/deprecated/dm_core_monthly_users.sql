{{ config(
    enabled = false
) }}

SELECT
    a.year_month,                 -- The year and month from the calendar table
    a.start_date,                 -- The start date of the month from the calendar table
    a.end_date,                   -- The end date of the month from the calendar table
    a.nr_active_users,
    a.nr_activations,
    a.new_activations,
    a.re_activations,
    a.nr_churns,
    a.website_visitors,
    a.nr_signups,
    a.churn_rate,
    a.retention_rate,
    NULL as growth_rate,
    a.revenue_per_active_user,
    a.nps_score,
    a.lifespan
FROM {{ ref('rpt_core_monthly_report') }} a