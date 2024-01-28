{{ config(materialized='table') }}

SELECT
    -- Table keys
    a.year_month,
    a.start_date,
    a.end_date,
    -- Table values General
    a.nr_active_users,
    a.nr_activations,
    a.new_activations,
    a.re_activations,
    a.nr_churns,
    a.churn_rate,
    a.retention_rate,
    a.lifespan,
    -- Table values Finances
    b.amount_invoiced,
    b.amount_recovered,
    b.amount_recovered_non_cash,
    b.collection_success_ratio,
    b.payment_delay,
    b.release_delay,
    b.estimation_lt,
    b.definitive_lt
FROM {{ ref('dm_core_monthly_marketplace_users') }} a
LEFT JOIN {{ ref('rpt_finances_monthly_performance') }} b ON a.year_month = b.year_month
WHERE partner_marketplace = 'Work'