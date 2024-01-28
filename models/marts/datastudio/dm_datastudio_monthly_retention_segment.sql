{{ config(materialized='table') }}

SELECT
    -- Table keys
    year_month,               
    start_date,                 
    end_date,                  
    retention_segment,
    -- Table values
    nr_active_users,
    nr_churns,
    nr_activations,
    churn_rate,
    mrr_churned_unit_value,
    mrr_churned
FROM {{ ref('rpt_retention_monthly_kpis') }}