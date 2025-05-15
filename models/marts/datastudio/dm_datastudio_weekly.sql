{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

SELECT
  a.year_week,
  a.start_date,
  a.end_date,
  a.payment_delay,
  a.release_delay,
  a.invoiced_debt,
  a.fast_collection_ratio,
  b.activation_points
FROM {{ ref('rpt_finances_weekly_performance') }} a
LEFT JOIN {{ ref('agg_activation_weekly_activation_points') }} b ON a.year_week = b.year_week