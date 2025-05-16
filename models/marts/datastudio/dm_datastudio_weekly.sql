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
  a.fast_collection_ratio
FROM {{ ref('rpt_finances_weekly_performance') }}