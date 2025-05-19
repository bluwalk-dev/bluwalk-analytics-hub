{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

SELECT
  year_week,
  start_date,
  end_date,
  payment_delay,
  release_delay,
  invoiced_debt,
  fast_collection_ratio
FROM {{ ref('rpt_finances_weekly_performance') }}