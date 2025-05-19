{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

WITH 
    
    work_marketplace AS (
        SELECT * 
        FROM {{ ref('agg_wm_daily_activity') }}
    ), 

    service_marketplace AS (
        SELECT * 
        FROM {{ ref('agg_sm_daily_activity') }}
)

SELECT
    date,
    year_week,
    year_month,
    user_id,
    contact_id,
    partner_key,
    partner_marketplace,
    partner_category,
    partner_name
FROM (
    SELECT * FROM work_marketplace
    UNION ALL
    SELECT * FROM service_marketplace
)
ORDER BY date DESC, user_id DESC