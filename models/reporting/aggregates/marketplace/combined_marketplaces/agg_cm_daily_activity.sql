{{ config(materialized='table') }}

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
    user_id,
    contact_id,
    partner_id,
    partner_name,
    partner_category,
    partner_marketplace,
    date,
    year_week,
    year_month
FROM (
    SELECT * FROM work_marketplace
    UNION ALL
    SELECT * FROM service_marketplace
)
ORDER BY date DESC