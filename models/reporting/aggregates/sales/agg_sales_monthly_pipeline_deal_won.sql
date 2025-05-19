WITH
all_deals AS (
    SELECT CAST(close_date AS DATE) AS close_date, deal_pipeline_id, deal_pipeline_name as pipeline_name, deal_value as bonus FROM {{ ref('fct_deals') }} WHERE is_closed_won = TRUE
    UNION ALL
    SELECT CAST(close_date AS DATE) AS close_date, deal_pipeline_id, deal_pipeline_name as pipeline_name, deal_team_bonus as bonus FROM {{ ref('int_hubspot_drivfit_deals') }} WHERE is_closed_won = TRUE
)

SELECT
    a.year_month,
    b.deal_pipeline_id,
    b.pipeline_name,
    COUNT(*) as nr_won_deals,
FROM {{ ref('util_calendar') }} a
LEFT JOIN all_deals b ON a.date = b.close_date
WHERE 
    deal_pipeline_id IN (
        '155110085', -- Personal Vehicle
        '180181749', -- TVDE Training
        '180181752', -- Physical Fuel Card
        '180111309', -- Digital Fuel Card
        '177902584', -- Parcel Delivery
        '177891797', -- Groceries Delivery
        '165261801' -- Drivfit - Drivers
    )
GROUP BY year_month, pipeline_name, deal_pipeline_id
ORDER BY year_month DESC, pipeline_name DESC