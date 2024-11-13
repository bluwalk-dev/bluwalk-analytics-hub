WITH
all_deals AS (
    SELECT CAST(close_date AS DATE) AS close_date, deal_pipeline_id, deal_value as bonus FROM {{ ref('fct_deals') }} WHERE is_closed_won = TRUE
    UNION ALL
    SELECT CAST(close_date AS DATE) AS close_date, deal_pipeline_id, deal_team_bonus as bonus FROM {{ ref('stg_drivfit__hubspot_deals') }} WHERE is_closed_won = TRUE
),
bonus_until_202409 AS (
    SELECT
        a.year_month,
        ROUND(SUM(b.bonus), 2) bonus
    FROM {{ ref('util_calendar') }} a
    LEFT JOIN all_deals b ON a.date = b.close_date
    WHERE 
        deal_pipeline_id IN (
            -- Bluwalk
            '155110085', -- Personal Vehicle
            'default', -- Insurance
            '180181749', -- TVDE Training
            '180181752', -- Physical Fuel Card
            '180111309', -- Digital Fuel Card
            '177902584', -- Parcel Delivery
            '177880551', -- Food Delivery
            '177891797', -- Groceries Delivery
            '155112898', -- Uber
            '166787800' -- Bolt
        ) AND
        year_month <= 202409 AND
        b.bonus > 0
    GROUP BY year_month
),
bonus_current AS (
    SELECT
        a.year_month,
        ROUND(SUM(b.bonus), 2) bonus
    FROM {{ ref('util_calendar') }} a
    LEFT JOIN all_deals b ON a.date = b.close_date
    WHERE 
        deal_pipeline_id IN (
            -- Bluwalk
            '155110085', -- Personal Vehicle
            'default', -- Insurance
            '180181749', -- TVDE Training
            '180181752', -- Physical Fuel Card
            '180111309', -- Digital Fuel Card
            '177902584', -- Parcel Delivery
            '177880551', -- Food Delivery
            '177891797', -- Groceries Delivery
            '155112898', -- Uber
            '166787800', -- Bolt
            '165261801', -- Drivfit - Drivers
            -- Drivfit
            '586124258'  -- Drivfit - Companies
        ) AND
        year_month > 202409 AND
        b.bonus > 0
    GROUP BY year_month
)

SELECT * FROM bonus_until_202409
UNION ALL
SELECT * FROM bonus_current
ORDER BY year_month DESC