{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

WITH 

rds AS (
    SELECT DISTINCT
        b.date,
        b.year_week,
        b.year_month,
        a.user_id,
        a.contact_id,
        'Ridesharing' as partner_category,
        CASE
            WHEN a.vehicle_contract_type = 'car_rental' THEN 'RDS: Vehicle Rental'
            ELSE 'RDS: Connected Vehicle'
        END retention_segment
    FROM {{ ref('fct_user_rideshare_trips') }} a
    LEFT JOIN {{ ref('util_calendar') }} b ON CAST(a.request_local_time AS DATE) = b.date
    WHERE b.year >= 2020
), 

others AS (
    SELECT
        date,
        year_week,
        year_month,
        a.user_id,
        a.contact_id,
        partner_category,
        CASE
            WHEN partner_category = 'Food Delivery' THEN 'FDL: Food Delivery'
            WHEN partner_category = 'Shopping' THEN 'GRC: Groceries'
            WHEN partner_category = 'Courier' THEN 'PRC: Parcel'
        END retention_segment
    FROM {{ ref('agg_wm_daily_activity') }} a
    WHERE partner_category != 'TVDE'
), 

worker_net_income AS (
    SELECT 
        date,
        contact_id,
        product_category as partner_category,
        sum(amount) as user_net_income
    FROM {{ ref('fct_financial_user_transactions') }}
    WHERE 
        extract(year from date) >= 2021 AND
        product_category IN ('Ridesharing', 'Shopping', 'Courier', 'Food Delivery')
    GROUP BY date, contact_id, partner_category
)

SELECT
    x.date,
    x.year_week,
    x.year_month,
    x.user_id,
    x.contact_id,
    x.retention_segment,
    x.partner_category,
    y.user_net_income
FROM (
    SELECT * FROM rds
    UNION ALL
    SELECT * FROM others
) x
LEFT JOIN worker_net_income y ON 
    x.date = y.date AND 
    x.contact_id = y.contact_id AND 
    x.partner_category = y.partner_category
ORDER BY date DESC, user_id DESC
