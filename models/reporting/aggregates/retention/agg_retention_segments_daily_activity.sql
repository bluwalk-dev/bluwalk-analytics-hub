{{ config(materialized='table') }}

WITH rds AS (
    SELECT DISTINCT
        b.date,
        b.year_week,
        b.year_month,
        a.user_id,
        a.contact_id,
        CASE
            WHEN a.vehicle_contract_type = 'car_rental' THEN 'RDS: Vehicle Rental'
            ELSE 'RDS: Connected Vehicle'
        END retention_segment
    FROM {{ ref('fct_user_rideshare_trips') }} a
    LEFT JOIN {{ ref('util_calendar') }} b ON CAST(a.request_local_time AS DATE) = b.date
    WHERE b.year >= 2020
), others AS (
    SELECT
        date,
        year_week,
        year_month,
        a.user_id,
        a.contact_id,
        CASE
            WHEN partner_category = 'Food Delivery' THEN 'FDL: Food Delivery'
            WHEN partner_category = 'Shopping' THEN 'GRC: Groceries'
            WHEN partner_category = 'Courier' THEN 'PRC: Parcel'
        END retention_segment
    FROM {{ ref('agg_wm_daily_activity') }} a
    WHERE partner_category != 'TVDE'

)

SELECT * FROM (
    SELECT * FROM rds
    UNION ALL
    SELECT * FROM others
)
ORDER BY date DESC, user_id DESC
