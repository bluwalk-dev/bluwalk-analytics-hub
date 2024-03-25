{{ config(materialized='table') }}

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
    SELECT * FROM {{ ref('agg_sm_daily_fuel_energy_activity') }}
    UNION ALL
    SELECT * EXCEPT (vehicle_fuel_type) FROM {{ ref('agg_sm_daily_vehicle_connected_activity') }}
    UNION ALL
    SELECT * EXCEPT (vehicle_fuel_type) FROM {{ ref('agg_sm_daily_vehicle_rental_activity') }}
)
ORDER BY date DESC, user_id DESC