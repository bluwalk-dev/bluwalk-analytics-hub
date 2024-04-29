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
    -- Fuel and Energy Services
    SELECT * FROM {{ ref('agg_sm_daily_fuel_energy_activity') }}
    UNION ALL
    -- Vehicle Registration Services
    SELECT * EXCEPT (vehicle_fuel_type) FROM {{ ref('agg_sm_daily_vehicle_connected_activity') }}
    UNION ALL
    -- Vehicle Rental Services
    SELECT * EXCEPT (vehicle_fuel_type) FROM {{ ref('agg_sm_daily_vehicle_rental_activity') }}
    UNION ALL
    -- Training Services
    SELECT * FROM {{ ref('agg_sm_daily_training_activity') }}
    UNION ALL
    -- Insurance Services
    SELECT * FROM {{ ref('agg_sm_daily_insurance_activity') }}
    -- Subscription Services
)
ORDER BY date DESC, user_id DESC