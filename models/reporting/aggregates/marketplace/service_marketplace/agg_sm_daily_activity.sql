{{ config(materialized='table') }}

SELECT 
    *
FROM (
    SELECT * FROM {{ ref('agg_sm_daily_fuel_energy_activity') }}
    UNION ALL
    SELECT * FROM {{ ref('agg_sm_daily_vehicle_connected_activity') }}
    UNION ALL
    SELECT * FROM {{ ref('agg_sm_daily_vehicle_rental_activity') }}
)
ORDER BY date DESC