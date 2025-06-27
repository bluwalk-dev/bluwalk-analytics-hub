SELECT
    date,
    count(*) service_vehicles
FROM {{ ref('int_fleet_service_vehicles_per_day_list') }}
GROUP BY date
ORDER BY date DESC