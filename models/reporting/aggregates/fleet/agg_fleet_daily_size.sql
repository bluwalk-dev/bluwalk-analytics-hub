SELECT
    date,
    count(*) fleet_size
FROM {{ ref('int_fleet_vehicles_per_day_list') }}
GROUP BY date