WITH daily_service AS (
    SELECT
        date,
        year_month,
        count(*) service_vehicles
    FROM {{ ref('int_fleet_service_vehicles_per_day_list') }}
    GROUP BY date, year_month
)

SELECT
    year_month,
    ROUND(AVG(service_vehicles),0) service_vehicles
FROM daily_service
GROUP BY year_month
ORDER BY year_month DESC