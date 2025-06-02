WITH monthly_odometer AS (
    SELECT
        vehicle_id,
        year_month,
        odometer,
        ROW_NUMBER() OVER (PARTITION BY vehicle_id, year_month ORDER BY date DESC) AS rn
    FROM {{ ref('fct_fleet_vehicle_odometers') }}
)

SELECT
    a.vehicle_id,
    b.vehicle_deal_id,
    CAST(a.year_month AS INT64) AS year_month,
    a.odometer
FROM monthly_odometer a
LEFT JOIN {{ ref('dim_fleet_vehicles') }} b ON a.vehicle_id = b.vehicle_id
WHERE rn = 1
ORDER BY vehicle_id, year_month DESC