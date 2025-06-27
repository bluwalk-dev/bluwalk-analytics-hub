WITH ordered AS (
  SELECT
    vehicle_id,
    status,
    mov_dt,
    LEAD(mov_dt) OVER (
      PARTITION BY vehicle_id
      ORDER BY mov_dt
    ) AS next_time
  FROM {{ ref('stg_odoo_drivfit__fleet_vehicle_status') }}
)

SELECT
    a.vehicle_id,
    b.vehicle_license_plate,
    a.mov_dt AS service_start,
    COALESCE(
        next_time,
        DATETIME(CURRENT_TIMESTAMP(), 'Europe/Lisbon')
    ) AS service_end
FROM ordered a
LEFT JOIN {{ ref('dim_fleet_vehicles') }} b ON a.vehicle_id = b.vehicle_id
WHERE a.status = 'service'
ORDER BY a.vehicle_id, a.mov_dt