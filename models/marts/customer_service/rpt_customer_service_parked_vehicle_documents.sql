{{ 
  config(
    tags = ['google_sheets'],
    meta = 
        { "
            url": "https://docs.google.com/spreadsheets/d/13G8KHJhiiW4EZHlxoqweW7AEGHxDTCgOB8_4YZj5ths" 
        }
  ) 
}}

WITH park_vehicles AS (
    SELECT
        c.vehicle_license_plate,
        c.vehicle_name,
        b.station_name,
        b.station_location,
        a.status,
        a.stage
    FROM {{ ref('stg_odoo_drivfit__fleet_vehicles') }} a
    LEFT JOIN {{ ref('dim_fleet_stations') }} b ON a.station_id = b.station_id
    LEFT JOIN {{ ref('dim_fleet_vehicles') }} c ON a.id = c.vehicle_id
    WHERE
        stage = 'active' AND 
        status = 'park'
)

SELECT *
FROM (

    SELECT
        a.vehicle_license_plate,
        a.vehicle_name,
        a.station_name,
        a.station_location as location_name,
        'Uber' as partner,
        b.document_type_name,
        b.document_status,
        CAST(b.document_expires_at AS DATE) expiration_date
    FROM park_vehicles a
    LEFT JOIN {{ ref("base_uber_vehicle_documents") }} b ON 
        a.vehicle_license_plate = b.vehicle_plate AND
        a.station_location = b.location_name

    UNION ALL

    SELECT
        a.vehicle_license_plate,
        a.vehicle_name,
        a.station_name,
        a.station_location as location_name,
        'Bolt' as partner,
        b.document_name,
        b.document_status,
        b.expiration_date
    FROM park_vehicles a
    LEFT JOIN {{ ref("base_bolt_vehicle_documents") }} b ON 
        a.vehicle_license_plate = b.vehicle_plate AND
        a.station_location = b.location_name
)
ORDER BY vehicle_license_plate, partner