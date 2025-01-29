WITH park_vehicles AS (
    SELECT *
    FROM {{ ref("stg_drivfit__vehicle_current_status") }}
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