{{ config(materialized='table') }}

SELECT
    destination_location_id location_id,
    CONCAT(
        destination_airport_name, ' (',
        destination_code_iata, ')'
    ) airport_name,
    destination_navigation_link airport_url,
    flight_ident_iata,
    origin_city flight_origin_city,
    CAST(flight_scheduled_in AS DATE) arrival_date,
    FORMAT('%02d:%02d', 
        EXTRACT(HOUR FROM flight_scheduled_in),
        EXTRACT(MINUTE FROM flight_scheduled_in)
    ) AS arrival_time,
    flight_seats expected_passengers
FROM {{ ref("base_flight_aware_scheduled_arrivals") }}
WHERE flight_scheduled_in > current_timestamp()
ORDER BY destination_code_iata, flight_scheduled_in ASC