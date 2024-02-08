SELECT
    destination_code_iata,
    flight_ident_iata,
    origin_name,
    flight_scheduled_in,
    flight_seats
FROM {{ ref("base_flight_aware_scheduled_arrivals") }}
WHERE flight_scheduled_in > current_timestamp()
ORDER BY destination_code_iata, flight_scheduled_in ASC
LIMIT 150