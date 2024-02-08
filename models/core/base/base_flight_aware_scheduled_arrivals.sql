WITH arrivals AS (
    SELECT * FROM
        (SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY flight_ident_iata, flight_scheduled_in  
                ORDER BY load_timestamp DESC
            ) AS __row_number
        FROM {{ ref("stg_flight_aware__scheduled_arrivals") }} 
        )
    WHERE __row_number = 1
), aircraft_airline_capacity AS (
    SELECT
        flight_operator_iata,
        flight_aircraft_type,
        MAX(flight_seats) flight_seats
    FROM {{ ref("stg_flight_aware__scheduled_arrivals") }}
    GROUP BY 
        flight_operator_iata,
        flight_aircraft_type
), aircraft_capacity AS (
    SELECT
        flight_aircraft_type,
        MAX(flight_seats) flight_seats
    FROM {{ ref("stg_flight_aware__scheduled_arrivals") }}
    GROUP BY 
        flight_aircraft_type
)

SELECT
    a.flight_ident_iata,
    a.origin_code_iata,
    e.airport_city origin_city,
    a.destination_code_iata,
    d.airport_city destination_city,
    d.airport_name destination_airport_name,
    d.airport_location_id destination_location_id,
    a.flight_operator_iata,
    a.flight_aircraft_type,
    CASE
        WHEN a.flight_seats IS NULL THEN IFNULL(b.flight_seats, c.flight_seats)
        ELSE a.flight_seats
    END flight_seats,
    a.flight_estimated_in,
    a.flight_scheduled_in
FROM arrivals a
LEFT JOIN aircraft_airline_capacity b ON a.flight_operator_iata = b.flight_operator_iata AND a.flight_aircraft_type = b.flight_aircraft_type
LEFT JOIN aircraft_capacity c ON a.flight_aircraft_type = c.flight_aircraft_type
LEFT JOIN {{ ref("stg_flight_aware__airports") }} d ON a.destination_code_iata = d.airport_iata_code
LEFT JOIN {{ ref("stg_flight_aware__airports") }} e ON a.origin_code_iata = e.airport_iata_code
WHERE flight_scheduled_in IS NOT NULL
ORDER BY a.flight_scheduled_in ASC