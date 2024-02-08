with

source as (
    SELECT *
    FROM {{ source('flight_aware', 'scheduled_arrivals') }}
),

transformation as (

    select
        
        -- KLM
        replace(
            -- Easyjet
            replace(replace(flight_ident_iata, 'EC', 'U2'), 'DS', 'U2'),
            'WA', 'KL'
            )
        flight_ident_iata,
        -- KLM
        replace(
            -- Easyjet
            replace(replace(flight_operator_iata, 'EC', 'U2'), 'DS', 'U2'),
            'WA', 'KL'
            )
        flight_operator_iata,
        origin_code_iata,
        destination_code_iata,
        flight_aircraft_type,
        flight_scheduled_in,
        flight_estimated_in,
        (flight_seats_cabin_business + flight_seats_cabin_coach + flight_seats_cabin_first) flight_seats,
        TIMESTAMP_MILLIS(load_timestamp) load_timestamp

    from source

)

select * from transformation
