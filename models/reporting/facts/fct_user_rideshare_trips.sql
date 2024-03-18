SELECT 
    x.*,
    CASE
        -- /////// REGEX BASED EXTRACTION \\\\\\\\
        WHEN 
            REGEXP_EXTRACT(address_pickup, r'(\d{4}-\d{3})') IS NOT NULL 
            THEN REGEXP_EXTRACT(address_pickup, r'(\d{4}-\d{3})')

        -- /////// RULE BASED EXTRACTION \\\\\\\\
        -- Faro Airport Matching
        WHEN
            REGEXP_CONTAINS(address_pickup, r'(?i)Faro') AND
            (REGEXP_CONTAINS(address_pickup, r'(?i)Aeroporto') OR REGEXP_CONTAINS(address_pickup, r'(?i)Airport'))
            OR address_pickup = 'Kiss & Fly, Aéroport d Algarve Faro (FAO)'
            THEN '8006-901'
        -- Lisbon Airport Matching
        WHEN 
            ((REGEXP_CONTAINS(address_pickup, r'(?i)Lisbon') OR REGEXP_CONTAINS(address_pickup, r'(?i)Lisboa')) AND
            (REGEXP_CONTAINS(address_pickup, r'(?i)Aeroporto') OR REGEXP_CONTAINS(address_pickup, r'(?i)Airport')))
            OR address_pickup LIKE 'Terminal 1, Departures%'
            OR address_pickup LIKE 'Terminal 1, Partidas%'
            OR address_pickup = 'Terminal 1'
            THEN '1700-008'
        -- Porto Airport Matching
        WHEN 
            ((REGEXP_CONTAINS(address_pickup, r'(?i)Porto') OR REGEXP_CONTAINS(address_pickup, r'(?i)Oporto')) AND
            (REGEXP_CONTAINS(address_pickup, r'(?i)Aeroporto') OR REGEXP_CONTAINS(address_pickup, r'(?i)Airport')))
            OR address_pickup = 'Arrivals'
            OR address_pickup = 'Chegadas'
            OR address_pickup = 'Partidas'
            OR address_pickup = 'Departures'
            OR address_pickup = 'Desembarque'
            THEN '4470-558'
        -- Porto Parque Nascente Shopping
        WHEN 
            REGEXP_CONTAINS(address_pickup, r'(?i)Parque Nascente')
            THEN '4435-182'
        -- Porto Estação de Campanhã
        WHEN 
            address_pickup = 'Campanhã Subway Station'
            OR address_pickup = 'Largo da Estação (Rotunda), Campanhã'
            OR address_pickup = 'Largo da Estação, Campanhã'
            OR address_pickup = 'Pinheiro de Campanhã Street'
            OR address_pickup = 'Kiss&Ride, Campanhã'
            OR address_pickup = 'Metro Campanhã, Campanhã'
            THEN '4300-430'
    END address_pickup_zip
FROM (
    SELECT * FROM {{ ref('base_uber_trips') }}
    UNION ALL
    SELECT * FROM {{ ref('base_bolt_trips') }}
    UNION ALL
    SELECT * FROM {{ ref('base_freenow_trips') }}
) x
ORDER BY request_local_time DESC