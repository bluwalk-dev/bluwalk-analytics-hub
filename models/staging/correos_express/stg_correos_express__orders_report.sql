with

source as (

    SELECT
        *
    FROM {{ source('correos', 'correos_daily_report') }}
    
),

transformation as (

    SELECT DISTINCT

        CAST(date AS DATE) AS date,
        4 AS sales_partner_id,
        CAST(REPLACE(nif,' ','') AS STRING) AS user_vat,
        CAST(name AS STRING) AS user_name,
        CAST(correos_id AS STRING) AS partner_account_uuid,
        CAST(route AS STRING) AS user_delivery_route,
        CAST(licence_plate as STRING) AS vehicle_license_plate,
        CAST(delegation as STRING) AS partner_hub,
        CAST(number_of_deliveries as INT64) AS nr_deliveries,
        CAST(unit_value as NUMERIC) AS unit_value,
        CAST(total_value as NUMERIC) AS total_value

    FROM source

)

select * from transformation
