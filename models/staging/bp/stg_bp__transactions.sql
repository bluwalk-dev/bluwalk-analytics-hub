with

source as (
    SELECT DISTINCT
        *
    FROM {{ source('bp', 'transactions_v3') }}
),

transformation as (

    select
        TO_HEX(MD5(timestamp || card_name || quantity)) as transaction_key,
        *
    from (
        select
            CAST(unicode AS STRING) as transaction_id,
            CAST(data AS DATE) as local_date,
            CAST(REPLACE(hora,'#','00:00:00') AS TIME) as local_time,
            DATETIME(CAST(data AS DATE), CAST(REPLACE(hora,'#','00:00:00') AS TIME)) local_date_time,
            TIMESTAMP(DATETIME(CAST(data AS DATE), CAST(REPLACE(hora,'#','00:00:00') AS TIME)), 'Europe/Lisbon') timestamp,
            CAST(cartao AS INT64) as card_number,
            CONCAT('B', FORMAT('%06d', cartao)) as card_name,
            CAST(kms AS INT64) as vehicle_mileage,
            CAST(codigo_posto AS STRING) as station_code,
            CAST(nome_posto AS STRING) as station_name,
            CAST(produto AS STRING) as product,
            ROUND(CAST(volume AS NUMERIC),2) as quantity,
            ROUND(CAST(valor_talao AS NUMERIC),2) as value_station_price,
            TIMESTAMP_MILLIS(load_timestamp) as extraction_timestamp
            
        from source
    )

)

select * from transformation
ORDER BY timestamp desc