with

source as (
    select
        *
    from {{ source('miio', 'ev_transactions') }}
),

transformation as (

    SELECT
        id as transaction_id,
        posto as station_id,
        tomada as plug_id,
        TIMESTAMP(CAST(dataInicio AS DATETIME), 'Europe/Lisbon') as start_timestamp,
        CAST(dataInicio AS DATETIME) as start_date,
        TIMESTAMP(CAST(dataFim AS DATETIME), 'Europe/Lisbon') as stop_timestamp,
        CAST(dataFim AS DATETIME) as stop_date,
        ROUND(CAST(duracao AS FLOAT64)*24*60, 2) as duration_min,
        CAST(REGEXP_EXTRACT(energia, r'(\d+\.?\d*)') AS FLOAT64) as quantity,
        CAST(cartao AS STRING) as full_card_name,
        CONCAT('M', LPAD(RIGHT(CAST(cartao AS STRING), 6), 6, '0')) AS card_name,
        CAST(colaborador AS STRING) as driver_name,
        ROUND(CAST(custoTotal/1.23 AS NUMERIC),2) AS net_value,
        ROUND(CAST(custoTotal AS NUMERIC),2) AS total_value,
        CAST(estado AS STRING) as status,
        CAST(fatura AS STRING) as invoice_url,
        TIMESTAMP_SECONDS(CAST(loadTimestampEpoch/1000 AS INT64)) as load_timestamp,
        CAST(loadUser AS STRING) as load_user
    FROM source
)

SELECT 
    TO_HEX(MD5(stop_timestamp || card_name || quantity)) as transaction_key,
    *
FROM transformation