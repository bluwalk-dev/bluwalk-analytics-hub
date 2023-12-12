WITH

source as (
    select
        *
    from {{ source('prio', 'ff_invoice_transactions') }} 
),

transformation as (
    
    SELECT
        TO_HEX(MD5(timestamp || card_name || quantity)) as transaction_key,
        *
    FROM (
        SELECT
            CAST(FATURA AS STRING) invoice_nr,
            CAST(DATA_FATURA AS DATE) invoice_date,
            UPPER(CAST(POSTO AS STRING)) as station_name,
            CAST(TIPO_DE_REDE AS STRING) as network_type,
            DATETIME(CAST(data AS DATE), CAST(HORA AS TIME)) local_date_time,
            TIMESTAMP(DATETIME(CAST(data AS DATE), CAST(HORA AS TIME)), 'Europe/Lisbon') timestamp,
            CAST(DATA AS DATE) as date,
            CAST(HORA AS TIME) as time,
            CAST(REPLACE(REPLACE(CART__O, ' ', ''), CHR(160), '') AS INT64) card_full_nr,
            CAST(REPLACE(DESC_CART__O, 'MUVE', '') AS INTEGER) card_nr,
            CONCAT('P', FORMAT('%06d', CAST(REPLACE(DESC_CART__O, 'MUVE', '') AS INTEGER))) card_name,
            CAST(DESC_CART__O AS STRING) card_description,
            CAST(LITROS / POW(10,(LENGTH(CAST(LITROS AS STRING))-2)) AS NUMERIC) quantity,
            CAST(COMBUST__VEL AS STRING) as product,
            CAST(REPLACE(REPLACE(RECIBO, ' ', ''), CHR(160), '') AS STRING) as receipt_nr,
            CAST(VALOR_L__QUIDO / POW(10,(LENGTH(CAST(VALOR_L__QUIDO AS STRING))-2)) AS NUMERIC) value_net,
            CAST(IVA / 100 AS NUMERIC) value_vat,
            CAST(TOTAL / POW(10,(LENGTH(CAST(TOTAL AS STRING))-2)) AS NUMERIC) value_total,
            CAST(KM_S AS INTEGER) vehicle_mileage,
            CAST(REPLACE(REPLACE(ID_CONDUTOR, ' ', ''), CHR(160), '') AS STRING) driver_id,
            CAST(CAST(VALOR_UNIT_ AS INTEGER)/1000 AS NUMERIC) price,
            CAST(CAST(VALOR_REF AS INTEGER) / 1000 AS NUMERIC) reference_price,
            CAST(VALOR_DESC_ AS NUMERIC) discount_value,
            CAST(CLIENTE AS STRING) customer,
            CAST(TIPO_DE_PAGAMENTO AS STRING) payment_format

        FROM source
    )
)

SELECT * FROM transformation