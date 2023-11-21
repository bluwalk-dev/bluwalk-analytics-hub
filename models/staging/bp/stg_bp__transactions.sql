with

source as (
    SELECT DISTINCT
        *
    FROM {{ source('bp', 'transactions_v2') }}
),

transformation as (

    select
        TO_HEX(MD5(timestamp || card_name || quantity)) as transaction_key,
        *
    from (
        select
            CAST(numeracao_negocio AS STRING) as transaction_id,
            CAST(status AS STRING) transaction_status,
            CAST(dia_hora AS DATETIME) as local_date_time,
            TIMESTAMP(dia_hora, 'Europe/Lisbon') timestamp,
            CAST(nr_cartao AS INT64) as card_number,
            CONCAT('B', FORMAT('%06d', nr_cartao)) as card_name,
            CAST(kms AS INT64) as vehicle_mileage,
            CAST(posto AS STRING) as station_name,
            CAST(nr_items AS INT64) as nr_items,
            CAST(produto AS STRING) as product,
            ROUND(CAST(quantidade AS NUMERIC),2) as quantity,
            ROUND(CAST(valor_preco_posto AS NUMERIC),2) as value_station_price,
            CAST(tipo_de_transacao AS STRING) transaction_type,
            CAST(resultado AS STRING) transaction_outcome,
            CAST(estado_confirmacao AS STRING) confirmation_status,
            CAST(valor_do_issuer AS NUMERIC) issuer_value,
            CAST(valor_do_supplier AS NUMERIC) supplier_value,
            CAST(nome_perfil AS STRING) card_profile,
            TIMESTAMP_MILLIS(load_timestamp) extraction_timestamp
            
        from source
    )

)

select * from transformation
ORDER BY timestamp desc