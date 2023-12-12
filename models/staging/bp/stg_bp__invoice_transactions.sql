WITH

source as (
    SELECT DISTINCT
        *
    FROM {{ source('bp', 'invoice_transactions') }}
),

transformation as (
    
    SELECT
        TO_HEX(MD5(timestamp || card_name || quantity)) as transaction_key,
        *
    FROM (
        SELECT
            CAST(fatura AS STRING) as invoice,
            CAST(data_fatura AS DATE) as invoice_date,
            CAST(nr_transacoes AS INTEGER) as nr_of_transactions,
            CAST(nr_fatura AS STRING) as invoice_nr,
            CAST(moeda AS STRING) as currency,
            CAST(dia_hora AS TIMESTAMP) as timestamp,
            DATETIME(CAST(dia_hora AS TIMESTAMP), 'Europe/Lisbon') local_date_time,
            CAST(nr_transacao AS INTEGER) as transaction_nr,
            CAST(nr_cartao AS INT64) as card_number,
            CONCAT('B', FORMAT('%06d', nr_cartao)) card_name,
            CAST(proprietario AS STRING) proprietario,
            CAST(matricula AS STRING) matricula,
            CAST(km AS INTEGER) as vehicle_mileage,
            CAST(dia_laboral AS BOOLEAN) as labour_day,
            UPPER(CAST(posto AS STRING)) as station_name,
            UPPER(CAST(produto AS STRING)) as product,
            ROUND(CAST(quantidade AS NUMERIC), 2) as quantity,
            CAST(preco AS NUMERIC) as price,
            CAST(valor_liquido AS NUMERIC) as value_net,
            CAST(iva AS NUMERIC) value_vat,
            CAST(total_faturar AS NUMERIC) as value_total, 
            CAST(taxa_iva AS NUMERIC) as vat_rate,
            CAST(valor_posto AS NUMERIC) as value_station_price,
            CAST(loadUser AS STRING) as load_user,
            TIMESTAMP_MILLIS(loadTimestampEpoch) AS extraction_timestamp
            
        from source
    )

)

SELECT * FROM transformation
ORDER BY invoice_date DESC