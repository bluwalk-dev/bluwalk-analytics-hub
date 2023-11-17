with

source as (
    select
        *
    from {{ source('prio', 'ff_invoice_transactions') }} 
),

transformation as (

    SELECT
        CAST(POSTO AS STRING) posto,
        CAST(TIPO_DE_REDE AS STRING) tipo_de_rede,
        CAST(DATA AS DATE) data,
        CAST(HORA AS TIME) hora,
        CAST(REPLACE(REPLACE(CART__O, ' ', ''), CHR(160), '') AS INT64) cartao,
        CAST(DESC_CART__O AS STRING) descricao_cartao,
        CAST(LITROS / POW(10,(LENGTH(CAST(LITROS AS STRING))-2)) AS NUMERIC) litros,
        CAST(COMBUST__VEL AS STRING) combustivel,
        CAST(REPLACE(REPLACE(RECIBO, ' ', ''), CHR(160), '') AS STRING) recibo,
        CAST(VALOR_L__QUIDO / POW(10,(LENGTH(CAST(VALOR_L__QUIDO AS STRING))-2)) AS NUMERIC) valor_liquido,
        CAST(IVA / 100 AS NUMERIC) iva,
        CAST(KM_S AS INTEGER) kms,
        CAST(REPLACE(REPLACE(ID_CONDUTOR, ' ', ''), CHR(160), '') AS STRING) id_condutor,
        CAST(FATURA AS STRING) fatura,
        CAST(DATA_FATURA AS DATE) data_fatura,
        CAST(TOTAL / POW(10,(LENGTH(CAST(TOTAL AS STRING))-2)) AS NUMERIC) total,
        CAST(CAST(VALOR_UNIT_ AS INTEGER)/1000 AS NUMERIC) valor_unitario,
        CAST(CAST(VALOR_REF AS INTEGER) / 1000 AS NUMERIC) valor_referencia,
        CAST(VALOR_DESC_ AS NUMERIC) valor_desconto,
        CAST(CLIENTE AS STRING) cliente,
        CAST(TIPO_DE_PAGAMENTO AS STRING) tipo_de_pagamento

    FROM source

)

SELECT * FROM transformation