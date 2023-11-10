with

source as (
    select
        *
    from {{ source('appmaker', 'driver_statement') }}
),

transformation as (

    SELECT
    
        SAFE_CAST(ds_id AS INT64) ds_id,
        CASE
            WHEN payment_nif IS NULL THEN NULL
            ELSE CAST(CONCAT("PT", payment_nif) AS STRING)
        END vat,
        SAFE_CAST(payment_cycle AS INT64) payment_cycle,
        CAST(doc_url AS STRING) doc_url,
        CAST(created_timestamp AS TIMESTAMP) created_timestamp,
        CAST(created_by AS STRING) created_by,
        CAST(amount AS NUMERIC) amount,
        CAST(status AS STRING) status,
        SAFE_CAST(sent AS INT64) sent,
        SAFE_CAST(invoice_id AS INT64) invoice_id,
        CAST(driver_statement_type AS STRING) driver_statement_type,
        issued,
        tax

    FROM source

)

SELECT * FROM transformation

