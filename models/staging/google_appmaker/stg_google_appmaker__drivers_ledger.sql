with

source as (
    select
        *
    from {{ source('appmaker', 'driver_ledger_table') }}
),

transformation as (

    SELECT
    
        CASE
            WHEN driver_operation_id = '' THEN NULL
            ELSE driver_operation_id 
        END driver_operation_id,
        CAST(code as STRING) code,
        CAST(description AS STRING) description,
        CASE
            WHEN payment_nif IS NULL THEN NULL
            ELSE CAST(CONCAT("PT", payment_nif) AS STRING)
        END vat,
        SAFE_CAST(payment_cycle AS INT64) payment_cycle,
        CAST(amount AS NUMERIC) amount

    FROM source

)

SELECT * FROM transformation

