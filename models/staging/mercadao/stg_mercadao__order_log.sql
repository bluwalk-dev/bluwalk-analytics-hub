with

source as (
    select
        *
    from {{ source('mercadao', 'order_log') }} 
),

transformation as (

    SELECT
    
        CAST(id AS STRING) AS partner_account_uuid,
        CAST(date AS date) AS date,
        CAST(amount AS NUMERIC) AS nr_orders,
        CAST(orderType AS STRING) AS order_type,
        TIMESTAMP_MILLIS(extractedEpoch) AS extraction_timestamp

    FROM source

)

SELECT * FROM transformation