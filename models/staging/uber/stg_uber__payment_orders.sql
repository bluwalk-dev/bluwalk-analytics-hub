with

source as (
    select
        *
    from {{ source('uber', 'payments_order') }}
),

transformation as (

    SELECT DISTINCT 
        CAST(transactionUUID AS STRING) transaction_uuid,
        CAST(driverUUID AS STRING) partner_account_uuid,
        CAST(firstName AS STRING) first_name,
        CAST(lastName AS STRING) last_name,
        CAST(tripUUID AS STRING) trip_uuid,
        CAST(description AS STRING) transaction_description,
        CAST(orgName AS STRING) org_name,
        CAST(orgAltName AS STRING) org_alt_name,
        TIMESTAMP(CAST(localDateTime AS DATETIME), 'Europe/Lisbon') as timestamp,
        CAST(amount AS NUMERIC) amount,
        CAST(localDateTime AS DATETIME) local_datetime
    FROM source

)

select * from transformation