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
        CAST(originalTimestamp AS STRING) original_timestamp,
        CASE 
            WHEN originalTimestamp IS NULL THEN TIMESTAMP(CAST(localDateTime AS DATETIME), 'Europe/Lisbon')
            ELSE PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E*S", REPLACE(originalTimestamp, ' WET', ''))
        END as timestamp,
        CASE 
            WHEN originalTimestamp IS NULL THEN CAST(localDateTime AS DATETIME)
            ELSE PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E*S", REPLACE(originalTimestamp, ' WET', ''))
        END local_datetime,
        CAST(amount AS NUMERIC) amount,
        TIMESTAMP_MILLIS(loadTimestamp) load_timestamp
    FROM source

)

select * from transformation