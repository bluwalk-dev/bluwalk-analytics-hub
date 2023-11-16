with

source as (
    select
        *
    from {{ source('mercadao', 'account_status_log') }} 
),

transformation as (

    SELECT
    
        CAST(id AS STRING) partner_account_uuid,
        CAST(name AS STRING) shopper_name,
        CAST(citizenCard AS INTEGER) shopper_id_card,
        CAST(taxID AS INTEGER) shopper_tax_id,
        CAST(email AS STRING) shopper_email,
        CAST(phoneNumber AS STRING) shopper_phone_number,
        CAST(city AS STRING) shopper_city,
        CAST(deletedAt AS STRING) deleted_at,
        CAST(poupaMaisCard AS STRING) poupa_mais_card,
        CAST(status AS STRING) account_status,
        TIMESTAMP_MILLIS(extractedEpoch) extraction_timestamp

    FROM source

)

SELECT * FROM transformation