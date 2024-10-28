with

source as (
    select
        *
    from {{ source('google_sheets', 'transaction_account_forwarding') }}
),

transformation as (

    SELECT
    
        TRIM(sender_partner_vat) AS sender_partner_vat,
        TRIM(receiver_partner_vat) AS receiver_partner_vat,

    FROM source

)

SELECT * FROM transformation

