SELECT
    *
FROM 
    {{ ref('stg_odoo__transaction_accounts') }}