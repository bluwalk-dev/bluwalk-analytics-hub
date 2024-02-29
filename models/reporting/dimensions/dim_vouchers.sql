SELECT 
    a.id as voucher_id,
    a.name as voucher_name,
    a.expire_date as voucher_expire_date,
    a.redemption_date as voucher_redemption_date,
    a.redemption_transaction_id as voucher_redemption_transaction_id,
    a.redemption_cycle as voucher_redemption_statement,
    a.voucher_type_id as voucher_type_id,
    b.name as voucher_type_name,
    a.partner_id as contact_id,
    a.amount as voucher_amount,
    a.is_expired as is_voucher_expired,
    a.is_redeemed as is_voucher_redeemed,
    a.ref as voucher_reference,
    a.create_date as voucher_create_date
FROM {{ ref('stg_odoo__vouchers') }} a  -- Source table: staged fleet vehicles data
LEFT JOIN {{ ref('stg_odoo__voucher_types') }} b ON a.voucher_type_id = b.id
