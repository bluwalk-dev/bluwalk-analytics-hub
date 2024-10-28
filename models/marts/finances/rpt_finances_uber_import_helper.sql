SELECT
    a.partner_account_uuid,
    a.contact_id,
    a.user_id,
    CASE
        WHEN d.receiver_transaction_account_id IS NULL THEN FALSE
        ELSE TRUE
    END as is_forwarded,
    IFNULL(d.receiver_transaction_account_id, b.transaction_account_id) transaction_account_id,
    IFNULL(d.receiver_analytic_account_id, c.analytic_account_id) analytic_account_id
FROM {{ ref("dim_partners_accounts") }} a
LEFT JOIN {{ ref('dim_transaction_accounts') }} b on a.user_id = b.user_id
LEFT JOIN {{ ref("dim_accounting_analytic_accounts") }} c ON a.contact_id = c.analytic_account_owner_contact_id
LEFT JOIN {{ ref("base_odoo_transaction_account_forwarding") }} d ON a.contact_id = d.sender_contact_id
WHERE 
    a.sales_partner_id = 1 AND 
    a.partner_account_uuid IS NOT NULL AND
    c.analytic_account_type = 'User'