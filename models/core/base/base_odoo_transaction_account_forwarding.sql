WITH transaction_accounts AS (
    SELECT
        a.user_id,
        a.user_vat,
        a.contact_id,
        b.transaction_account_id,
        c.analytic_account_id
    FROM {{ ref('dim_users') }} a
    LEFT JOIN {{ ref('dim_transaction_accounts') }} b on a.user_id = b.user_id
    LEFT JOIN {{ ref('dim_accounting_analytic_accounts') }} c on a.contact_id = c.analytic_account_owner_contact_id
    WHERE c.analytic_account_type = 'User'
)

select
    a.sender_partner_vat,
    b.user_id sender_user_id,
    b.contact_id sender_contact_id,
    b.transaction_account_id sender_transaction_account_id,
    b.analytic_account_id sender_analytic_account_id,
    a.receiver_partner_vat,
    c.user_id receiver_user_id,
    c.contact_id receiver_contact_id,
    c.transaction_account_id as receiver_transaction_account_id,
    c.analytic_account_id receiver_analytic_account_id,
FROM {{ ref('stg_google_sheets__transaction_account_forwarding') }} a
LEFT JOIN transaction_accounts b on a.sender_partner_vat = b.user_vat
LEFT JOIN transaction_accounts c on a.receiver_partner_vat = c.user_vat