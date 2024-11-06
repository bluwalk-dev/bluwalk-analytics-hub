WITH legal_agreements AS (
    SELECT DISTINCT contact_id
    FROM {{ ref('dim_legal_agreements') }}
    WHERE 
        legal_agreement_state = 'accepted' AND
        legal_agreement_template_name IN ('Individual Agreement Contract', 'Contrato de Prestação de Serviços - Particulares') AND
        contact_id IS NOT NULL
)

SELECT
    a.partner_account_uuid,
    a.contact_id,
    a.user_id,
    e.user_name,
    e.user_location,
    CASE
        WHEN d.receiver_transaction_account_id IS NULL THEN FALSE
        ELSE TRUE
    END as is_forwarded,
    IFNULL(d.receiver_transaction_account_id, b.transaction_account_id) transaction_account_id,
    IFNULL(d.receiver_analytic_account_id, c.analytic_account_id) analytic_account_id,
    CASE
        WHEN f.contact_id IS NULL THEN 'not_accepted'
        ELSE 'accepted'
    END as legal_agreement_status,
    a.sales_partner_id
FROM {{ ref("dim_partners_accounts") }} a
LEFT JOIN {{ ref('dim_transaction_accounts') }} b on a.user_id = b.user_id
LEFT JOIN {{ ref("dim_accounting_analytic_accounts") }} c ON a.contact_id = c.analytic_account_owner_contact_id
LEFT JOIN {{ ref("base_odoo_transaction_account_forwarding") }} d ON a.contact_id = d.sender_contact_id
LEFT JOIN {{ ref("dim_users") }} e ON a.contact_id = e.contact_id
LEFT JOIN legal_agreements f ON a.contact_id = f.contact_id
WHERE 
    c.analytic_account_type = 'User'