WITH users_in_current_debts AS (
    SELECT DISTINCT contact_id FROM {{ ref('fct_deals') }}
    WHERE deal_pipeline_id = '197150438' and is_closed = false
)

SELECT 
    a.user_id,
    b.user_email,
    b.user_vat,
    a.risk_balance amount_total,
    CASE
        WHEN (a.risk_accounting_balance IS NULL OR a.risk_accounting_balance != a.risk_balance) THEN TRUE
        ELSE FALSE
    END to_invoice,
FROM {{ ref('base_hubspot_contacts') }} a
LEFT JOIN {{ ref('dim_users') }} b ON a.user_id = b.user_id
LEFT JOIN users_in_current_debts c ON a.contact_id = c.contact_id
WHERE 
  a.risk_balance < 0 and
  a.risk_deposit_amount = 0 and
  a.idle_work_marketplace IS NOT NULL AND
  a.idle_work_marketplace > 15 AND
  a.idle_work_marketplace < 90 AND
  c.contact_id IS NULL
