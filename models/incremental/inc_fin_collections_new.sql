SELECT 
    a.user_id,
    b.user_email,
    b.user_vat,
    a.risk_balance amount_total,
    CASE
        WHEN (a.risk_accounting_balance IS NULL OR a.risk_accounting_balance != risk_balance) THEN TRUE
        ELSE FALSE
    END to_invoice,
FROM {{ ref('base_hubspot_contacts') }} a
LEFT JOIN {{ ref('dim_users') }} b ON a.user_id = b.user_id
WHERE 
  a.risk_balance < 0 and
  a.risk_deposit_amount = 0 and
  a.contact_id not in (
    SELECT DISTINCT contact_id FROM {{ ref('fct_deals') }}
    WHERE deal_pipeline_id = '197150438' and is_closed = false
  ) AND
  a.idle_work_marketplace IS NOT NULL AND
  a.idle_work_marketplace > 15 AND
  a.idle_work_marketplace < 90
