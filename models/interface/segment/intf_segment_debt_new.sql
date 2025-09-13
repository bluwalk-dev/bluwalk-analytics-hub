WITH 

users_in_current_debts AS (
    SELECT DISTINCT contact_id 
    FROM bluwalk-analytics-hub.core.core_hubspot_deals
    WHERE deal_pipeline_id = '197150438' and is_closed = false
),

user_debt_invoices AS (
    SELECT
        a.user_id,
        SUBSTR( STRING_AGG(invoice_link, '; '), 1, LENGTH(STRING_AGG(invoice_link, '; ')) - 1 ) AS invoice_link
    FROM {{ ref('dim_users') }} a
    INNER JOIN {{ ref('fct_financial_user_debt_invoices') }} b ON a.contact_id = b.contact_id
    WHERE b.payment_state != 'paid'
    GROUP BY a.user_id 
)

SELECT 
    a.user_id,
    b.user_name,
    b.user_email,
    b.user_vat,
    a.risk_balance amount_total,
    CASE
        WHEN (a.risk_accounting_balance IS NULL OR a.risk_accounting_balance != a.risk_balance) THEN TRUE
        ELSE FALSE
    END to_invoice,
    d.invoice_link,
    a.risk_account_idle_time
FROM bluwalk-analytics-hub.staging.stg_hubspot_contacts a
LEFT JOIN {{ ref('dim_users') }} b ON a.user_id = b.user_id
LEFT JOIN users_in_current_debts c ON a.contact_id = c.contact_id
LEFT JOIN user_debt_invoices d ON a.user_id = d.user_id
WHERE 
  a.risk_balance < 0 and
  a.risk_deposit_amount = 0 and
  a.idle_work_marketplace IS NOT NULL AND
  a.risk_account_idle_time > 15 AND
  a.risk_account_idle_time < date_diff(current_date(), DATE(2024,3,31), DAY) AND
  c.contact_id IS NULL
