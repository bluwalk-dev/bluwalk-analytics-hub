select 
    CASE
        WHEN partner_id = 3725 THEN 'Uber'
        WHEN partner_id = 3738 THEN 'Bolt'
        ELSE NULL
    END partner,
    date, 
    name, 
    amount, 
    note
from {{ ref('stg_odoo__account_bank_statement_lines') }} `odoo_realtime.account_bank_statement_line`
where 
  extract(year from date) <= 2024 and 
  partner_id IN (3725, 3738)

UNION ALL

SELECT
    CASE
        WHEN a.partner_id = 1771 THEN 'Uber'
        WHEN a.partner_id = 2189 THEN 'Bolt'
        ELSE NULL
    END partner,
    b.date,
	a.payment_ref,
	a.amount,
    null
FROM {{ ref('stg_odoo_enterprise__account_bank_statement_lines') }} a
LEFT JOIN bluwalk-analytics-hub.staging.stg_odoo_ee_account_moves b on a.move_id = b.id
where 
	a.partner_id IN (1771, 2189) and
	b.company_id = 4 and
    b.journal_id = 47