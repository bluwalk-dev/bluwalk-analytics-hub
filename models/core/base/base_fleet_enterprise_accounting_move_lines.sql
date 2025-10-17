select
    a.key,
    a.id,
    a.date,
    a.name,
    a.partner_id entity_id,
    d.accounting_contact_name contact_name,
    a.move_id,
    a.move_name,
    a.parent_state move_state,
    a.journal_id,
    b.journal_name,
    a.ref,
    a.account_id,
    c.account_name,
    c.account_code,
    a.debit,
    a.credit,
    a.balance,
    a.product_id,
    a.tax_line_id,
    a.full_reconcile_id,
    a.payment_id,
    a.create_date
from bluwalk-analytics-hub.staging.stg_odoo_ee_account_move_lines a
left join {{ ref('dim_accounting_journals') }} b ON 
    a.journal_id = b.journal_id AND 
    b.journal_financial_system_id = 4
left join {{ ref('dim_accounting_accounts') }} c ON 
    a.account_id = c.account_id AND 
    c.account_financial_system_id = 4
LEFT JOIN bluwalk-analytics-hub.core.core_contacts_ee d ON 
    a.partner_id = d.accounting_contact_id
WHERE 
    a.company_id = 5 
    AND parent_state = 'posted'