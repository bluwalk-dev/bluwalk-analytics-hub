select
    a.id,
    a.date,
    a.name,
    a.partner_id entity_id,
    d.contact_full_name contact_name,
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
    a.tax_line_id,
    a.analytic_account_id,
    e.analytic_account_name,
    a.full_reconcile_id,
    a.payment_id,
    a.create_date
from {{ ref('stg_odoo__account_move_lines') }} a
left join {{ ref('dim_accounting_journals') }} b ON a.journal_id = b.journal_id
left join {{ ref('dim_accounting_accounts') }} c ON a.account_id = c.account_id
LEFT JOIN {{ ref('dim_contacts') }} d ON a.partner_id = d.contact_id
LEFT JOIN {{ ref('dim_accounting_analytic_accounts') }} e ON a.analytic_account_id = e.analytic_account_id
ORDER BY date DESC