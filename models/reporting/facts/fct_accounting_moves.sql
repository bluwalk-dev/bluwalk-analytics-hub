-- Odoo CE transactions --
SELECT
    key,
    financial_system_id,
    financial_system,
    a.id,
    a.name,
    a.date,
    c.year_month,
    c.year_week,
    a.invoice_date,
    a.invoice_date_due invoice_due_date,
    a.ref reference,
    a.state move_state,
    CASE
        WHEN a.type  = 'in_invoice' THEN 'Supplier Invoice'
        WHEN a.type  = 'entry' THEN 'Payment and Misc'
        WHEN a.type  = 'in_deposit' THEN 'Supplier Deposit'
        WHEN a.type  = 'out_invoice' THEN 'Customer Invoice'
        WHEN a.type  = 'in_refund_deposit' THEN 'Supplier Deposit Refund'
        WHEN a.type  = 'in_refund' THEN 'Supplier Refund'
        WHEN a.type  = 'out_refund' THEN 'Customer Invoice Refund'
        WHEN a.type  = 'out_deposit' THEN 'Customer Deposit'
        WHEN a.type  = 'out_refund_deposit' THEN 'Customer Deposit Refund'
        ELSE NULL
    END move_type,
    a.journal_id,
    b.journal_name,
    a.partner_id contact_id,
    e.contact_vat,
    a.amount_untaxed_signed amount_untaxed,
    a.amount_tax_signed amount_tax,
    a.amount_total_signed amount_total,
    a.amount_residual_signed amount_due,
    a.invoice_payment_state payment_state,
    a.iexpress_invoice_permalink invoice_link
FROM {{ ref('stg_odoo__account_moves') }} a
LEFT JOIN {{ ref('dim_accounting_journals') }} b ON
    a.journal_id = b.journal_id AND
    a.financial_system = b.journal_financial_system
LEFT JOIN {{ ref('util_calendar') }} c ON c.date = a.date
LEFT JOIN {{ ref('dim_contacts') }} e ON a.partner_id = e.contact_id

UNION ALL

-- Odoo EE transactions --
SELECT
    TO_HEX(MD5('odoo_ee' || 'account.move' || id)) as key,
    4 as financial_system_id,
    'odoo_ee' as financial_system,
    a.id,
    a.name,
    a.date,
    c.year_month,
    c.year_week,
    a.invoice_date,
    a.invoice_date_due invoice_due_date,
    a.ref reference,
    a.state move_state,
    CASE
        WHEN a.move_type  = 'in_invoice' THEN 'Supplier Invoice'
        WHEN a.move_type  = 'entry' THEN 'Payment and Misc'
        WHEN a.move_type  = 'in_deposit' THEN 'Supplier Deposit'
        WHEN a.move_type  = 'out_invoice' THEN 'Customer Invoice'
        WHEN a.move_type  = 'in_refund_deposit' THEN 'Supplier Deposit Refund'
        WHEN a.move_type  = 'in_refund' THEN 'Supplier Refund'
        WHEN a.move_type  = 'out_refund' THEN 'Customer Invoice Refund'
        WHEN a.move_type  = 'out_deposit' THEN 'Customer Deposit'
        WHEN a.move_type  = 'out_refund_deposit' THEN 'Customer Deposit Refund'
        ELSE NULL
    END move_type,
    a.journal_id,
    b.journal_name,
    NULL as contact_id,
    d.accounting_contact_vat,
    a.amount_untaxed_signed amount_untaxed,
    a.amount_tax_signed amount_tax,
    a.amount_total_signed amount_total,
    a.amount_residual_signed amount_due,
    a.payment_state payment_state,
    CONCAT("https://enterprise.bluwalk.com/pt/my/invoices/", a.id, "?access_token=", access_token) invoice_link
FROM bluwalk-analytics-hub.staging.stg_odoo_ee_account_moves a
LEFT JOIN {{ ref('dim_accounting_journals') }} b ON
    a.journal_id = b.journal_id AND
    b.journal_financial_system = 'odoo_ee'
LEFT JOIN {{ ref('util_calendar') }} c ON c.date = a.date
LEFT JOIN bluwalk-analytics-hub.core.core_contacts_ee d ON a.partner_id = d.accounting_contact_id
WHERE company_id = 4