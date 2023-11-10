SELECT
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
    a.amount_untaxed_signed amount_untaxed,
    a.amount_tax_signed amount_tax,
    a.amount_total_signed amount_total,
    a.amount_residual_signed amount_due,
    a.invoice_payment_state payment_state
FROM {{ ref('stg_odoo__account_moves') }} a
LEFT JOIN {{ ref('dim_accounting_journals') }} b ON a.journal_id = b.journal_id
LEFT JOIN {{ ref('util_calendar') }} c ON c.date = a.date