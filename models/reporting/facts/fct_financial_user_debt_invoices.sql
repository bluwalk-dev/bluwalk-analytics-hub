SELECT 
    id,
    name,
    year_month,
    year_week,
    date,
    invoice_date,
    invoice_due_date,
    reference,
    contact_id,
    payment_state,
    amount_untaxed,
    amount_tax,
    amount_total,
    amount_due
FROM {{ ref('fct_accounting_moves') }} am
WHERE 
    journal_id = 99 AND
    move_state = 'posted'