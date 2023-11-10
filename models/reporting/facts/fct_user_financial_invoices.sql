WITH payments AS (
    SELECT * FROM {{ ref('fct_accounting_move_lines') }} aml
    LEFT JOIN {{ ref('stg_odoo__account_payments') }} ap ON aml.payment_id = ap.id
    WHERE 
        aml.full_reconcile_id IS NOT NULL AND
        aml.payment_id IS NOT NULL
)

SELECT 
    b.id,
    b.name,
    b.year_month,
    b.date,
    b.invoice_date,
    b.invoice_due_date,
    b.reference,
    b.contact_id,
    b.payment_state,
    b.amount_untaxed,
    b.amount_tax,
    b.amount_total,
    b.amount_due,
    a.create_date,
    a.payment_date
FROM
    (SELECT
        i.move_id,
        CAST(i.create_date AS TIMESTAMP) create_date,
        TIMESTAMP(CONCAT(CAST(p.payment_date AS STRING), ' 20:00:00'), 'Europe/Lisbon') payment_date
    FROM {{ ref('fct_accounting_move_lines') }} i
    LEFT JOIN payments p ON i.full_reconcile_id = p.full_reconcile_id
    WHERE
        i.move_id != p.move_id AND
        i.journal_id = 60 AND 
        i.move_state = 'posted' AND
        i.full_reconcile_id IS NOT NULL
    ) a
LEFT JOIN {{ ref('fct_accounting_moves') }} b ON a.move_id = b.id
ORDER BY date DESC
