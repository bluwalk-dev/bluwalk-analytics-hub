SELECT
    c.date,
    c.year_month,
    c.year_week,
    apr.amount amount_recovered,
    am.amount_total invoice_amount_total,
    aj.journal_name payment_journal_name,
    aj.journal_type payment_journal_type,
    am.contact_id,
    am.id invoice_id,
    am.name invoice_name,
    am.invoice_date invoice_date,
    CASE
        WHEN DATE_DIFF(c.date, am.invoice_date, DAY)<=7 THEN 'fast'
        ELSE 'slow'
    END collection_speed
FROM {{ ref('stg_odoo__account_partial_reconciles') }} apr
LEFT JOIN {{ ref('util_calendar')}} c ON c.date = apr.max_date
LEFT JOIN {{ ref('fct_accounting_move_lines')}} aml_debit on apr.debit_move_id = aml_debit.id
LEFT JOIN {{ ref('fct_accounting_move_lines')}} aml_credit on apr.credit_move_id = aml_credit.id
LEFT JOIN {{ ref('dim_accounting_journals')}} aj on aj.journal_id = aml_credit.journal_id
LEFT JOIN {{ ref('fct_accounting_moves') }} am on aml_debit.move_id = am.id
WHERE 
    am.journal_id = 99 AND 
    am.move_state = 'posted' AND
    am.financial_system = 'odoo_ce'
ORDER BY date DESC