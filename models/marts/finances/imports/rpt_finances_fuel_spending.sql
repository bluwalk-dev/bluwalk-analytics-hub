SELECT 
    b.date, 
    contact_id, 
    -1 * sum(amount) fuel_spending
FROM {{ ref('fct_financial_user_transaction_lines') }} a
LEFT JOIN {{ ref('util_calendar') }} b on a.payment_cycle = b.year_week
WHERE 
    group_id = 1 AND 
    account_type = 'user'
GROUP BY b.date, payment_cycle, contact_id
ORDER BY contact_id desc, date desc