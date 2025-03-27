SELECT 
    payment_cycle, 
    contact_id, 
    -1 * sum(amount) fuel_spending
FROM {{ ref('fct_financial_user_transaction_lines') }} a
WHERE 
    group_id = 1 AND 
    account_type = 'user'
GROUP BY payment_cycle, contact_id
ORDER BY contact_id desc, payment_cycle desc