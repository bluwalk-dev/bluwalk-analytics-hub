SELECT
    contact_id,
    statement,
    ROUND(SUM(amount),2) AS balance
FROM {{ ref('fct_financial_user_transactions') }}
GROUP BY contact_id, statement