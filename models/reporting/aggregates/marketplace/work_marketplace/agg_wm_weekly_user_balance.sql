SELECT
    contact_id,
    statement,
    ROUND(SUM(amount),2) AS balance
FROM {{ ref('fct_user_financial_transactions') }}
GROUP BY contact_id, statement