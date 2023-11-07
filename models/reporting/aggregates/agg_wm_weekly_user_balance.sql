SELECT
    contact_id,
    statement,
    SUM(amount) AS balance
FROM {{ ref('fct_user_financial_transactions') }}
GROUP BY contact_id, statement