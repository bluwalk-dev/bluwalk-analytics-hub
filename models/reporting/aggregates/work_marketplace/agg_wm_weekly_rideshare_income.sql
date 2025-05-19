select
    contact_id,
    statement,
    ROUND(SUM(amount),2) total_income,
from {{ ref('fct_financial_user_transactions') }}
WHERE product_name IN ('Trips - Uber', 'Trips - Bolt')
GROUP BY contact_id, statement