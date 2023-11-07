select
    contact_id,
    statement,
    SUM(amount) total_income,
from {{ ref('fct_user_financial_transactions') }}
WHERE product_name IN ('Trips - Uber', 'Trips - Bolt')
GROUP BY contact_id, statement