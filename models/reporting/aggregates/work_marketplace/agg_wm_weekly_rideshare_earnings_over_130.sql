SELECT
    contact_id,
    statement,
    LEAST(COUNT(DISTINCT date),7) AS total_worked_days,
    ROUND(LEAST(SUM(greater_than_or_equal_to_130),7) / LEAST(COUNT(DISTINCT date),7) * 100, 2) AS percentage_over_130
FROM (
    SELECT 
        contact_id,
        date, 
        statement,
        CASE WHEN SUM(amount) >= 130 THEN 1 ELSE 0 END AS greater_than_or_equal_to_130
    from {{ ref('fct_financial_user_transactions') }}
    where product_name IN ('Trips - Uber', 'Trips - Bolt') and date >= '2023-01-01'
    group by date, statement, contact_id
)
GROUP BY contact_id, statement