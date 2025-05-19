SELECT
    a.user_id,
    b.user_email,
    CASE
        WHEN LEFT(a.card_name,1) = 'B' THEN 'BP'
        WHEN LEFT(a.card_name,1) = 'P' THEN 'Prio'
        WHEN LEFT(a.card_name,1) = 'M' THEN 'Miio'
        ELSE ''
    END supplier,
    a.card_name,
    MAX(a.end_date) last_activity
FROM {{ ref('fct_service_orders_energy') }} a
LEFT JOIN {{ ref('dim_users') }} b
ON a.contact_id = b.contact_id
WHERE
  a.end_date > DATE_SUB(current_date, INTERVAL 15 DAY)
GROUP BY
  a.user_id,
  b.user_email,
  a.card_name