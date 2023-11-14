SELECT
    user_id,
    MAX(date) groceries_last_activity
FROM {{ ref("base_mercadao_orders") }}
WHERE 
    date > DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY) AND
    user_id IS NOT NULL
GROUP BY user_id