SELECT
    user_id,
    MAX(end_date) food_delivery_last_activity
FROM {{ ref("fct_user_job_orders") }}
WHERE
    partner_category = 'Food Delivery'
    AND user_id IS NOT NULL
    AND end_date > DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)
GROUP BY
    user_id