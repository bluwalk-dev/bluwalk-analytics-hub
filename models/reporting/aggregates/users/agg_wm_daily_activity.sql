{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

SELECT * FROM (
    SELECT DISTINCT
        c.date,
        c.year_week,
        c.year_month,
        a.user_id,
        a.contact_id,
        b.partner_key,
        b.partner_marketplace,
        b.partner_category,
        b.partner_name
    FROM {{ ref('fct_financial_user_transaction_lines') }} a
    LEFT JOIN {{ ref('fct_work_orders') }} b ON a.transaction_hash = b.work_order_hash
    LEFT JOIN {{ ref('util_calendar') }} c ON a.date = c.date
    WHERE 
        a.account_type = 'user' AND
        b.work_order_id IS NOT NULL AND
        extract(year from a.date) >= 2020
)
ORDER BY date DESC, user_id DESC