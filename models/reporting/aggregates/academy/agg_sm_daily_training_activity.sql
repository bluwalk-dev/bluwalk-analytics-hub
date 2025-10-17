WITH training_payments AS (
    SELECT 
        cast(so.date_order as date) date,
        so.partner_id AS contact_id,
        u.user_id,
        CASE
            WHEN STRPOS(sol.name,'ONLINE')>0 THEN '7a05ff42f1a4bb8765401c9855e5c943'
            ELSE 'f690a48cb5a6653005e387a28ee2faae'
        END partner_key
    FROM bluwalk-analytics-hub.staging.stg_odoo_bw_sale_order_lines sol
    LEFT JOIN bluwalk-analytics-hub.staging.stg_odoo_bw_sale_orders so ON sol.order_id = so.id
    LEFT JOIN {{ ref('dim_users') }} u ON so.partner_id = u.contact_id
    WHERE 
        sol.event_id IS NOT NULL AND
        price_total = 50 AND
        so.state = 'sale'
)

SELECT DISTINCT
    b.date,
    b.year_week,
    b.year_month,
    a.user_id,
    a.contact_id,
    a.partner_key,
    c.partner_marketplace,
    c.partner_category,
    c.partner_name
FROM training_payments a
LEFT JOIN {{ ref('util_calendar') }} b ON a.date = b.date
LEFT JOIN {{ ref('dim_partners') }} c ON a.partner_key = c.partner_key