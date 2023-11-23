SELECT
    a.work_order_id,
    a.work_order_name,
    a.work_order_status,
    a.statement,
    a.contact_id,
    c.user_id,
    a.user_partner_uuid,
    a.location,
    a.start_date,
    a.end_date,
    a.sales_gross,
    a.sales_net,
    a.sales_taxes,
    a.sales_tax_rate,
    a.partner_fee_gross,
    a.partner_fee_net,
    a.partner_fee_taxes,
    a.partner_fee_tax_rate,
    a.partner_payout,
    b.partner_key,
    b.partner_name,
    b.partner_marketplace,
    b.partner_category,
    b.partner_contact_id,
    b.sales_partner_id,
    a.nr_trips
FROM {{ ref('stg_odoo__trips') }} a
LEFT JOIN {{ ref('dim_partners') }} b ON a.res_sales_partner_id = b.sales_partner_id
LEFT JOIN {{ ref('dim_users') }} c ON a.contact_id = c.contact_id
WHERE b.partner_marketplace = 'Work'

