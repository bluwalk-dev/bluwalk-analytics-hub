SELECT
    a.work_order_id,
    a.work_order_name,
    a.work_order_status,
    a.work_order_hash,
    a.statement,
    a.contact_id,
    c.user_id,
    a.user_partner_uuid,
    a.location,
    a.start_date,
    a.end_date,
    d.year_month,
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
FROM bluwalk-analytics-hub.staging.stg_odoo_bw_trips a
LEFT JOIN {{ ref('dim_partners') }} b ON a.res_sales_partner_id = b.sales_partner_id
LEFT JOIN bluwalk-analytics-hub.core.core_users c ON a.contact_id = c.contact_id
LEFT JOIN {{ ref('util_calendar') }} d ON a.end_date = d.date
WHERE 
    b.partner_marketplace = 'Work' AND
    d.year_month IS NOT NULL

