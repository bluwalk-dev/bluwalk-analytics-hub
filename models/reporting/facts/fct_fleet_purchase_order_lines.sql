SELECT
    a.id as po_line_id,
    cast(a.date_planned as date) as date,
    a.name as po_line_name,
    a.vehicle_id,
    a.product_id,
    a.repair_order_id,
    a.partner_id as supplier_partner_id,
    b.contact_name as supplier_name,
    a.state as po_line_state,
    a.order_id,
    a.price_subtotal as price_net,
    a.price_total as price_gross
FROM {{ ref('stg_odoo_drivfit__purchase_order_lines') }} a
LEFT JOIN bluwalk-analytics-hub.core.core_contacts_flt b ON a.partner_id = b.contact_id