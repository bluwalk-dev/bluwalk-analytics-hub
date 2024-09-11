SELECT 
    c.partner_key,
    a.sales_partner_id,
    c.partner_name,
    b.contact_id,
    b.user_vat,
    b.user_id,
    b.user_name,
    a.date,
    a.partner_account_uuid,
    a.user_delivery_route,
    a.vehicle_license_plate,
    a.partner_hub,
    a.nr_deliveries,
    a.unit_value,
    a.total_value
FROM {{ ref("stg_correos_express__orders_report") }} a
LEFT JOIN {{ ref('dim_users') }} b on b.user_vat = a.user_vat
LEFT JOIN {{ ref('dim_partners') }} c ON a.sales_partner_id = c.sales_partner_id
ORDER BY date DESC