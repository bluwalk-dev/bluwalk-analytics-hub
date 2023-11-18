SELECT 
    b.contact_id,
    b.user_id,
    b.user_name,
    a.date,
    a.user_delivery_route,
    a.vehicle_license_plate,
    a.partner_hub,
    a.nr_deliveries,
    a.unit_value,
    a.total_value
FROM {{ ref("stg_correos_express__orders_report") }} a
LEFT JOIN {{ ref('dim_users') }} b on b.user_vat = a.user_vat
ORDER BY date DESC