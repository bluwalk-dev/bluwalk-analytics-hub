SELECT
    b.year_week,
    a.contact_id,
    a.date,
    a.user_vat,
    a.user_name,
    a.partner_account_uuid,
    a.user_delivery_route,
    a.vehicle_license_plate,
    a.partner_hub,
    a.nr_deliveries,
    a.unit_value,
    a.total_value
FROM {{ ref("base_correos_express_orders") }} a
LEFT JOIN {{ ref("util_calendar") }} b ON a.date = b.date
ORDER BY date DESC, contact_id DESC