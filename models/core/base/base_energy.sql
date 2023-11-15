SELECT
    a.id energy_id,
    b.user_id,
    a.partner_id contact_id,
    a.card_name card_name,
    a.start_date,
    a.end_date,
    a.station_name,
    a.station_type,
    a.supplier_id supplier_contact_id,
    c.short_name supplier_name,
    a.product,
    ROUND(a.quantity, 2) quantity,
    a.measurement_unit,
    ROUND(a.cost, 2) cost,
    ROUND(a.margin, 2) margin,
    ROUND(a.cost + a.margin, 2) price,
    a.payment_cycle statement
FROM {{ ref('stg_odoo__fuel') }} a
LEFT JOIN {{ ref('dim_users') }} b ON a.partner_id = b.contact_id
LEFT JOIN {{ ref('dim_contacts') }} c ON a.supplier_id = c.contact_id
WHERE fuel_source = 'electricity'

