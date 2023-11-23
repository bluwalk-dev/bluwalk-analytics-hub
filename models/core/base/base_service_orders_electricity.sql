SELECT
    a.transaction_key,
    c.partner_key,
    c.partner_name,
    a.energy_id,
    b.user_id,
    a.contact_id,
    a.card_name,
    a.start_date,
    a.end_date,
    a.energy_source,
    a.station_name,
    a.station_type,
    a.product,
    a.quantity,
    a.measurement_unit,
    a.cost,
    a.margin,
    a.statement

FROM {{ ref('stg_odoo__fuel') }} a
LEFT JOIN {{ ref('dim_users') }} b ON a.contact_id = b.contact_id
LEFT JOIN {{ ref('dim_partners') }} c ON a.supplier_contact_id = c.partner_contact_id
WHERE 
    a.energy_source = 'electricity' AND
    c.partner_category = 'Energy'

