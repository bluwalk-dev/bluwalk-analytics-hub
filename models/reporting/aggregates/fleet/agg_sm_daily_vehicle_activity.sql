SELECT
    a.date,
    a.year_week,
    a.year_month,
    b.user_id,
    b.contact_id,
    b.vehicle_contract_type,
    b.vehicle_fuel_type,
    c.partner_key,
    c.partner_marketplace,
    c.partner_category,
    c.partner_name
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('dim_vehicle_contracts') }} b ON 
    a.date BETWEEN b.start_date AND IFNULL(b.end_date, CURRENT_DATE())
LEFT JOIN {{ ref('dim_partners') }} c ON b.service_partner_id = c.service_partner_id
WHERE 
    a.date < CURRENT_DATE AND
    a.year >= 2020 AND
    b.user_id IS NOT NULL
ORDER BY a.date DESC, user_id DESC