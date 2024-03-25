SELECT DISTINCT
    c.date,
    c.year_week,
    c.year_month,
    b.user_id,
    b.contact_id,
    b.vehicle_fuel_type,
    d.partner_key,
    d.partner_marketplace,
    d.partner_category,
    d.partner_name
FROM {{ ref('fct_user_rideshare_trips') }} a
LEFT JOIN {{ ref('dim_vehicle_contracts') }} b ON a.vehicle_contract_id = b.vehicle_contract_id
LEFT JOIN {{ ref('util_calendar') }} c ON CAST(a.request_timestamp AS DATE) = c.date
LEFT JOIN {{ ref('dim_partners') }} d ON b.service_partner_id = d.service_partner_id
WHERE 
    a.vehicle_contract_type = 'free_loan' AND
    c.year >= 2020
ORDER BY c.date DESC, user_id DESC