SELECT DISTINCT
    b.user_id,
    b.contact_id,
    '' partner_key,
    '' partner_name,
    'Vehicle' partner_stream,
    'Service' partner_marketplace,
    c.date,
    c.year_week,
    c.year_month
FROM {{ ref('fct_user_rideshare_trips') }} a
LEFT JOIN {{ ref('dim_vehicle_contracts') }} b ON a.vehicle_contract_id = b.vehicle_contract_id
LEFT JOIN {{ ref('util_calendar') }} c ON CAST(a.request_timestamp AS DATE) = c.date
WHERE 
    a.vehicle_contract_type = 'free_loan' AND
    c.year >= 2020
ORDER BY c.date DESC