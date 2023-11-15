SELECT
    b.user_id,
    b.contact_id,
    NULL partner_id,
    'Drivfit' partner_name,
    'Vehicle' partner_stream,
    'Service' partner_marketplace,
    a.date,
    a.year_week,
    a.year_month
FROM {{ ref('util_calendar') }} a
LEFT JOIN {{ ref('dim_vehicle_contracts') }} b
ON a.date BETWEEN b.start_date AND IFNULL(b.end_date, CURRENT_DATE())
WHERE 
    a.date <= CURRENT_DATE AND
    a.year >= 2020 AND
    b.user_id IS NOT NULL
ORDER BY date DESC