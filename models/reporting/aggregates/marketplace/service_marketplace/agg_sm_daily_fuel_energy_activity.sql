SELECT DISTINCT
    a.user_id,
    a.contact_id,
    '' partner_key,
    supplier_name partner_name,
    'Energy' partner_stream,
    'Service' partner_marketplace,
    b.date,
    b.year_week,
    b.year_month
FROM {{ ref('fct_service_orders_energy') }} a
LEFT JOIN {{ ref('util_calendar') }} b ON CAST(a.end_date AS DATE) = b.date
WHERE 
    b.year >= 2020
ORDER BY b.date DESC