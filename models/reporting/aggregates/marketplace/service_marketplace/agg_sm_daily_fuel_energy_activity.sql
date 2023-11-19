WITH fuel_and_energy AS (
    SELECT * FROM {{ ref('base_fuel') }}
    UNION ALL
    SELECT * FROM {{ ref('base_energy') }}
)

SELECT DISTINCT
    a.user_id,
    a.contact_id,
    '' partner_key,
    supplier_name partner_name,
    'Fuel and Energy' partner_stream,
    'Service' partner_marketplace,
    b.date,
    b.year_week,
    b.year_month
FROM fuel_and_energy a
LEFT JOIN {{ ref('util_calendar') }} b ON CAST(a.end_date AS DATE) = b.date
WHERE 
    b.year >= 2020
ORDER BY b.date DESC