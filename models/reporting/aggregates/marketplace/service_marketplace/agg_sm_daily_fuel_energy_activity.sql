SELECT DISTINCT
    b.date,
    b.year_week,
    b.year_month,
    a.user_id,
    a.contact_id,
    a.partner_key,
    c.partner_marketplace,
    c.partner_category,
    a.partner_name
FROM {{ ref('fct_service_orders_energy') }} a
LEFT JOIN {{ ref('util_calendar') }} b ON CAST(a.end_date AS DATE) = b.date
LEFT JOIN {{ ref('dim_partners') }} c ON a.partner_key = c.partner_key
WHERE b.year >= 2020
ORDER BY date DESC, user_id DESC