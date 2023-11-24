SELECT
    c.date,
    b.partner_category,
    COUNT(*) new_accounts
FROM {{ ref("dim_partners_accounts") }} a
LEFT JOIN {{ ref("dim_partners") }} b ON a.sales_partner_id = b.sales_partner_id
LEFT JOIN {{ ref("util_calendar") }} c ON CAST(a.create_date AS DATE) = c.date
WHERE a.create_date <= current_date
GROUP BY c.date, b.partner_category