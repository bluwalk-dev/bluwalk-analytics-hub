SELECT DISTINCT
    a.partner_key,
    a.sales_partner_id,
    a.org_name,
    a.location_id,
    b.location_name,
    a.login_id,
    a.sales_tax_rate
FROM {{ ref('stg_bolt__cron_config') }} a
LEFT JOIN {{ ref('dim_locations') }} b ON a.location_id = b.location_id

UNION ALL

SELECT DISTINCT
    a.partner_key,
    a.sales_partner_id,
    a.org_alt_name org_name,
    a.location_id,
    b.location_name,
    a.login_id,
    a.sales_tax_rate
FROM {{ ref('stg_uber__cron_config') }} a
LEFT JOIN {{ ref('dim_locations') }} b ON a.location_id = b.location_id