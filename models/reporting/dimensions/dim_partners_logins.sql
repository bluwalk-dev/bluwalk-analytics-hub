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
    'a6bedefed0135d2ca14b8a1eb7512a50' as partner_key,
    1 as sales_partner_id,
    a.org_name,
    a.odoo_location_id as location_id,
    b.location_name,
    a.org_id as login_id,
    6 as sales_tax_rate
FROM `bluwalk-analytics-hub.staging.stg_uber_organizations` a
LEFT JOIN {{ ref('dim_locations') }} b ON a.odoo_location_id = b.location_id