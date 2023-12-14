SELECT DISTINCT
    partner_key,
    sales_partner_id,
    org_name,
    location_id,
    login_id,
    sales_tax_rate
FROM {{ ref('stg_bolt__cron_config') }}

UNION ALL

SELECT DISTINCT
    partner_key,
    sales_partner_id,
    org_alt_name org_name,
    location_id,
    login_id,
    sales_tax_rate
FROM {{ ref('stg_uber__cron_config') }}