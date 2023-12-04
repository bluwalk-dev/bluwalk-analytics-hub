SELECT DISTINCT
    partner_key,
    sales_partner_id,
    location_id,
    login_id,
    sales_tax_rate
FROM {{ ref('stg_bolt__cron_config') }}