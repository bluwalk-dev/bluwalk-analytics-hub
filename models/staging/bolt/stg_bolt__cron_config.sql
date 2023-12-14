SELECT
    'e8d8ba0b559bd193d89e320f44686eee' partner_key,
    2 sales_partner_id,
    CAST(org_id AS STRING) login_id,
    org_name,
    report_type,
    last_loaded,
    folder_id,
    sales_tax_rate,
    location_id
FROM {{ source('bolt', 'cron_config') }}