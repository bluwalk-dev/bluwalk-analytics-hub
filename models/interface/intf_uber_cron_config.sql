SELECT
    login_id,
    org_name,
    report_type,
    last_loaded
FROM {{ ref('stg_uber__cron_config') }}