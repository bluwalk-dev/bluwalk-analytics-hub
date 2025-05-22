SELECT
    login_id,
    report_type,
    last_loaded
FROM {{ ref('stg_uber__cron_config') }}