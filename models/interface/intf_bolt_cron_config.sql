SELECT
    login_id,
    report_type,
    last_loaded
FROM {{ ref('stg_bolt__cron_config') }}