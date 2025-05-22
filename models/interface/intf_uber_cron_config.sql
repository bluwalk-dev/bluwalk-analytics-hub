SELECT
    login_id,
    org_name
FROM {{ ref('stg_uber__cron_config') }}