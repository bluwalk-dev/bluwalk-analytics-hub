WITH last_loaded_report AS (
    SELECT bolt_company_id as org_id, 'EARNINGS_REPORT' as report_type, max(date) last_loaded
    FROM {{ ref('stg_bolt__driver_earnings') }} GROUP BY org_id, report_type
    
    UNION ALL

    SELECT bolt_company_id as org_id, 'DRIVERS_REPORT' as report_type, max(date) last_loaded
    FROM {{ ref('stg_bolt__driver_engagement') }} GROUP BY org_id, report_type
)

SELECT
    x.org_id,
    x.org_name,
    x.report_type,
    y.last_loaded
FROM (    
    SELECT
        org_id,
        org_name,
        b.name as report_type
    FROM {{ ref('stg_bolt__organizations') }} a
    CROSS JOIN {{ ref('stg_bolt__report_types') }} b
    WHERE a.is_active = true
) x
LEFT JOIN last_loaded_report y ON 
    x.org_id = y.org_id AND
    x.report_type = y.report_type