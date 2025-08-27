WITH last_loaded_report AS (
    SELECT 
        org_id, 
        report_type,
        CAST(max(end_local_time) AS DATE) last_loaded
    FROM {{ ref('stg_uber__report_requests') }}
    WHERE status = 'processed'
    GROUP BY org_id, report_type
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
    FROM {{ ref('stg_uber__organizations') }} a
    CROSS JOIN {{ ref('stg_uber__report_types') }} b
) x
LEFT JOIN last_loaded_report y ON 
    x.org_id = y.org_id AND
    x.report_type = y.report_type