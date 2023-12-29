SELECT DISTINCT 
    a.user_id
FROM {{ ref('fct_deals') }} a
LEFT JOIN {{ ref('fct_user_rideshare_trips') }} b ON a.user_id = b.user_id
WHERE 
    (CAST(b.request_timestamp AS DATE) BETWEEN DATE_ADD(a.create_date, INTERVAL 10 DAY) and DATE_ADD(a.create_date, INTERVAL 20 DAY) ) AND
    a.deal_pipeline_id = '243997154' AND 
    a.deal_pipeline_stage_id = '409678799'