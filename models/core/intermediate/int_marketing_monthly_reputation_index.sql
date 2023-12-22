WITH 

nps AS (
    SELECT 
        year_month,
        nps_score AS metric,
        44.4 AS baseline,
        0.7143 AS weight
    FROM {{ ref('agg_customer_service_monthly_nps') }}
),

service_rating AS (
    select 
        year_month,
        average_rating AS metric,
        0.7575 AS baseline,
        0.2857 AS weight
    from {{ ref('agg_customer_service_monthly_average_rating') }}
)

SELECT 
    a.year_month,
    a.metric nps_metric,
    ROUND(a.metric / a.baseline * 100, 2) nps_score,
    b.metric sr_metric,
    ROUND(b.metric / b.baseline * 100, 2) sr_score,
    ROUND(
        a.metric / a.baseline * 100 * a.weight +
        b.metric / b.baseline * 100 * b.weight
    , 0) reputation_index
FROM nps a
LEFT JOIN service_rating b ON a.year_month = b.year_month
ORDER BY a.year_month DESC