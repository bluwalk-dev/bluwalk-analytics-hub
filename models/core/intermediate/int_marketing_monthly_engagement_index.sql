WITH 

retention_rate AS (
    select 
        year_month,
        retention_rate AS metric,
        0.7638 AS baseline,
        0.3333 AS weight
    from {{ ref('dm_core_monthly_marketplace_users') }}
    where partner_marketplace = 'Work'
),
social_media_engagement AS (
    select 
        year_month,
        monthly_engagement_rate AS metric,
        0.1258 AS baseline,
        0.6666 AS weight
    from {{ ref('agg_marketing_monthly_social_media_impressions') }}
)

SELECT 
    a.year_month,
    a.metric rr_metric,
    ROUND(a.metric / a.baseline * 100, 2) rr_score,
    b.metric sme_metric,
    ROUND(b.metric / b.baseline * 100, 2) sme_score,
    ROUND(
        a.metric / a.baseline * 100 * a.weight +
        b.metric / b.baseline * 100 * b.weight
    , 0) engagement_index
FROM retention_rate a
LEFT JOIN social_media_engagement b ON a.year_month = b.year_month
ORDER BY a.year_month DESC