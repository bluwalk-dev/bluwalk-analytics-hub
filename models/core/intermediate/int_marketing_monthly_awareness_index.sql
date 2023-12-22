WITH 

web_traffic AS (
    SELECT 
        year_month,
        unique_visitors AS metric,
        80252 AS baseline,
        0.4000 AS weight
    FROM {{ ref('agg_marketing_monthly_website_traffic') }}
),

organic_ratio AS (
    SELECT
        year_month,
        ROUND(SUM(organic_reach) / SUM(total_reach), 4) metric,
        0.05 AS baseline,
        0.3333 AS weight
    FROM (
        SELECT
            year_month,
            organic_ratio__organic_reach organic_reach,
            organic_ratio__total_reach total_reach
        FROM {{ ref('agg_marketing_monthly_social_media_reach') }}
        UNION ALL
        SELECT
            year_month,
            unique_source_organic_visitors organic_reach,
            unique_source_total_visitors total_reach
        FROM {{ ref('agg_marketing_monthly_website_traffic') }}
    )
    GROUP BY year_month
),

social_media_reach AS (
    SELECT
        year_month,
        total_reach AS metric,
        625781 AS baseline,
        0.2666 AS weight
    FROM {{ ref('agg_marketing_monthly_social_media_reach') }}
)

SELECT 
    a.year_month,
    a.metric wt_metric,
    ROUND(a.metric / a.baseline * 100, 2) wt_score,
    b.metric or_metric,
    ROUND(b.metric / b.baseline * 100, 2) or_score,
    c.metric smr_metric,
    ROUND(c.metric / c.baseline * 100, 2) smr_score,
    ROUND(
        a.metric / a.baseline * 100 * a.weight +
        b.metric / b.baseline * 100 * b.weight +
        c.metric / c.baseline * 100 * c.weight
    , 0) awareness_index
FROM web_traffic a
LEFT JOIN organic_ratio b ON a.year_month = b.year_month
LEFT JOIN social_media_reach c ON a.year_month = c.year_month
ORDER BY a.year_month DESC