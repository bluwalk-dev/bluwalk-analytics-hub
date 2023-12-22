WITH organic_traffic_share AS (
    SELECT
        date,
        organic_reach,
        total_reach
    FROM {{ ref('agg_marketing_daily_source_social_media_reach') }}
    WHERE organic_reach IS NOT NULL
)

SELECT
    b.year_month,
    SUM(c.organic_reach) organic_ratio__organic_reach,
    SUM(c.total_reach) organic_ratio__total_reach,
    SUM(a.total_reach) total_reach
FROM {{ ref('agg_marketing_daily_source_social_media_reach') }} a
LEFT JOIN organic_traffic_share c ON a.date = c.date
LEFT JOIN {{ ref('util_calendar') }} b on a.date = b.date
GROUP BY year_month
ORDER BY year_month DESC