SELECT
    b.year_month,
    SUM(total_activity) total_activity,
    ROUND(AVG(total_followers),0) total_followers,
    ROUND(SUM(total_activity)/AVG(total_followers), 4) monthly_engagement_rate
FROM {{ ref('agg_marketing_daily_source_social_media_impressions') }} a
LEFT JOIN {{ ref('util_calendar') }} b on a.date = b.date
GROUP BY year_month
ORDER BY year_month DESC