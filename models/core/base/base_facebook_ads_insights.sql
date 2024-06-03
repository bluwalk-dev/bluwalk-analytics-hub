select
    CAST(a.date_start AS DATE) date,
    e.year_week,
    e.year_month,
    a.ad_id,
    b.name as ad_name,
    b.adset_id,
    c.name as adset_name,
    c.campaign_id,
    d.name campaign_name,
    a.clicks,
    a.frequency,
    a.impressions,
    a.link_clicks,
    a.reach,
    a.social_spend,
    a.spend,
    a.unique_clicks,
    a.unique_impressions
FROM {{ ref("stg_facebook__insights") }} a
LEFT JOIN {{ ref("stg_facebook__ads") }} b on a.ad_id = b.id
LEFT JOIN {{ ref("stg_facebook__ad_sets") }} c on b.adset_id = c.id
LEFT JOIN {{ ref("stg_facebook__campaigns") }} d on c.campaign_id = d.id
LEFT JOIN {{ ref("util_calendar") }} e ON CAST(a.date_start AS DATE) = e.date
ORDER BY date_start DESC