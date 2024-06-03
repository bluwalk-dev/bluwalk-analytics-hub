select
    CAST(a.date_start AS DATE) date,
    e.year_week,
    e.year_month,
    b.id,
    b.final_urls as ad_final_urls,
    b.type as ad_type,
    a.ad_group_id,
    c.name ad_group_name,
    c.campaign_id,
    d.name as campaign_name,
    a.clicks,
    a.conversion_value,
    a.conversions,
    a.cost/1000000 cost,
    a.impressions,
    a.interaction_types,
    a.interactions

FROM {{ ref("stg_google_ads__ad_performance") }} a
LEFT JOIN {{ ref("stg_google_ads__ads") }} b ON a.ad_id = b.id
LEFT JOIN {{ ref("stg_google_ads__ad_groups") }} c ON a.ad_group_id = c.id
LEFT JOIN {{ ref("stg_google_ads__campaigns") }} d ON c.campaign_id = d.id
LEFT JOIN {{ ref("util_calendar") }} e ON CAST(a.date_start AS DATE) = e.date
ORDER BY 
    a.date_start DESC,
    campaign_id DESC,
    ad_group_id DESC