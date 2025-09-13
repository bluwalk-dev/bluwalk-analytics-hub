WITH google_ads AS (
    SELECT 
        'Google Ads' media_platform,
        date,
        year_week,
        year_month,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        SUM(impressions) impressions,
        SUM(clicks) clicks,
        SUM(cost) cost
    FROM {{ ref("base_google_ads_insights") }}
    GROUP BY date, year_week, year_month, campaign_id, campaign_name, ad_group_id, ad_group_name
), 

facebook_ads AS (
    SELECT 
        'Facebook Ads' media_platform,
        date,
        year_week,
        year_month,
        campaign_id,
        campaign_name,
        adset_id ad_group_id,
        adset_name ad_group_name,
        SUM(impressions) impressions,
        SUM(clicks) clicks,
        SUM(spend) cost
    FROM {{ ref("base_facebook_ads_insights") }}
    GROUP BY date, year_week, year_month, campaign_id, campaign_name, ad_group_id, ad_group_name
)

SELECT 
    a.date,
    a.year_week,
    a.year_month,
    a.media_platform,
    b.stream,
    b.pipeline_id,
    c.label pipeline_name,
    a.campaign_id,
    a.campaign_name,
    a.ad_group_id,
    a.ad_group_name,
    a.impressions,
    a.clicks,
    a.cost
FROM (
    SELECT * FROM google_ads
    UNION ALL
    SELECT * FROM facebook_ads
) a
LEFT JOIN {{ ref("stg_google_sheets__ad_group_classification") }} b ON a.ad_group_id = b.ad_group_id
LEFT JOIN bluwalk-analytics-hub.staging.stg_hubspot_deal_pipelines c ON b.pipeline_id = c.pipeline_id
ORDER BY date DESC