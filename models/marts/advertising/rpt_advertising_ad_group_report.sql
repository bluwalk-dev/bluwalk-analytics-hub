WITH google_ads AS (
    SELECT 
        'Google Ads' media_platform,
        date,
        year_week,
        year_month
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
        year_month
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

SELECT * FROM google_ads
UNION ALL
SELECT * FROM facebook_ads
ORDER BY date DESC