{{ config(materialized='table') }}

WITH form_submit AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        COUNT(*) AS nr_form_submit,
        SUM(CASE WHEN source LIKE '%facebook%' OR source IN ('fb', 'ig') THEN 1 ELSE 0 END) AS facebook_form_submit,
        SUM(CASE WHEN source = 'google' THEN 1 ELSE 0 END) AS google_form_submit
    FROM {{ ref('fct_marketing_form_submits') }}
    WHERE page_location LIKE '%viatura-propria%'
    GROUP BY event_date
), 

ads_spend AS (
    SELECT
        date,
        SUM(CASE WHEN media_platform = 'Google Ads' THEN cost ELSE 0 END) AS google_spend,
        SUM(CASE WHEN media_platform = 'Facebook Ads' THEN cost ELSE 0 END) AS facebook_spend
    FROM {{ ref('rpt_advertising_ad_group_report') }}
    WHERE pipeline_id = '155110085'
    GROUP BY date
),

page_views AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        COUNT(*) AS nr_page_views
    FROM {{ ref('fct_marketing_page_views') }}
    WHERE page_location LIKE '%viatura-propria%'
    GROUP BY event_date
),

deals AS (
    SELECT
        create_date,
        COUNT(*) AS deal_created,
        SUM(CASE WHEN original_source = 'PAID_SOCIAL' THEN 1 ELSE 0 END) AS facebook_deal_created,
        SUM(CASE WHEN original_source = 'PAID_SEARCH' THEN 1 ELSE 0 END) AS google_deal_created,
        SUM(CASE WHEN is_closed_won THEN 1 ELSE 0 END) AS deal_won
    FROM {{ ref('fct_deals') }}
    WHERE 
        deal_pipeline_id = '155110085' AND
        source_url LIKE '%viatura-propria%' 
    GROUP BY create_date
)

SELECT
    pv.event_date,
    pv.nr_page_views,
    COALESCE(fs.facebook_form_submit, 0) AS facebook_form_submit,
    COALESCE(fs.google_form_submit, 0) AS google_form_submit,
    COALESCE(fs.nr_form_submit, 0) AS nr_form_submit,
    COALESCE(ads.google_spend, 0) + COALESCE(ads.facebook_spend, 0) AS total_spend,
    COALESCE(ads.facebook_spend, 0) AS facebook_spend,
    COALESCE(ads.google_spend, 0) AS google_spend,
    COALESCE(d.deal_created, 0) AS deal_created,
    COALESCE(d.facebook_deal_created, 0) AS deal_created_facebook,
    COALESCE(d.google_deal_created, 0) AS deal_created_google,
    COALESCE(d.deal_won, 0) AS deal_won
FROM page_views pv
LEFT JOIN form_submit fs ON pv.event_date = fs.event_date
LEFT JOIN ads_spend ads ON pv.event_date = ads.date
LEFT JOIN deals d ON pv.event_date = d.create_date
ORDER BY pv.event_date DESC
