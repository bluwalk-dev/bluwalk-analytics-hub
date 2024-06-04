{{ config(materialized='table') }}

WITH form_submit AS (
    select
        PARSE_DATE('%Y%m%d', event_date) event_date,
        count(*) nr_form_submit
    from {{ ref('fct_marketing_form_submits') }}
    where page_location like '%viatura-propria%'
    group by event_date
), 

google_ads_spend AS (
    select
        date,
        sum(cost) google_spend
    from {{ ref('rpt_advertising_ad_group_report') }}
    where 
        media_platform = 'Google Ads' AND
        pipeline_id = '155110085'
    group by date
),

facebook_ads_spend AS (
    select
        date,
        sum(cost) facebook_spend
    from {{ ref('rpt_advertising_ad_group_report') }}
    where 
        media_platform = 'Facebook Ads' AND
        pipeline_id = '155110085'
    group by date
),

page_views AS (
    select
        PARSE_DATE('%Y%m%d', event_date) event_date,
        count(*) nr_page_views
    from {{ ref('fct_marketing_page_views') }}
    where page_location like '%viatura-propria%'
    group by event_date
),

deal_created AS (
    select
        create_date,
        count(*) deal_created
    from {{ ref('fct_deals') }}
    where deal_pipeline_id = '155110085'
    group by create_date
),

deal_won AS (
    select
        close_date,
        count(*) deal_won
    from {{ ref('fct_deals') }}
    where 
        deal_pipeline_id = '155110085' AND
        close_date IS NOT NULL AND
        is_closed_won = true
    group by close_date
)

SELECT
    a.event_date,
    a.nr_page_views,
    b.nr_form_submit,
    (c.facebook_spend + d.google_spend) total_spend,
    IFNULL(c.facebook_spend, 0) facebook_spend,
    IFNULL(d.google_spend, 0) google_spend,
    IFNULL(e.deal_created, 0) deal_created,
    IFNULL(f.deal_won, 0) deal_won
FROM page_views a
LEFT JOIN form_submit b ON a.event_date = b.event_date
LEFT JOIN facebook_ads_spend c ON a.event_date = c.date
LEFT JOIN google_ads_spend d ON a.event_date = d.date
LEFT JOIN deal_created e ON a.event_date = e.create_date
LEFT JOIN deal_won f ON a.event_date = f.close_date
ORDER BY event_date DESC
