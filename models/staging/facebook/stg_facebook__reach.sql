with

source as (
    SELECT
    *
    FROM {{ source('facebook_pages', 'unique_daily_page_metrics_total') }}
),

transformation as (

    select
        
        CAST(date AS DATE) date,
        CAST(page_consumptions_unique as INTEGER) page_consumptions_reach,
        CAST(page_impressions_unique as INTEGER) page_reach,
        CAST(page_impressions_organic_unique_v_2 as INTEGER) page_reach_organic,
        CAST(page_impressions_paid_unique as INTEGER) page_reach_paid,
        CAST(page_posts_impressions_unique as INTEGER) post_reach,
        CAST(page_posts_impressions_organic_unique as INTEGER) post_reach_organic,
        CAST(page_posts_impressions_paid_unique as INTEGER) post_reach_paid

    from source
    WHERE page_id = '454268861443176'
)

select * from transformation
