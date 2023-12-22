with

source as (
    SELECT
        CAST(date AS DATE) date,
        CAST(page_content_activity as INTEGER) page_content_activity,
        CAST(page_actions_post_reactions_anger_total AS INTEGER) page_actions_post_reactions_anger_total,
        CAST(page_actions_post_reactions_haha_total AS INTEGER) page_actions_post_reactions_haha_total,
        CAST(page_actions_post_reactions_like_total AS INTEGER) page_actions_post_reactions_like_total,
        CAST(page_actions_post_reactions_love_total AS INTEGER) page_actions_post_reactions_love_total,
        CAST(page_actions_post_reactions_sorry_total AS INTEGER) page_actions_post_reactions_sorry_total,
        CAST(page_actions_post_reactions_wow_total AS INTEGER) page_actions_post_reactions_wow_total,
        CAST(page_impressions_organic_v_2 AS INTEGER) page_impressions_organic,
        CAST(page_impressions_paid AS INTEGER) page_impressions_paid,
        CAST(page_impressions AS INTEGER) page_impressions,
        CAST(page_consumptions AS INTEGER) page_consumptions,
        CAST(page_engaged_users as INTEGER) page_engaged_users,
        CAST(page_fans as INTEGER) page_fans,
    FROM {{ source('facebook_pages', 'daily_page_metrics_total') }}
    WHERE page_id = '454268861443176'
),

transformation as (

    select
        
        *

    from source

)

select * from transformation
