with

source as (
    SELECT
        * 
    FROM {{ source('instagram_business', 'user_insights') }}
),

transformation as (

    select
        
        CAST(date AS DATE) date,
        CAST(follower_count as INTEGER) follower_count,
        CAST(profile_views as INTEGER) profile_views,
        CAST(impressions as INTEGER) impressions,
        CAST(impressions_7_d as INTEGER) impressions_7_d,
        CAST(impressions_28_d as INTEGER) impressions_28_d,
        CAST(reach as INTEGER) reach,
        CAST(reach_7_d as INTEGER) reach_7_d,
        CAST(reach_28_d as INTEGER) reach_28_d

    from source
    WHERE id = 17841458407920737

)

select * from transformation
