{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

with

source as (
    select
        *
    from {{ source('google_sheets', 'ad_group_classification') }}
),

transformation as (

    select
        
        CAST(media_platform AS STRING) media_platform,
        CAST(campaign_id AS STRING) campaign_id,
        CAST(campaign_name AS STRING) campaign_name,
        CAST(ad_group_id AS STRING) ad_group_id,
        CAST(ad_group_name AS STRING) ad_group_name,
        CAST(stream AS STRING) stream,
        CAST(pipeline AS STRING) pipeline_id

    FROM source
    WHERE ad_group_id IS NOT NULL

)

select * from transformation