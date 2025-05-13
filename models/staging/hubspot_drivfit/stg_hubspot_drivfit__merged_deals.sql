with

source as (
    select
        *
    from {{ source('hubspot_drivfit', 'merged_deal') }}
),

transformation as (

    select
        
        CAST (deal_id AS INT64) AS deal_id,
        CAST (merged_deal_id AS INT64) AS merged_deal_id,

    from source

)

select * from transformation