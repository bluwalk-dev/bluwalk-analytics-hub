with

source as (
    select
        *
    from {{ source('hubspot_drivfit', 'deal') }}
),

transformation as (

    select
        
        CAST (deal_id AS INT64) AS deal_id,
        
        CAST (property_dealname AS STRING) AS deal_name,
        CAST (deal_pipeline_id AS STRING) AS deal_pipeline_id,
        CAST (deal_pipeline_stage_id AS STRING) AS deal_pipeline_stage_id,
        CAST (is_deleted AS BOOL) AS is_deleted,
        CAST (property_hs_is_closed_won AS BOOL) AS is_closed_won,
        CAST (property_hs_is_closed AS BOOL) AS is_closed,
        CAST (owner_id AS INT64) AS owner_id,
        CAST (property_amount AS FLOAT64) AS deal_value,
        CAST (property_deal_bonus_value AS FLOAT64) AS deal_team_bonus,

        CAST (property_closedate AS TIMESTAMP) AS close_date,
        CAST (property_createdate AS TIMESTAMP) AS create_date

    from source
    where _fivetran_deleted is false

)

select * from transformation