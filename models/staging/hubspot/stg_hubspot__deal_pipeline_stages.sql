with

source as (
    select
        *
    from {{ source('hubspot', 'deal_pipeline_stage') }}
),

transformation as (

    select
        

        CAST (stage_id AS STRING) AS stage_id,
        CAST (active AS BOOL) AS active,
        CAST (closed_won AS BOOL) AS closed_won,
        CAST (created_at AS TIMESTAMP) AS created_at,
        CAST (display_order AS INT64) AS display_order,
        CAST (label AS STRING) AS label,
        CAST (pipeline_id AS STRING) AS pipeline_id,
        CAST (probability AS FLOAT64) AS probability,
        CAST (updated_at AS TIMESTAMP) AS updated_at

    from source
    where _fivetran_deleted is false

)

select * from transformation