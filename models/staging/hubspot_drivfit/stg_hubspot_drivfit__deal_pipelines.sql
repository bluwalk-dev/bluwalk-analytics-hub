with

source as (
    select
        *
    from {{ source('hubspot_drivfit', 'deal_pipeline') }}
),

transformation as (

    select
        
        CAST (pipeline_id AS STRING) AS pipeline_id,
        CAST (label AS STRING) AS label,
        CAST (created_at AS TIMESTAMP) AS created_at,
        CAST (updated_at AS TIMESTAMP) AS updated_at

    from source
    where _fivetran_deleted is false

)

select * from transformation