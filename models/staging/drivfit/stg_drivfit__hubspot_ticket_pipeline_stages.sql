with

source as (
    select
        *
    from {{ source('drivfit_hubspot', 'ticket_pipeline_stage') }}
),

transformation as (

    select
        
        * EXCEPT(_fivetran_deleted, _fivetran_synced)

    from source

)

select * from transformation