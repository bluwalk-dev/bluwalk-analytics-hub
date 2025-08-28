with

source as (
    select
        *
    from {{ source('bolt_v2', 'src_bolt_report_types') }}
),

transformation as (

    SELECT
        *
    FROM source

)

select * from transformation


    
