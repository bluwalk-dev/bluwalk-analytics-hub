with

source as (
    select
        *
    from {{ source('uber_v2', 'src_uber_organizations') }}
),

transformation as (

    SELECT *
    FROM source

)

select * from transformation