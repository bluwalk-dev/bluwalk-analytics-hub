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

select * from bluwalk-analytics-hub.staging.stg_uber_organizations