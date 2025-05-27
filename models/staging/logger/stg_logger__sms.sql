with

source as (
    SELECT
        * 
    FROM {{ source('generic', 'log_sms') }}
),

transformation as (

    select
        
        *

    from source
)

select * from transformation
