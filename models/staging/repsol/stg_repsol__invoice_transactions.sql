with

source as (
    select
        *
    from {{ source('repsol', 'invoice_transactions') }} 
),

transformation as (

    SELECT
        
        *

    FROM source

)

SELECT * FROM transformation