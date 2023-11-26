with

source as (
    SELECT *
    FROM {{ source('odoo_static', 'ir_translation') }}
),

transformation as (

    select
        *
    from source
    
)

SELECT * FROM transformation