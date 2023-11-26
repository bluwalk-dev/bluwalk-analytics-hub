with

source as (
    SELECT *
    FROM {{ source('odoo_static', 'hr_department') }}
),

transformation as (

    select
        *
    from source
    

)

SELECT * FROM transformation
