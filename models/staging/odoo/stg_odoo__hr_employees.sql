with

source as (
    SELECT *
    FROM {{ source('odoo_static', 'hr_employee') }}
),

transformation as (

    select
        *
    from source
    

)

SELECT * FROM transformation