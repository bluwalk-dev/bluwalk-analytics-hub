SELECT
    id country_id,
    name country_name,
    code country_code
FROM {{ ref('stg_odoo__res_countries') }}