SELECT
    id AS country_id,  -- Unique identifier for the country
    name AS country_name,  -- Name of the country
    code AS country_code  -- Code associated with the country
FROM {{ ref('stg_odoo_drivfit__res_countries') }}