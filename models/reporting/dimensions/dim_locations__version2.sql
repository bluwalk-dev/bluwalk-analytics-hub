{{ config(alias='dim_locations') }}

SELECT
    CAST(rcc.id as INT64) AS location_id,
    CAST(rcc.name as STRING) AS name,
    CAST(c.country_name AS STRING) AS country,
    CAST(c.country_code AS STRING) AS country_code,
    CAST(rcc.is_published AS BOOL) AS active

FROM {{ ref('stg_odoo__res_country_cities') }} rcc
LEFT JOIN {{ ref('dim_countries') }} c ON rcc.country_id = c.country_id
ORDER BY location_id ASC