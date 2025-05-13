SELECT
    CAST(rcc.id AS INT64) AS location_id,
    CAST(rcc.name AS STRING) AS location_name,
    CAST(c.country_name AS STRING) AS location_country,
    CAST(c.country_code AS STRING) AS location_country_code,
    CAST(rcc.is_published AS BOOL) AS location_active,
    CASE 
        WHEN c.country_code = 'PT' THEN
            CASE WHEN rcc.name = 'Açores' THEN 'Atlantic/Azores'
            WHEN rcc.name = 'Madeira' THEN 'Atlantic/Madeira'
            ELSE 'Europe/Lisbon'
            END
        WHEN c.country_code = 'FR' THEN 'Europe/Paris'
        ELSE ''
    END location_timezone
FROM {{ ref('stg_odoo_drivfit__res_country_cities') }} rcc
LEFT JOIN {{ ref('dim_countries') }} c ON rcc.country_id = c.country_id
ORDER BY location_id ASC