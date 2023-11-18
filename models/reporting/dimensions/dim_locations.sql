/*
  location_enriched model
  This model enriches the city location data from the stg_odoo__res_country_cities source
  by joining it with the dim_countries dimension table. It provides a comprehensive view of city locations,
  including their names, associated country information, and active status.

  Source Tables:
  - stg_odoo__res_country_cities: Contains basic city location information.
  - dim_countries: Contains detailed country information.
*/

SELECT
    CAST(rcc.id AS INT64) AS location_id,  -- Unique identifier for the city location, cast to INT64 for consistency
    CAST(rcc.name AS STRING) AS location_name,  -- Name of the city location
    CAST(c.country_name AS STRING) AS location_country,  -- Name of the country associated with the city location
    CAST(c.country_code AS STRING) AS location_country_code,  -- Country code associated with the city location
    CAST(rcc.is_published AS BOOL) AS location_active  -- Flag indicating if the city location is published (active)

FROM {{ ref('stg_odoo__res_country_cities') }} rcc  -- Source table: staged country cities data
LEFT JOIN {{ ref('dim_countries') }} c ON rcc.country_id = c.country_id  -- Joining with countries dimension table for country details
ORDER BY location_id ASC  -- Ordering by location ID in ascending order
