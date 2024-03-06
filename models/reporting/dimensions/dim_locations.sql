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
    CAST(rcc.is_published AS BOOL) AS location_active,  -- Flag indicating if the city location is published (active)
    CASE 
        WHEN rcc.id = 1 THEN ST_GEOGFROMTEXT('POLYGON ((-8.777277 41.4709246, -8.8363285 41.4276924, -8.6523075 41.0103691, -7.9052372 41.1170188, -7.8709049 41.2348564, -7.9739017 41.3648514, -8.6069889 41.3401104, -8.5946293 41.3967946, -8.777277 41.4709246))')
        WHEN rcc.id = 2 THEN ST_GEOGFROMTEXT('POLYGON ((-9.3416996 39.2943453, -9.5696659 38.8422723, -9.4776554 38.6473348, -9.1137333 38.6977267, -9.0176029 38.7587928, -8.7841434 39.055875, -8.9928837 39.2985964, -9.3416996 39.2943453))')
        WHEN rcc.id = 3 THEN ST_GEOGFROMTEXT('POLYGON ((-8.80213 37.4489564, -8.9161131 37.2841477, -9.0328428 36.974294, -8.1910155 37.0784499, -7.9960081 36.9775853, -7.8847716 36.9512508, -7.3999998 37.1671437, -7.5181029 37.5274131, -8.085272 37.3212885, -8.80213 37.4489564))')
    END AS location_polygon


FROM {{ ref('stg_odoo__res_country_cities') }} rcc  -- Source table: staged country cities data
LEFT JOIN {{ ref('dim_countries') }} c ON rcc.country_id = c.country_id  -- Joining with countries dimension table for country details
ORDER BY location_id ASC  -- Ordering by location ID in ascending order
