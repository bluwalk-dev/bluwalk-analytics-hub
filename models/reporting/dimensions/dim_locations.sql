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
        WHEN rcc.id = 4 THEN ST_GEOGFROMTEXT('POLYGON ((-8.8091015 41.6118185, -8.8049816 41.4730575, -8.777277 41.4709246, -8.5946293 41.3967946, -8.6069889 41.3401104, -7.9739017 41.3648514, -7.9398083 41.4751154, -7.8093456 41.5748447, -7.8477978 41.6138719, -7.9961132 41.6046308, -7.9837536 41.6785231, -8.0977367 41.6918557, -8.0551647 41.8188897, -8.165028 41.8158193, -8.4946178 41.7400353, -8.5440563 41.6323503, -8.8091015 41.6118185))')
        WHEN rcc.id = 5 THEN ST_GEOGFROMTEXT('POLYGON ((-8.7857555 40.5235362, -8.9780163 40.0731608, -8.3847545 39.9321982, -8.1732677 40.0857701, -8.1265758 39.9343042, -7.7420543 40.100478, -7.8244518 40.2536712, -7.8244518 40.5005666, -8.0634044 40.3646768, -8.4561657 40.2830111, -8.7857555 40.5235362))')
        WHEN rcc.id = 6 THEN ST_GEOGFROMTEXT('POLYGON ((-8.6523075 41.0103691, -8.7857555 40.5235362, -8.4561657 40.2830111, -8.2611583 40.5079389, -8.3270763 40.7372551, -8.1046032 40.8578501, -8.2556652 41.0652579, -8.6523075 41.0103691))')
        WHEN rcc.id = 7 THEN ST_GEOGFROMTEXT('POLYGON ((-17.3006685 32.8922984, -17.3034151 32.6093286, -16.6167696 32.6116422, -16.6222627 32.889992, -17.3006685 32.8922984))')
        WHEN rcc.id = 11 THEN ST_GEOGFROMTEXT('POLYGON ((-8.1265758 39.9343042, -8.1732677 40.0857701, -8.3847545 39.9321982, -8.9780163 40.0731608, -9.5122455 39.3620526, -9.3416996 39.2943453, -8.9928837 39.2985964, -8.8887713 39.4681484, -8.7377093 39.4766291, -8.6772845 39.5550265, -8.6333392 39.6946506, -8.6882709 39.7706905, -8.4795306 39.8403199, -8.4465716 39.7305688, -8.1265758 39.9343042))')        
    END AS location_polygon


FROM {{ ref('stg_odoo__res_country_cities') }} rcc  -- Source table: staged country cities data
LEFT JOIN {{ ref('dim_countries') }} c ON rcc.country_id = c.country_id  -- Joining with countries dimension table for country details
ORDER BY location_id ASC  -- Ordering by location ID in ascending order
