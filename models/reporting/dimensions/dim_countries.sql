/*
  country_enriched model
  This model extracts country data from the stg_odoo__res_countries source table.
  It provides basic country information, including country names and their corresponding codes.

  Source Table:
  - stg_odoo__res_countries: Contains country information.
*/

SELECT
    id AS country_id,  -- Unique identifier for the country
    name AS country_name,  -- Name of the country
    code AS country_code  -- Code associated with the country
FROM {{ ref('stg_odoo__res_countries') }}  -- Source table: staged countries data
