/*
  product_enriched model
  This model enriches the product data from the stg_odoo__product_products source
  by joining it with product templates, categories, and their respective English translations.
  It provides a comprehensive view of products, including their names, categories, and group classifications.

  Source Tables:
  - stg_odoo__product_products: Contains basic product information.
  - stg_odoo__product_templates: Contains product template details.
  - stg_odoo__product_categories: Contains product category information.
  - stg_odoo__ir_translations: Contains translations for various fields.
*/

WITH productNameTranslation AS (
  SELECT 
    res_id AS id, 
    value AS product_name
  FROM {{ ref('stg_odoo__ir_translations') }}
  WHERE name = 'product.template,name' AND lang = 'en_US'
), 
productCategoryTranslation AS (
  SELECT 
    res_id AS id, 
    value AS product_category
  FROM {{ ref('stg_odoo__ir_translations') }}
  WHERE name = 'product.category,name' AND lang = 'en_US'
)

SELECT
  p.id AS product_id,  -- Unique identifier for the product
  p.default_code AS product_code,  -- Default code of the product
  it.product_name,  -- English name of the product
  it2.product_category,  -- English category name of the product

  -- Product group classification based on income deduction type
  CASE 
    WHEN income_deduction_type = 'deduction' THEN 'Deduction'
    WHEN income_deduction_type = 'income' THEN 'Income'
    ELSE NULL 
  END AS product_group,

  -- Flag indicating if the product is used in user transactions
  (pc.income_deduction_type <> 'n_a' AND pc.income_deduction_type IS NOT NULL) AS user_transaction

FROM {{ ref('stg_odoo__product_products') }} p
LEFT JOIN {{ ref('stg_odoo__product_templates') }} pt ON pt.id = p.product_tmpl_id
LEFT JOIN {{ ref('stg_odoo__product_categories') }} pc ON pt.categ_id = pc.id
LEFT JOIN productNameTranslation it ON pt.id = it.id 
LEFT JOIN productCategoryTranslation it2 ON pc.id = it2.id 
WHERE p.active IS TRUE
ORDER BY product_code ASC
