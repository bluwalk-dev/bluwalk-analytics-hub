SELECT
  p.id as product_id, 
  p.default_code as product_code,
  pt.name as product_description,
  pc.name as product_category,
  pc.complete_name as product_category_tree
FROM {{ ref('stg_odoo_drivfit__product_products') }} p
LEFT JOIN {{ ref('stg_odoo_drivfit__product_templates') }} pt ON pt.id = p.product_tmpl_id
LEFT JOIN {{ ref('stg_odoo_drivfit__product_categories') }} pc ON pt.categ_id = pc.id
WHERE p.active = true