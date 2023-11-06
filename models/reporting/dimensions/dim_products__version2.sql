{{ config(alias='dim_products') }}

WITH productNameTranslation AS (
  SELECT res_id id, value product_name
  FROM {{ ref('stg_odoo__ir_translations') }}
  WHERE name = 'product.template,name' and lang = 'en_US'
), productCategoryTranslation AS (
  SELECT res_id id, value product_category
  FROM {{ ref('stg_odoo__ir_translations') }}
  WHERE name = 'product.category,name' and lang = 'en_US'
)

select
  p.id product_id,
  p.default_code product_code,
  it.product_name,
  it2.product_category,
  if(income_deduction_type = 'deduction', 'Deduction', if(income_deduction_type = 'income', 'Income', NULL)) product_group,
  if((pc.income_deduction_type = 'n_a' or pc.income_deduction_type IS NULL), FALSE, TRUE) user_transaction
from {{ ref('stg_odoo__product_products') }} p
left join {{ ref('stg_odoo__product_templates') }} pt on pt.id = p.product_tmpl_id
left join {{ ref('stg_odoo__product_categories') }} pc on pt.categ_id = pc.id
left join productNameTranslation it on pt.id = it.id 
left join productCategoryTranslation it2 on pc.id = it2.id 
where p.active is true
ORDER BY product_code ASC