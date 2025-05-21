SELECT 
  b.year_month, 
  a.po_line_name, 
  a.price_net, 
  a.supplier_name,
  c.product_code
FROM {{ ref('fct_fleet_purchase_order_lines') }} a
JOIN {{ ref('util_calendar') }} b  ON a.date = b.date
JOIN {{ ref('int_odoo_drivfit_products') }} c  ON a.product_id = c.product_id
WHERE a.product_id NOT IN (279, -- Custo recondicionamento via reparação (delivery)
                         293, -- Agenciamento - Inspeção viaturas
                         303, -- Parqueamento Agente
                         289, -- IPO
                         308 -- Pneus
                         ) 
order by b.year_month desc