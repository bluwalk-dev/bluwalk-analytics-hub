SELECT 
  b.year_month, 
  c.product_code, 
  SUM(a.amount) as revenue
FROM {{ ref('fct_fleet_billable_items') }} a
JOIN {{ ref('util_calendar') }} b ON a.period_end = b.date
JOIN {{ ref('int_odoo_drivfit_products') }} c  ON a.product_id = c.product_id
WHERE a.product_id IN (281, -- Danos na Viatura
                     355, -- Franquia (ajustes)
                     285, -- Franquia de Sinistro
                     354, -- Pack Proteção
                     304, -- Penalização na Limpeza Exterior
                     305, -- Penalização na Limpeza Interior
                     306, -- Penalização por Diferença de Combustível
                     291, -- Identificação de Multas
                     321) -- Transporte viaturas (reboque)
group by year_month, c.product_code
order by year_month desc