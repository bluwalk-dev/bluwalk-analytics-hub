select 
    a.opl_conditions_id as conditions_id,
    CAST(
        COALESCE(
            REGEXP_EXTRACT(description, r'Km Adicional\\s+(\\d{6})'),
            CAST(payment_cycle AS STRING)
        ) AS INT64
    ) AS year_week,
    payment_cycle,
    SUM(amount) AS debited_amount
from {{ ref('fct_fleet_billable_items') }} a
left join {{ ref('fct_fleet_rental_contracts') }} b on a.contract_id = b.vehicle_contract_id
where 
  vehicle_contract_length = 'mid-term' and 
  status = 'invoiced' and
  opl_conditions_id is not null and
  product_id = 294
group by conditions_id, year_week, payment_cycle