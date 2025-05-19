SELECT
    b.card_name,
    a.user_id,
    b.partner_key,
    REPLACE(REPLACE(a.deal_pipeline_name, 'Fuel & Energy : ', ''), ' Card', '') card_type,
    MIN(b.start_date) activation_timestamp
FROM {{ ref('fct_deals') }} a
LEFT JOIN {{ ref('fct_service_orders_energy') }} b
ON
  a.user_id = b.user_id AND
  a.energy_card_name = b.card_name
WHERE
  a.deal_pipeline_id IN ('180181752', '180111309') AND -- Fuel and Energy pipelines
  a.is_closed = FALSE AND
  a.create_datetime < CAST(b.start_date AS TIMESTAMP)
GROUP BY
  b.card_name,
  b.partner_key,
  a.user_id,
  card_type