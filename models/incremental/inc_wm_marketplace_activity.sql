SELECT
  dl.user_id,
  tl.training_date,
  tl.training_time,
  tl.training_outcome,
  tl.training_location
FROM {{ ref("base_hubspot_deals") }} dl
LEFT JOIN {{ ref("stg_mercadao__training_schedule") }} tl ON dl.contact_id = tl.contact_id
WHERE
    tl.contact_id IS NOT NULL AND
    tl.training_date >= CAST(dl.create_date AS DATE) AND
    dl.is_closed = FALSE AND
    dl.deal_pipeline_id = '177891797' AND
    dl.deal_pipeline_stage_id != '324018671'