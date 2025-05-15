{{ 
  config(
    materialized='table',
    tags=['medium_freshness']
  ) 
}}

WITH 
call_created AS (
  SELECT * EXCEPT (__row_number) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) AS __row_number FROM {{ ref('stg_aircallV2__call_created') }}
    )
  WHERE __row_number = 1
),
call_ended AS (
  SELECT * EXCEPT (__row_number) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) AS __row_number FROM {{ ref('stg_aircallV2__call_ended') }}
    )
  WHERE __row_number = 1
),
call_answered AS (
  SELECT * EXCEPT (__row_number) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) AS __row_number FROM {{ ref('stg_aircallV2__call_answered') }}
    )
  WHERE __row_number = 1
)

select
  a._id,
  a.number_id,
  TIMESTAMP_SECONDS(a.started_at) as started_ts,
  DATETIME(TIMESTAMP_SECONDS(a.started_at), 'Europe/Lisbon') as started_local,
  TIMESTAMP_SECONDS(c.answered_at) as answered_ts,
  DATETIME(TIMESTAMP_SECONDS(c.answered_at), 'Europe/Lisbon') as answered_local,
  TIMESTAMP_SECONDS(b.ended_at) as ended_ts,
  DATETIME(TIMESTAMP_SECONDS(b.ended_at), 'Europe/Lisbon') as ended_local,
  a.direction,
  a.number_name,
  b.missed_call_reason,
  CASE
      WHEN b.missed_call_reason IN ('agents_did_not_answer', 'no_available_agent') THEN true
      WHEN b.missed_call_reason IS NULL THEN true
      ELSE false
  END valid_call,
  a.raw_digits number_from,
  b.recording,
  b.duration as duration_sec,
  '' assigned_to,
  NULL user_id,
  '' transferred_by,
  '' transferred_to,
  a.archived,
  b.asset
from call_created a
left join call_ended b on a._id = b._id
left join call_answered c on a._id = c._id
where a.started_at < 1724974628