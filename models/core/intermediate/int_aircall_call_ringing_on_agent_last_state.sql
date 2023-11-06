WITH call_ringing_on_agent_log AS (
    SELECT
        id,
        call_uuid,
        loaded_at
    FROM {{ ref('stg_aircall__call_ringing_on_agent') }}
    UNION ALL
    SELECT
        id,
        call_uuid,
        loaded_at
    FROM {{ ref('stg_aircallV2__call_ringing_on_agent') }}
)

SELECT * EXCEPT (__row_number) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY call_uuid ORDER BY loaded_at DESC) AS __row_number 
    FROM call_ringing_on_agent_log)
WHERE __row_number = 1