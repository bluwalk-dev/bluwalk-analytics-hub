WITH call_ended_log AS (
    SELECT
        id,
        call_uuid,
        ended_at,
        duration,
        number_name,
        asset,
        loaded_at
    FROM {{ ref('stg_aircall__call_ended') }}
    UNION ALL
    SELECT
        id,
        call_uuid,
        ended_at,
        duration,
        number_name,
        asset,
        loaded_at
    FROM {{ ref('stg_aircall__call_ended') }}
)

SELECT * EXCEPT (__row_number) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY call_uuid ORDER BY loaded_at DESC) AS __row_number 
    FROM call_ended_log)
WHERE __row_number = 1