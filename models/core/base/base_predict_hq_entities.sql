WITH last_version_entities AS (
    SELECT * EXCEPT(__row_number) FROM
        (SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY event_id, venue_id
                ORDER BY load_timestamp DESC
            ) AS __row_number
        FROM {{ ref("stg_predict_hq__entities") }} 
        )
    WHERE __row_number = 1
)

SELECT
  *
FROM last_version_entities