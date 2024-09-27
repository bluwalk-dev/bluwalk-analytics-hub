WITH sessions AS (
    SELECT * FROM
        (SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY id 
                ORDER BY load_timestamp DESC
            ) AS __row_number
        FROM {{ ref("stg_evio__charging_sessions") }} 
        )
    WHERE __row_number = 1
)

SELECT * 
FROM sessions