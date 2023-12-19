SELECT * EXCEPT (__row_number, load_timestamp) FROM
    (SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY plan_id
            ORDER BY load_timestamp DESC
        ) AS __row_number
    FROM {{ ref("stg_circuit__plans") }} 
    )
WHERE __row_number = 1