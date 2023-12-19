SELECT * EXCEPT (__row_number) FROM
    (SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY driver_id
            ORDER BY load_timestamp DESC
        ) AS __row_number
    FROM {{ ref("stg_circuit__drivers") }} 
    )
WHERE __row_number = 1