SELECT 
    * EXCEPT(__row_number)
FROM
    (SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY document_id
            ORDER BY load_timestamp DESC
        ) AS __row_number
    FROM {{ ref("stg_mds__documents") }} 
    )
WHERE __row_number = 1