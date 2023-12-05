SELECT * FROM
    (SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY partner_account_uuid, date 
            ORDER BY load_timestamp DESC
        ) AS __row_number
    FROM {{ ref("stg_bolt__earnings") }} 
    )
WHERE __row_number = 1