SELECT * EXCEPT (__row_number) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) AS __row_number 
    FROM {{ ref('stg_zendesk__tickets') }})
WHERE __row_number = 1