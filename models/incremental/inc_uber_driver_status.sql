{{ config(materialized='incremental', unique_key='partner_account_uuid') }}

{%- set table_exists = table_exists(this.table, 'incremental') -%}

WITH ranked_driver_status AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY partner_account_uuid
               ORDER BY loadTimestamp DESC
           ) AS rank
    FROM {{ ref('stg_uber__driver_status') }}
    WHERE loadTimestamp > (
        {% if table_exists %}
            SELECT MAX(loadTimestamp) FROM {{ this }}
        {% else %}
            0
        {% endif %}
    )
)

SELECT 
    partner_account_uuid,
    first_name,
    last_name,
    phone,
    email,
    vehicle_plate,
    onboarding_status,
    status,
    timestamp,
    loadTimestamp,
    extraction_ts
FROM ranked_driver_status
WHERE rank = 1