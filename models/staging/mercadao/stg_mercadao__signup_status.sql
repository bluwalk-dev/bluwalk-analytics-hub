{{ config(materialized='table') }}

with

source as (
    select
        *
    from {{ source('mercadao', 'signup_status') }} 
),

transformation as (

    SELECT
    
        CAST(TRIM(email) AS STRING) as email,
        CASE
            WHEN CAST(TRIM(profile_created) AS STRING) = 'Sim' THEN true
            ELSE false
        END as profile_created,
        CASE
            WHEN CAST(TRIM(account_status) AS STRING) = 'Conta ativa' THEN true
            ELSE false
        END as account_created

    FROM source

)

SELECT * FROM transformation