with

source as (
    select
        *
    from {{ source('hubspot', 'engagement_email') }}
),

transformation as (

    select
        CAST(property_hs_unique_id AS STRING) as property_hs_unique_id,
        CAST(engagement_id AS INT64) AS engagement_id,
        CAST(property_hs_createdate AS TIMESTAMP) AS create_ts,
        DATETIME(CAST(property_hs_createdate AS TIMESTAMP), 'Europe/Lisbon') AS create_local_time,
        CAST(property_hs_object_id AS INT64) AS object_id,
        CAST(property_hubspot_owner_id AS INT64) as hubspot_owner_id,
        CASE 
            WHEN property_hs_email_direction = 'EMAIL' THEN 'outbound'
            ELSE 'inbound'
        END as email_direction,
        CAST(property_hs_email_to_email AS STRING) AS email_address_to,
        CONCAT(CAST(property_hs_email_to_firstname as string), ' ', CAST(property_hs_email_to_lastname as string)) as email_name_to,
        CAST(property_hs_email_from_email as string) as email_address_from,
        CONCAT(CAST(property_hs_email_from_firstname as string), ' ', CAST(property_hs_email_from_lastname as string)) as email_name_from

    from source
    where _fivetran_deleted is false

)

select * from transformation