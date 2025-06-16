with

source as (
    select
        *
    from {{ source('hubspot_drivfit', 'contact') }}
),

transformation as (

    SELECT
    
        CAST(id AS INT64) hs_contact_id,
        CAST(property_firstname AS STRING) first_name,
        CAST(property_lastname AS STRING) last_name,
        CAST(property_email AS STRING) email,
        CAST(is_deleted AS BOOL) is_deleted,
        CAST(property_hs_merged_object_ids AS STRING) merged_objects,
        CAST(property_hs_calculated_phone_number AS STRING) contact_phone_nr,
        property_birthday,
        property_mobilephone,
        property_address,
        property_zip,
        property_city,
        property_cidade_de_operacao,
        property_numero_cartao_cidadao,
        property_nr_carta_conducao,
        property_carta_conducao,
        property_nif_contacto

    FROM source
    WHERE _fivetran_deleted IS FALSE

)

select * from transformation