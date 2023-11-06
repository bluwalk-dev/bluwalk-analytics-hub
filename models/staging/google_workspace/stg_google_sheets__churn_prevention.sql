with

    source as (select * from {{ source("google_sheets", "churn_prevention") }}),

    transformation as (

        SELECT
            CAST(userPartner_ID AS INT64) as contact_id,
            CAST(Nome AS STRING) as user_name,
            CAST(Cidade AS STRING) as user_location,
            CAST(Raz__o AS STRING) as exit_reason,
            CAST(Data_Contacto AS DATE) as contacted_at,
            CAST(Data_Devolu____o AS DATE) as return_at,
            CAST(Contra_medida AS STRING) as retention_measure,
            CAST(Reten____o AS STRING) retained,
            CAST(Preven____o AS STRING) prevention,
            CAST(Notas AS STRING) comments
        FROM source
        WHERE userPartner_ID IS NOT NULL

    )

select *
from transformation
