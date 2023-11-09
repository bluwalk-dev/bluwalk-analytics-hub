with

source as (
    select
        *
    from {{ source('mercadao', 'training_schedule') }}
),

transformation as (

    SELECT
    
        CAST(ID_Cliente AS INT64) contact_id,
        PARSE_DATE('%Y/%m/%d', IF(Data_em_que_se_agendou is null, Data_para_que_se_agendou, Data_em_que_se_agendou)) training_scheduling_date,
        PARSE_DATE('%Y/%m/%d', Data_para_que_se_agendou) training_date,
        Hor__rio training_time,
        CAST(replace(Contacto, ' ','') AS STRING) phone_nr,
        CAST(E_mail AS STRING) email,
        CAST(Zona_Pretendida AS STRING) training_location,
        UPPER(Avalia____o__48h_) training_outcome

    FROM source
    WHERE Data_para_que_se_agendou is not null and PARSE_DATE('%Y/%m/%d', Data_para_que_se_agendou) >= '2022-01-01'

)

SELECT * FROM transformation
