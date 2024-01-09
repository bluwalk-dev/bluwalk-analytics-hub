with

source as (
    select
        *
    from {{ source('generic', 'calendar') }}
),

transformation as (

    SELECT 
        CAST(data as DATE) as date,
        CAST(ano as INT) as year,
        CAST(trimestre as INT) as quarter,
        CAST(mes as INT) as month,
        CAST(dia as INT) as day,
        CAST(FORMAT_DATE('%w', CAST(data AS DATE)) AS INT64) +1 as week_day,
        CAST(FORMAT_DATE('%u', CAST(data AS DATE)) AS INT64) as week_day_iso,
        CAST(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(mes_nome, 'Janeiro', 'January'), 'Fevereiro', 'February'), 'Março', 'March'), 'Abril', 'April'), 'Maio', 'May'), 'Junho', 'June'), 'Julho', 'July'), 'Agosto', 'August'), 'Setembro', 'September'), 'Outubro', 'October'), 'Novembro', 'November'), 'Dezembro', 'December') as STRING) as month_name,
        CAST(replace(replace(replace(replace(replace(replace(replace(dia_nome, 'Segunda', 'Monday'), 'Terça', 'Tuesday'), 'Quarta', 'Wednesday'), 'Quinta', 'Thursday'), 'Sexta', 'Friday'), 'Sábado', 'Saturday'), 'Domingo', 'Sunday') as STRING) as dayName,
        CAST(semana as INT) as week,
        CAST(payment_cycle as INT) as year_week,
        CAST(ano_mes as INT) as year_month,
        CAST(CONCAT(CAST(ano as INT), 'Q', CAST(trimestre as INT)) AS STRING) as year_quarter,
        CAST(payroll_month as INT) as year_month_payroll
    FROM source

)

select * from transformation