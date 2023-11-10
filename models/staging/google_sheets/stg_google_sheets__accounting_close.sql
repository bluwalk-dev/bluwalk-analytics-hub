with

source as (
    select
        *
    from {{ source('google_sheets', 'fin_month_closing') }}
),

transformation as (

    SELECT
    
        CAST(month AS INT64) as year_month,
        CAST(estimation AS DATE) AS estimation_release,
        CAST(definitive AS DATE) AS definitive_release,
        CAST(estimationLT AS INT64) AS estimation_lt,
        CAST(definitiveLT AS INT64) AS definitive_lt

    FROM source

)

SELECT * FROM transformation

