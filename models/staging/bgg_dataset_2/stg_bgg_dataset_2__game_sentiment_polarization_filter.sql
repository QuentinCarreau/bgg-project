with 

source as (

    select * from {{ source('bgg_dataset_2', 'game_sentiment_polarization_filter') }}

),

renamed as (

    select
        id,
        game_name,
        comment,
        vader_compound as vader,
        IF(vader_compound >= 0.05, 1, 0) AS vader_positive,
        IF(vader_compound <= -0.05, 1, 0) AS vader_negative
    from source

)

select * from renamed