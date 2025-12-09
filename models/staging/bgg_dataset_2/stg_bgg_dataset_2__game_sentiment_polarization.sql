with 

source as (

    select * from {{ source('bgg_dataset_2', 'game_sentiment_polarization') }}

),

renamed as (

    select
        `unnamed: 0` as user_id,
        user as user_name,
        rating as avg_rating,
        comment,
        id,
        `name` as game_name,
        vader_compound as vader,
        IF(vader_compound >= 0.05, 1, 0) AS vader_positive,
        IF(vader_compound <= -0.05, 1, 0) AS vader_negative

    from source

)

select * from renamed