with 

source as (

    select * from {{ source('bgg_dataset_2', 'game_sentiment_polarization') }}

),

renamed as (

    select
        unnamed: 0 as user_id,
        user as user_name,
        rating as user_rating,
        comment,
        id,
        name,
        vader_compound as vader

    from source

)

select * from renamed