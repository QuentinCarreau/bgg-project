with 

source as (

    select * from {{ source('bgg_dataset_2', 'game_sentiment_polarization_filter') }}

),

renamed as (

    select
        id,
        game_name,
        comment,
        vader_compound as vader

    from source

)

select * from renamed