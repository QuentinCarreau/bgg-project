with 

source as (

    select * from {{ source('bgg_dataset_3', 'bgg_categories') }}

),

renamed as (

    select
        bggid as id,
        exploration,
        miniatures,
        territory_building,
        card_game,
        educational,
        puzzle,
        collectible_components,
        word_game,
        print___play,
        electronic

    from source

)

select * from renamed