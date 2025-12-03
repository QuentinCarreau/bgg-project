with 

source as (

    select * from {{ source('bgg_dataset_3', 'bgg_categories') }}

),

renamed as (

    select
        bggid as id,
        exploration as categories_exploration,
        miniatures as categories_miniatures,
        territory_building as categories_territory_building,
        card_game as categories_card_game,
        educational as categories_educational,
        puzzle as categories_puzzle,
        collectible_components as categories_collectible_components,
        word_game as categories_word_game,
        print___play as categories_print_play,
        electronic as categories_electronic

    from source

)

select 
    "categories" AS category,
    * 
from renamed