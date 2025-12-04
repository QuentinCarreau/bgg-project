with 

source as (

    select * from {{ source('bgg_dataset_2', 'games_detailed_info2025') }}

),

renamed as (

    select
        id,
        `board game rank` as board_game_rank,

    from source

)

select * from renamed