with 

source as (

    select * from {{ source('bgg_dataset_2', 'bgg_game_duration_cat_2') }}

),

renamed as (

    select
        id,
        f0_ as game_duration

    from source

)

select * from renamed