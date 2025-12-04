with 

source as (

    select * from {{ source('bgg_dataset_2', 'join_bgg_split_all_type') }}

),

renamed as (

    select
        id,
        game_name,
        year,
        min_players,
        max_players,
        game_duration,
        age_min,
        categories,
        mechanics,
        family,
        designer,
        artist,
        publisher,
        nb_of_ratings,
        avg_rating,
        bayes_avg,
        owned,
        people_wishing,
        nb_weights,
        avg_difficulty,
        type

    from source

)

select * from renamed