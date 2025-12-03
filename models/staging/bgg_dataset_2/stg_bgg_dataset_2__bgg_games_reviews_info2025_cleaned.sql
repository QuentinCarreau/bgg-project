with 

source as (

    select * from {{ source('bgg_dataset_2', 'bgg_games_reviews_info2025_cleaned') }}

),

renamed as (

    select
        id,
        name,
        description,
        year,
        min_players,
        max_players,
        game_duration,
        age_min,
        designer,
        artist,
        publisher,
        nb_of_ratings,
        avg_rate,
        bayes_avg,
        owned_by,
        people_wanting,
        people_wishing,
        numweights,
        game_difficulty

    from source

)

select * from renamed