with 

source as (

    select * from {{ source('bgg_vues', 'bgg_split_cats_mechs_family_year_clean') }}

),

renamed as (

    select
        id,
        name,
        final_year as year,
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
        avg_rate,
        bayes_avg,
        owned_by as owned,
        people_wishing,
        numweights as nb_weights,
        game_difficulty as avg_difficulty

    from source

)

select * from renamed