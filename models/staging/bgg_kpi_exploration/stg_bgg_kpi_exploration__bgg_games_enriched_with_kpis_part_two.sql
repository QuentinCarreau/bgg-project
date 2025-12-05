with 

source as (

    select * from {{ source('bgg_kpi_exploration', 'bgg_games_enriched_with_kpis_part_two') }}

),

renamed as (

    select
        id,
        name as game_name,
        year,
        min_players,
        max_players,
        game_duration,
        age_min,
        categories,
        mechanics,
        family,
        publisher,
        nb_of_ratings,
        avg_rate as avg_rating,
        bayes_avg,
        owned,
        people_wishing,
        nb_weights,
        avg_difficulty,
        bgg_rank,
        difficulty,
        engaging_rate

    from source

)

select * from renamed