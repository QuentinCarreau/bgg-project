with 

source as (

    select * from {{ source('bgg_dataset_2', 'games_detailed_info2025') }}

),

renamed as (

    select
        id,
        `name` as game_name,
        `type`,
        `description`,
        CASE 
            WHEN yearpublished < 600 THEN 0  -- Remplacer par 0 si aberrant
            WHEN yearpublished > 2030 THEN 0  -- Remplacer par 0 si futur lointain
            ELSE yearpublished
        END AS yearpublished_clean,
        minplayers,
        maxplayers,
        minplaytime as min_playtime,
        maxplaytime as max_playtime,
        minage as min_age,
        boardgamecategory as category,
        boardgamemechanic as mechanic,
        boardgamefamily as family,
        boardgameexpansion as expansion,
        boardgameaccessory as accessory,
        boardgamecompilation as compilation,
        boardgameimplementation as implementation,
        boardgamedesigner as designer,
        boardgameartist as artist,
        boardgamepublisher as publisher,
        usersrated as user_rated,
        average as bgg_rate_average,
        bayesaverage as bondered_bgg_rate_average,
        `board game rank` as board_game_rank,
        `strategy game rank` as strategy_rank,
        `family game rank` as family_rank,
        stddev,
        median,
        owned as nb_owned,
        trading as nb_trading,
        wanting as nb_wanting,
        wishing as nb_wishing,
        numcomments as nb_comments,
        numweights as nb_weights,
        averageweight as avg_weight,
        boardgameintegration as integration,
        `abstract game rank` as abstract_rank,
        `party game rank` as party_game_rank,
        `thematic rank` as thematic_rank,
        `war game rank` as wargame_rank,
        `customizable rank` as customizable_rank,
        `children's game rank` as children_game_rank,
        `rpg item rank` as rpg_item_rank,
        `accessory rank` as accessory_rank

    from source

)

select * from renamed