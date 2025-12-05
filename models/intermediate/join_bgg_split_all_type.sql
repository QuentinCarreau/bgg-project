{{config(materialized="table")}}

select
    t1.id,
    t1.game_name,
    t1.year,
    t1.min_players,
    t1.max_players,
    t1.game_duration,
    t1.age_min,
    t1.categories,
    t1.mechanics,
    t1.family,
    t1.designer,
    t1.artist,
    t1.publisher,
    t1.nb_of_ratings,
    t1.avg_rating,
    t1.bayes_avg,
    t1.owned,
    t1.people_wishing,
    t1.nb_weights,
    t1.avg_difficulty,
    t2.final_type as type
FROM {{ ref('stg_bgg_vues__bgg_split_cats_mechs_family_year_clean') }} as t1
LEFT JOIN {{ ref('stg_bgg_dataset_2__mapping_table_type_final') }} as t2
    USING(id)
