{{config(materialized="table")}}

WITH first_join AS (
    SELECT
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
)

SELECT
    first_join.id,
    first_join.game_name,
    first_join.year,
    first_join.min_players,
    first_join.max_players,
    first_join.game_duration,
    first_join.age_min,
    t3.categories,
    first_join.mechanics,
    first_join.family,
    first_join.designer,
    first_join.artist,
    first_join.publisher,
    first_join.nb_of_ratings,
    first_join.avg_rating,
    first_join.bayes_avg,
    first_join.owned,
    first_join.people_wishing,
    first_join.nb_weights,
    first_join.avg_difficulty,
    t3.bgg_rank,
    t3.difficulty,
    t3.engaging_rate,
    first_join.type
FROM first_join
LEFT JOIN {{ ref('stg_bgg_kpi_exploration__bgg_games_enriched_with_kpis_part_two') }} as t3
    USING(id)
