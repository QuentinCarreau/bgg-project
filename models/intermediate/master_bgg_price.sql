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
,
second_join AS (
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
        first_join.type
FROM first_join
LEFT JOIN {{ ref('stg_bgg_kpi_exploration__bgg_games_enriched_with_kpis_part_two') }} as t3
    USING(id)
)
,
third_join AS (
    SELECT 
        second_join.id,
        second_join.game_name,
        second_join.year,
        second_join.min_players,
        second_join.max_players,
        second_join.game_duration,
        second_join.age_min,
        second_join.categories,
        second_join.mechanics,
        second_join.family,
        second_join.designer,
        second_join.artist,
        second_join.publisher,
        second_join.nb_of_ratings,
        second_join.avg_rating,
        second_join.bayes_avg,
        second_join.owned,
        second_join.people_wishing,
        second_join.nb_weights,
        second_join.avg_difficulty,
        second_join.bgg_rank,
        second_join.difficulty,
        t4.engagement_rate,
        second_join.type
    FROM second_join
    LEFT JOIN {{ ref('stg_bgg_dataset_2__mapping_table_engagement_rate') }} as t4
        USING(id)
)
,
fourth_join AS (
    SELECT 
        third_join.id,
        third_join.game_name,
        third_join.year,
        third_join.min_players,
        third_join.max_players,
        third_join.game_duration,
        third_join.age_min,
        third_join.categories,
        third_join.mechanics,
        third_join.family,
        third_join.designer,
        third_join.artist,
        third_join.publisher,
        third_join.nb_of_ratings,
        third_join.avg_rating,
        third_join.bayes_avg,
        third_join.owned,
        third_join.people_wishing,
        third_join.nb_weights,
        third_join.avg_difficulty,
        third_join.bgg_rank,
        third_join.difficulty,
        third_join.engagement_rate,
        third_join.type,
        t5.versions,
        t5.lang,
        t5.product as price,
        ROUND(t5.product / third_join.avg_rating , 2) AS price_over_quality,
        t5.game_name as game_name_price
    FROM third_join
    LEFT JOIN {{ ref('stg_bgg_dataset_2__bgg_price') }} AS t5
        USING(id)
)
,
fifth_join AS (
    SELECT
        fourth_join.*,
        t6.vader
    FROM fourth_join
    LEFT JOIN {{ ref('stg_bgg_dataset_2__avg_vader_rating_reviews') }} AS t6
        USING(id)
)

SELECT *
FROM fifth_join
WHERE price >= 5 AND price <= 100 