{{config(materialized="table")}}

WITH first_join AS (
    SELECT
        t1.id,
        t1.published_year,
        t1.game_name,
        t1.min_players,
        t1.max_players,
        t1.age_min,
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
        first_join.published_year,
        first_join.game_name,
        first_join.min_players,
        first_join.max_players,
        first_join.age_min,
        first_join.type,
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
        t3.difficulty

FROM first_join
LEFT JOIN {{ ref('stg_bgg_kpi_exploration__bgg_games_enriched_with_kpis_part_two') }} as t3
    USING(id)
)
,
third_join AS (
    SELECT 
        second_join.id,
        second_join.published_year,
        second_join.game_name,
        second_join.min_players,
        second_join.max_players,
        second_join.age_min,
        second_join.type,
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
        t4.engagement_rate

    FROM second_join
    LEFT JOIN {{ ref('stg_bgg_dataset_2__mapping_table_engagement_rate') }} as t4
        USING(id)
),

fourth_join AS (
    SELECT
        third_join.*,
        t6.vader,
        SAFE_DIVIDE((owned+people_wishing),nb_of_ratings) as popularity_score
    FROM third_join
    INNER JOIN {{ ref('stg_bgg_dataset_2__avg_vader_rating_reviews') }} AS t6
        USING(id)
)
,

SELECT
    fourth_join.*,
    CASE
        WHEN game_duration <= 5 THEN "0-5 min"
        WHEN game_duration <= 10 THEN "5-10 min"
        WHEN game_duration <= 20 THEN "10-20 min"
        WHEN game_duration <= 30 THEN "20-30 min"
        WHEN game_duration <= 45 THEN "30-45 min"
        WHEN game_duration <= 60 THEN "45-60 min"
        WHEN game_duration <= 90 THEN "60-90 min"
        ELSE "> 90 min"
    END AS game_duration_intervals
FROM fourth_join
LEFT JOIN {{ ref('stg_bgg_dataset_2__bgg_game_duration_cat_2') }} AS t7
        USING(id)
