{{config(materialized="table")}}

WITH unnested_publisher AS (
    SELECT
        t1.* EXCEPT(publisher),
        publisher_clean
    FROM {{ ref('master_bgg_features_count_enriched') }} AS t1
    LEFT JOIN UNNEST(t1.publisher) AS publisher_clean
)
,
join_table AS (
    SELECT
        t2.* EXCEPT(publisher),
        unnested_publisher.publisher_clean,
        REPLACE(unnested_publisher.publisher_clean,"(Self-Published)","Auto-édition") AS self_published_flag,
        REPLACE(unnested_publisher.publisher_clean,"(Web published)","Auto-édition") AS web_published_flag,
    FROM {{ ref('master_bgg_features_count_enriched') }} AS t2
    LEFT JOIN unnested_publisher
        USING(id)
)
,
all_info AS (
    SELECT
        join_table.*,
        CASE
            WHEN join_table.publisher_clean = "(Self-Published)" THEN "Auto-édition"
            WHEN join_table.publisher_clean = "(Web published)" THEN "Auto-édition"
            ELSE join_table.publisher_clean
        END AS publisher_new,
        CASE
            WHEN join_table.self_published_flag = "Auto-édition" THEN "Auto-édition"
            WHEN join_table.web_published_flag = "Auto-édition" THEN "Auto-édition"
            ELSE "Autres"
        END AS auto_edition_flag
    FROM join_table
)

SELECT
    all_info.id,
    all_info.published_year,
    all_info.type,
    all_info.game_name,
    all_info.categories,
    all_info.mechanics,
    all_info.family,
    all_info.designer,
    all_info.artist,
    all_info.nb_of_ratings,
    all_info.avg_rating,
    all_info.bayes_avg,
    all_info.owned,
    all_info.people_wishing,
    all_info.nb_weights,
    all_info.avg_difficulty,
    all_info.bgg_rank,
    all_info.engagement_rate,
    all_info.game_duration_intervals,
    all_info.min_players_intervals,
    all_info.max_players_intervals,
    all_info.difficulty,
    all_info.mean_price,
    all_info.estimated_turnover,
    all_info.price_over_quality,
    all_info.mean_vader,
    all_info.nb_comment,
    all_info.tot_vader_positive,
    all_info.tot_vader_negative,
    all_info.ratio_vader,
    all_info.push_your_luck,
    all_info.player_elimination,
    all_info.communication_limit,
    all_info.spie,
    all_info.secret,
    all_info.feeling,
    all_info.laught,
    all_info.accessibility,
    all_info.quick,
    all_info.esthetic,
    all_info.beautiful,
    all_info.easy,
    all_info.deduction,
    all_info.asymmetric,
    all_info.bluffing,
    all_info.fluidity,
    all_info.light,
    all_info.hilarious,
    all_info.entertaining,
    all_info.hidden_role,
    all_info.chaotic,
    all_info.publisher_new AS publisher,
    all_info.self_published_flag,
    all_info.web_published_flag,
    all_info.auto_edition_flag
FROM all_info