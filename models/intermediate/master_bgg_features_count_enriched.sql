WITH feature_count as ( 
    SELECT
        bgg.id,
        bgg.published_year,
        bgg.year,
        bgg.type,
        bgg.game_name,
        bgg.categories,
        bgg.mechanics,
        bgg.family,
        bgg.publisher,
        bgg.designer,
        bgg.artist,
        bgg.nb_of_ratings,
        bgg.avg_rating,
        bgg.bayes_avg,
        bgg.owned,
        bgg.people_wishing,
        bgg.nb_weights,
        bgg.avg_difficulty,
        bgg.bgg_rank,
        bgg.engagement_rate,
        bgg.game_duration_intervals,
        bgg.min_players_intervals,
        bgg.max_players_intervals,
        bgg.difficulty,
        bgg.mean_price,
        bgg.estimated_turnover,
        bgg.price_over_quality,
        polar.mean_vader,
        polar.nb_comment,
        polar.tot_vader_positive,
        polar.tot_vader_negative,
        polar.ratio_vader
        FROM {{ ref('bgg_game_price_join') }} as bgg
        LEFT JOIN {{ ref('game_sentiment_aggreg') }} as polar
            USING(id)
)

SELECT
    feature_count.*,
    push_your_luck,
    player_elimination,
    communication_limit,
    spie,
    secret,
    feeling,
    laught,
    accessibility,
    quick,
    esthetic, 
    beautiful,
    easy,
    deduction,
    asymmetric,
    bluffing,
    fluidity,
    light,
    hilarious,
    entertaining,
    hidden_role,
    chaotic
FROM feature_count
LEFT JOIN {{ ref('good_features_count_review_sup_5_aggreg') }} as good_feat
    USING(id)
