WITH game_mechanic AS (
    SELECT 
        id,
        published_year,
        type,
        game_name,
        categories,
        mechanics,
        family,
        publisher,
        designer,
        artist,
        nb_of_ratings,
        avg_rating,
        bayes_avg,
        owned,
        people_wishing,
        nb_weights,
        avg_difficulty,
        bgg_rank,
        engagement_rate,
        game_duration_intervals,
        min_players_intervals,
        max_players_intervals,
        difficulty,
        mean_price,
        estimated_turnover,
        price_over_quality,
        mean_vader,
        nb_comment,
        tot_vader_positive,
        tot_vader_negative,
        ratio_vader,
        push_your_luck,
        player_elimination,
        communication_limit,
        spie,
        easy,
        deduction,
        entertaining
    FROM {{ ref('master_bgg_features_count_enriched') }}
    WHERE 'Card Game' IN UNNEST(categories) 
    OR 'Deduction' IN UNNEST(categories) 
    OR 'Action / Dexterity' IN UNNEST(categories) 
    OR 'Spies / Secret Agents' IN UNNEST(categories) 
),

game_categorie AS (
    SELECT 
        id,
        published_year,
        type,
        game_name,
        categories,
        mechanics,
        family,
        publisher,
        designer,
        artist,
        nb_of_ratings,
        avg_rating,
        bayes_avg,
        owned,
        people_wishing,
        nb_weights,
        avg_difficulty,
        bgg_rank,
        engagement_rate,
        game_duration_intervals,
        min_players_intervals,
        max_players_intervals,
        difficulty,
        mean_price,
        estimated_turnover,
        price_over_quality,
        mean_vader,
        nb_comment,
        tot_vader_positive,
        tot_vader_negative,
        ratio_vader,
        push_your_luck,
        player_elimination,
        communication_limit,
        spie,
        easy,
        deduction,
        entertaining
    FROM {{ ref('master_bgg_features_count_enriched') }}
    WHERE 'Hand Management' IN UNNEST(mechanics) 
    OR 'Communication Limits' IN UNNEST(mechanics) 
    OR 'Player Elimination' IN UNNEST(mechanics) 
    OR 'Push Your Luck' IN UNNEST(mechanics) 
)

SELECT 
    p1.id,
    p1.published_year,
    p1.type,
    p1.game_name,
    p1.categories,
    p1.mechanics,
    p1.family,
    p1.publisher,
    p1.designer,
    p1.artist,
    p1.nb_of_ratings,
    p1.avg_rating,
    p1.bayes_avg,
    p1.owned,
    p1.people_wishing,
    p1.nb_weights,
    p1.avg_difficulty,
    p1.bgg_rank,
    p1.engagement_rate,
    p1.game_duration_intervals,
    p1.min_players_intervals,
    p1.max_players_intervals,
    p1.difficulty,
    p1.mean_price,
    p1.estimated_turnover,
    p1.price_over_quality,
    p1.mean_vader,
    p1.nb_comment,
    p1.tot_vader_positive,
    p1.tot_vader_negative,
    p1.ratio_vader,
    p1.push_your_luck,
    p1.player_elimination,
    p1.communication_limit,
    p1.spie,
    p1.easy,
    p1.deduction,
    p1.entertaining
FROM game_mechanic as p1
INNER JOIN game_categorie
    USING (id)
WHERE p1.difficulty = "casual"
AND p1.type = "Party Game"
AND p1.min_players_intervals in ('2','3')
AND p1.max_players_intervals in ('3-4', '5-6', '7-10')
AND p1.game_duration_intervals not in ('0-5 min', '5-10 min', '> 60 min')
AND p1.published_year BETWEEN "2010-01-01" AND "2022-01-01"
ORDER BY onwed DESC