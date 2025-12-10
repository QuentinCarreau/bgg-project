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
WHERE difficulty = "casual"
AND  min_players_intervals in ('2','3')
AND max_players_intervals in ('3-4', '5-6', '7-10')
AND game_duration_intervals not in ('0-5 min', '5-10 min', '> 60 min')
AND 'Hand Management' IN UNNEST(mechanics) 
AND 'Communication Limits' IN UNNEST(mechanics) 
AND 'Player Elimination' IN UNNEST(mechanics) 
AND 'Push Your Luck' IN UNNEST(mechanics) 
AND 'Card game' IN UNNEST(categories) 
AND 'Deduction' IN UNNEST(categories) 
AND 'Action / Dexterity' IN UNNEST(categories) 
AND 'Spies / Secret Agents' IN UNNEST(categories) 
AND type = "Party Game"
AND published_year BETWEEN "2010-01-01" AND "2022-01-01"