SELECT id, type, game_name, categories, mechanics, family, nb_of_ratings, avg_rating,
  COUNT(comment) as comment,
  SUM(push_your_luck) as push_your_luck, 
  SUM(player_elimination) as player_elimination, 
  SUM(communication_limit) as communication_limit, 
  SUM(spie) as spie, 
  SUM(secret) as secret, 
  SUM(feeling) as feeling, 
  SUM(laught) as laught, 
  SUM(accessibility) as accessibility, 
  SUM(beautiful) as beautiful, 
  SUM(easy) as easy, 
  SUM(deduction) as deduction, 
  SUM(bluffing) as bluffing, 
  SUM(fluidity) as fluidity, 
  SUM(hilarious) as hilarious, 
  SUM(entertaining) as entertaining, 
  SUM(chaotic) as chaotic
FROM {{ ref('master_bgg_features_count_enriched') }}
GROUP BY id, type, game_name, categories, mechanics, family, nb_of_ratings, avg_rating
ORDER BY comment DESC
