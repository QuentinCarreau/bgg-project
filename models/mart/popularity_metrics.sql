SELECT 
    safe_divide(nb_wanting, nb_owned) as interest_to_ownership, 
    nb_owned, 
    nb_wanting, 
    safe_divide(nb_comments, user_rated) as engagement_rate, 
    user_rated, nb_comments, 
    nb_wishing, 
    dbt.game_name, 
    safe_divide(nb_owned, user_rated) as popularity_rate,
    dbt.id, 
    dbt.type, 
    dbt.categories, 
    dbt.mechanics
  FROM {{ ref('stg_bgg_dataset_2__games_detailed_info2025') }} as gdi
  INNER JOIN {{ ref('master_bgg') }} as dbt
  using(id)
  order by popularity_rate desc