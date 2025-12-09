SELECT 
    safe_divide(nb_wanting, nb_owned) as interest_to_ownership, 
  FROM {{ ref('stg_bgg_dataset_2__games_detailed_info2025') }} as gdi
  INNER JOIN {{ ref('master_bgg') }} as dbt
  using(id)
  order by popularity_score desc