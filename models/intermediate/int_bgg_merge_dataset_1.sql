SELECT
        d1.id,
        d1.name,
        d1.year,
        d2.price,
        d2.product,
        d2.shipping,
        d2.stock,
        d2.country,
        d2.versions,
        d1.min_players,
        d1.max_players,
        d1.play_time,
        d1.min_age,
        d1.users_rated,
        d1.rating_average,
        d1.bgg_rank,
        d1.complexity_average,
        d1.owned_users,
        d1.mechanics,
        d1.domains
 FROM {{ ref('stg_bgg_dataset_1__BGG_Data_Set') }} d1
 LEFT JOIN {{ ref('stg_bgg_dataset_1__bgg_price_merged') }} d2
     USING(id)
 ORDER BY name