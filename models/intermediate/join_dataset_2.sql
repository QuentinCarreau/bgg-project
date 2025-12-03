WITH reviews AS ( 
    SELECT
        *
FROM {{ ref('stg_bgg_dataset_2__bgg_reviews_clean') }} AS rc
LEFT JOIN {{ ref('stg_bgg_dataset_2__bgg_games_reviews_info2025_cleaned') }} AS gr 
    USING (id)
)

SELECT *, 
FROM reviews