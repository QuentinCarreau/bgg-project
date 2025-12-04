select
    *
FROM {{ ref('stg_bgg_vues__bgg_split_cats_mechs_family_year_clean') }}
LEFT JOIN {{ ref('stg_bgg_dataset_2__enriched_types_clean') }}
    USING(id)
