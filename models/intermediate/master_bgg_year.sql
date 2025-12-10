SELECT
    m_bgg.*,
    date(t5.yearpublished_clean, 1, 1) as year_published
FROM {{ ref('master_bgg') }} as m_bgg
INNER JOIN {{ ref('stg_bgg_dataset_2__games_detailed_info2025') }} as t5
USING(id)
WHERE yearpublished_clean != 0