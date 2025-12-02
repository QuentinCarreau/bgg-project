WITH join_1 AS ( 
    SELECT
        *
    FROM {{ ref('stg_bgg_dataset_1__BGG_Data_Set') }}
    LEFT JOIN {{ ref('stg_bgg_dataset_3__bgg_categories') }}
        USING (id)
)

,

joint_2 AS (
    SELECT *
    FROM join_1
    LEFT JOIN {{ ref('stg_bgg_dataset_3__bgg_mechanics') }}
        USING (id)
)

SELECT *
FROM joint_2
LEFT JOIN {{ ref('stg_bgg_dataset_3__bgg_themes') }}
    USING (id)