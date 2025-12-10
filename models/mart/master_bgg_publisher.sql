{{config(materialized="table")}}

WITH unnested_publisher AS (
    SELECT
        t1.* EXCEPT(publisher),
        publisher_clean
    FROM {{ ref('master_bgg_features_count_enriched') }} AS t1
    LEFT JOIN UNNEST(t1.publisher) AS publisher_clean
)
,
join_table AS (
    SELECT
        t2.* EXCEPT(publisher),
        unnested_publisher.publisher_clean,
        REPLACE(unnested_publisher.publisher_clean,"(Self-Published)","Auto-édition") AS self_published_flag,
        REPLACE(unnested_publisher.publisher_clean,"(Web published)","Auto-édition") AS web_published_flag,
    FROM {{ ref('master_bgg_features_count_enriched') }} AS t2
    LEFT JOIN unnested_publisher
        USING(id)
)

SELECT
    join_table.*,
    CASE
        WHEN join_table.publisher_clean = "(Self-Published)" THEN "Auto-édition"
        WHEN join_table.publisher_clean = "(Web published)" THEN "Auto-édition"
        ELSE join_table.publisher_clean
    END AS publisher,
    CASE
        WHEN join_table.self_published_flag = "Auto-édition" THEN "Auto-édition"
        WHEN join_table.web_published_flag = "Auto-édition" THEN "Auto-édition"
        ELSE "Autres"
    END AS auto_edition_flag
FROM join_table