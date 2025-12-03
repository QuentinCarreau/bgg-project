WITH reviews_raw AS (
    SELECT *
    FROM {{ ref('stg_bgg_dataset_2__bgg_reviews_clean') }} AS rc
    LEFT JOIN {{ ref('stg_bgg_dataset_2__bgg_games_reviews_info2025_cleaned') }} AS gr
    USING (id)
),

cleaned AS (
    SELECT
        * EXCEPT(name_1),

        -- clean list-like columns
        REGEXP_REPLACE(
            REGEXP_REPLACE(designer, r"[\[\]']", ""),
            r"\s*,\s*", ", "
        ) AS designer_clean,

        REGEXP_REPLACE(
            REGEXP_REPLACE(artist, r"\[|\]|'", ""),
            r"\s*,\s*", ","
        ) AS artist_clean,

        REGEXP_REPLACE(
            REGEXP_REPLACE(publisher, r"\[|\]|'", ""),
            r"\s*,\s*", ","
        ) AS publisher_clean
    FROM reviews_raw
)

SELECT *
FROM (
    SELECT *
    FROM cleaned
)

