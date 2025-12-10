SELECT
    id,
    game_name,
    COUNT(comment) as nb_comment,
    AVG(vader) as mean_vader,
    SUM(vader_positive) as tot_vader_positive,
    SUM(vader_negative) as tot_vader_negative,
    SAFE_DIVIDE(SUM(vader_positive - vader_negative), COUNT(comment)) as ratio_vader
FROM {{ ref('stg_bgg_dataset_2__game_sentiment_polarization') }}
GROUP BY id, game_name