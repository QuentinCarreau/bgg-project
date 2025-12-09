SELECT
    id,
    AVG(product) as product
FROM {{ ref('stg_bgg_dataset_2__bgg_price') }}
WHERE product < 500 AND product > 0
GROUP BY id