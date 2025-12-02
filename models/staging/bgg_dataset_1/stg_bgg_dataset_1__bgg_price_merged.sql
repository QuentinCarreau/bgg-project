with 

source as (

    select * from {{ source('bgg_dataset_1', 'bgg_price_merged') }}

),

renamed as (

    select
<<<<<<< HEAD
=======
        external_id as id,
        name,
        price,
        product,
        shipping,
        stock,
        country,
        versions
>>>>>>> 7f6527120f4ff510561f15c0c19cc30132c9b3b7

    from source

)

select * from renamed