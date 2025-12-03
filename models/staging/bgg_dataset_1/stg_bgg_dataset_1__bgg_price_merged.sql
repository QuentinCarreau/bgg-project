with 

source as (

    select * from {{ source('bgg_dataset_1', 'bgg_price_merged') }}

),

renamed as (

    select
        external_id as id,
        name,
        price,
        product,
        shipping,
        stock,
        country,
        versions

    from source

)

select
    id,
    price,
    product,
    shipping,
    stock,
    country,
    versions
from renamed