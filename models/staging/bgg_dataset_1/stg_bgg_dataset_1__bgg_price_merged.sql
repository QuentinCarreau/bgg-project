with 

source as (

    select * from {{ source('bgg_dataset_1', 'bgg_price_merged') }}

),

renamed as (

    select

    from source

)

select * from renamed