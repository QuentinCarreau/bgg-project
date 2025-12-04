with 

source as (

    select * from {{ source('bgg_dataset_2', 'bgg_price') }}

),

renamed as (

    select
        versions,
        lang,
        product,
        external_id as id,
        name

    from source

)

select * from renamed