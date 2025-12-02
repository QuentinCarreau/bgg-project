with 

source as (

    select * from {{ source('bgg_store_top_sales', 'bgg-clean') }}

),

renamed as (

    select

    from source

)

select * from renamed