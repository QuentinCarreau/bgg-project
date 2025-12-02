with 

source as (

    select * from {{ source('bgg_dataset_2', 'bgg-26m-review') }}

),

renamed as (

    select

    from source

)

select * from renamed