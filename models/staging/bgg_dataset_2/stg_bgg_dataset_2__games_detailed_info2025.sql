with 

source as (

    select * from {{ source('bgg_dataset_2', 'games_detailed_info2025') }}

),

renamed as (

    select

    from source

)

select * from renamed