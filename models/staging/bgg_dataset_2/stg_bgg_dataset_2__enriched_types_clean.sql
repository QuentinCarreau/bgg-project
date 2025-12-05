with 

source as (

    select * from {{ source('bgg_dataset_2', 'enriched_types_clean') }}

),

renamed as (

    select
        id,
        enriched_type as type,
    from source

)

select * from renamed