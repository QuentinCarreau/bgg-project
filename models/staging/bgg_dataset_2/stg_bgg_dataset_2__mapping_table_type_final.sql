with 

source as (

    select * from {{ source('bgg_dataset_2', 'mapping_table_type_final') }}

),

renamed as (

    select
        id,
        categories,
        final_type

    from source

)

select * from renamed