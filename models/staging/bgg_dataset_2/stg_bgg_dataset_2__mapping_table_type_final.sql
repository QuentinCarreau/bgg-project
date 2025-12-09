with 

source as (

    select * from {{ source('bgg_dataset_2', 'mapping_table_type_final') }}

),

renamed as (

    select
        id,
        categories,
        IF (final_type = '', "Autre", final_type) as final_type

    from source

)

select * from renamed