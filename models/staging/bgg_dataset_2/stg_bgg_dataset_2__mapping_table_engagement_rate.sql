with 

source as (

    select * from {{ source('bgg_dataset_2', 'mapping_table_engagement_rate') }}

),

renamed as (

    select
        id,
        engagement_rate

    from source

)

select * from renamed