with 

source as (

    select * from {{ source('bgg_dataset_1', 'BGG_Data_set') }}

),

renamed as (

    select
        
    from source

)

select * from renamed