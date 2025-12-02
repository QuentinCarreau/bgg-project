with 

source as (

    select * from {{ source('dataset_sales_clean', 'data_sales_clean') }}

),

renamed as (

    select

    from source

)

select * from renamed