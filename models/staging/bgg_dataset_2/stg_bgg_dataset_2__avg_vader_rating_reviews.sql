with 

source as (

    select * from {{ source('bgg_dataset_2', 'avg_vader_rating_reviews') }}

),

renamed as (

    select
        id,
        name,
        vader_compound as vader,
        rating

    from source

)

select * from renamed