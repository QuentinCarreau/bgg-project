with 

source as (

    select * from {{ source('bgg_dataset_2', 'bgg_reviews_clean') }}

),

renamed as (

    select
        users_id,
        users_name,
        rating,
        comment,
        game_id,
        name

    from source

)

select * from renamed