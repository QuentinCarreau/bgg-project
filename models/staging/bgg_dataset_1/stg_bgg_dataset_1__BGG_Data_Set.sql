with 

source as (

    select * from {{ source('bgg_dataset_1', 'BGG_Data_Set') }}

),

renamed as (

    select
        id,
        name,
        `year published` as year,
        `min players` as min_players,
        `max players` as max_players,
        `play time` as play_time,
        `min age` as min_age,
        `users rated` as users_rated,
        `rating average` as rating_average,
        `bgg rank` as bgg_rank,
        `complexity average` as complexity_average,
        `owned users` as owned_users,
        mechanics,
        domains

    from source

)

select * from renamed
where id is not null