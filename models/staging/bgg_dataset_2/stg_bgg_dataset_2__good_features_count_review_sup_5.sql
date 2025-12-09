with 

source as (

    select * from {{ source('bgg_dataset_2', 'good_features_count_review_sup_5') }}

),

renamed as (

    select
        id,
        year,
        difficulty,
        rating,
        comment,
        `is_push your luck_mentioned` as push_your_luck,
        `is_player elimination_mentioned` as player_elimination,
        `is_communication limit_mentioned` as communication_limit,
        is_spie_mentioned as spie,
        is_secret_mentioned as secret,
        is_feeling_mentioned as feeling,
        is_laught_mentioned as laught,
        is_accessibility_mentioned as accessibility,
        is_quick_mentioned as quick,
        is_esthetic_mentioned as esthetic, 
        is_beautiful_mentioned as beautiful,
        is_easy_mentioned as easy,
        is_deduction_mentioned as deduction,
        is_asymmetric_mentioned as asymmetric,
        is_bluffing_mentioned as bluffing,
        is_fluidity_mentioned as fluidity,
        is_light_mentioned as light,
        is_hilarious_mentioned as hilarious,
        is_entertaining_mentioned as entertaining,
        `is_hidden role_mentioned` as hidden_role,
        is_chaotic_mentioned as chaotic

    from source

)

select * from renamed