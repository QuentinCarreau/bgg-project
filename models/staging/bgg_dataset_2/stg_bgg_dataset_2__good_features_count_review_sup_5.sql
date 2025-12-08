with 

source as (

    select * from {{ source('bgg_dataset_2', 'good_features_count_review_sup_5') }}

),

renamed as (

    select
        id,
        difficulty,
        rating,
        comment,
        is_fun_mentioned as fun,
        `is_simple rules_mentioned` as simple_rules,
        is_accessibility_mentioned as accessibility,
        is_quick_mentioned as quick,
        is_esthetic_mentioned as esthetic,
        is_beautiful_mentioned as beautiful,
        is_elegant_mentioned as elegant,
        `is_easy _mentioned` as easy,
        `is_ice breaker_mentioned` as ice_breaker,
        is_deduction_mentioned as deduction,
        is_asymmetric_mentioned as asymmetric,
        is_bluffing_mentioned as bluffing,
        is_fluidity_mentioned as fluidity,
        `is_straight-forward_mentioned` as straight_forward,
        is_light_mentioned as light,
        is_hilarious_mentioned as hilarious,
        is_entertaining_mentioned as entertaining,
        `is_hidden role_mentioned` as hidden_role,
        `is_hidden identity_mentioned` as hidden_identity,
        is_chaotic_mentioned as chaotic,
        is_guessing_mentioned as guessing,
        is_word_mentioned as word,
        is_memory_mentioned as memory,
        is_compact_mentioned as compact,
        is_cooperative_mentioned as cooperative,
        `is_family game_mentioned` as family_game,
        is_quizz_mentioned as quizz

    from source

)

select * from renamed