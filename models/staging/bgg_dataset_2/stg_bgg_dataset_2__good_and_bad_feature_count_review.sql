with 

source as (

    select * from {{ source('bgg_dataset_2', 'good_and_bad_feature_count_review') }}

),

renamed as (

    select
        `unnamed: 0` as user_id,
        user as user_name,
        rating as avg_rating,
        comment,
        id,
        `name` as game_name,
        is_fun_mentioned,
        `is_simple rules_mentioned` as is_simple_rules_mentioned,
        is_accessibility_mentioned,
        is_artistic_mentioned,
        is_esthetic_mentioned,
        `is_complex rules_mentioned` as is_complex_rules_mentioned,
        is_unfair_mentioned,
        `is_old school_mentioned` as is_old_school_mentioned,
        is_complexity_mentioned,
        is_boring_mentioned

    from source

)

select * from renamed