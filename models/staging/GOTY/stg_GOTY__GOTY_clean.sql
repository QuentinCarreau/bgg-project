with 

source as (

    select * from {{ source('GOTY', 'GOTY_clean') }}

),

renamed as (

    select
        name,
        author,
        illustrator,
        publisher,
        award,
        year,
        ceremony

    from source

)

select * from renamed