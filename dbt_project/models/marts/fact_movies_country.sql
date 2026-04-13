with fact_movies_country as (
    select * from {{ source('movies_sources', 'fact_movies_country') }}
),
movies as (
    select * from {{ source('movies_sources', 'dim_movies') }}
),
country as (
    select * from {{ source('movies_sources', 'dim_production_country') }}
)

select
    fm.movie_id,
    m.title,
    m.release_date,
    m.revenue,
    m.budget,
    m.profit,
    m.vote_average,
    m.vote_count,
    m.popularity,
    fm.country_id,
    c.country_name
from fact_movies_country fm
join movies m on fm.movie_id = m.movie_id
join country c on fm.country_id = c.country_id