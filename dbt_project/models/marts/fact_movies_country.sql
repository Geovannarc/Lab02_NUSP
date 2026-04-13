with bridge_movie_production_country as (
    select * from {{ source('movies_sources', 'bridge_movie_production_country') }}
),
movies as (
    select * from {{ source('movies_sources', 'dim_movies') }}
),
country as (
    select * from {{ source('movies_sources', 'dim_production_country') }}
)

select
    bg.movie_id,
    m.title,
    m.release_date,
    m.revenue,
    m.budget,
    m.profit,
    m.vote_average,
    m.vote_count,
    m.popularity,
    bg.production_country_id,
    c.country_name
from bridge_movie_production_country bg
join movies m on bg.movie_id = m.movie_id
join country c on bg.production_country_id = c.production_country_id