with fact_movies_genre as (
    select * from {{ source('movies_sources', 'fact_movies_genre') }}
),
movies as (
    select * from {{ source('movies_sources', 'dim_movies') }}
),
genre as (
    select * from {{ source('movies_sources', 'dim_genre') }}
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
    fm.genre_id,
    g.genre_name
from fact_movies_genre fm
join movies m on fm.movie_id = m.movie_id
join genre g on fm.genre_id = g.genre_id