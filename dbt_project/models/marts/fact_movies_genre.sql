with bridge_movie_genre as (
    select * from {{ source('movies_sources', 'bridge_movie_genre') }}
),
movies as (
    select * from {{ source('movies_sources', 'dim_movies') }}
),
genre as (
    select * from {{ source('movies_sources', 'dim_genre') }}
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
    bg.genre_id,
    g.genre_name
from bridge_movie_genre bg
join movies m on bg.movie_id = m.movie_id
join genre g on bg.genre_id = g.genre_id