select
    movie_id,
    title,
    genre_name,
    profit
from {{ ref('fact_movies_genre') }}
where profit IS NULL