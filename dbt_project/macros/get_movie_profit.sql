{% macro get_movie_profit(movie_alias='m') -%}
    COALESCE({{ movie_alias }}.revenue - {{ movie_alias }}.budget, 0) as profit
{%- endmacro %}