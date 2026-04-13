import pandas as pd
from sqlalchemy import create_engine, text
import os


class GoldLayerProcessor:
    def __init__(self):
        DB_NAME = os.getenv("POSTGRES_DB", "postgres")
        DB_USER = os.getenv("POSTGRES_USER", "postgres")
        DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
        DB_HOST = os.getenv("DB_HOST", "db")
        DB_PORT = os.getenv("DB_PORT", "5432")
        DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

        self.engine = create_engine(DB_URL)

    def run(self):
        df = pd.read_sql("SELECT * FROM staging_movies", self.engine)

        df = self._prepare(df)

        self._merge_dimensions(df)

    def _prepare(self, df):

        return df[[
            "id", "title", "release_date",
            "revenue", "budget", "original_language",
            "status", "genres", "production_companies", "production_countries",
            "vote_average", "vote_count", "popularity"
        ]].rename(columns={"id": "movie_id"})

    def _merge_dimensions(self, df):

        with self.engine.begin() as conn:

            languages = df["original_language"].dropna().unique().tolist()
            if languages:
                conn.execute(
                    text("""
                        INSERT INTO dim_language (language_code)
                        SELECT UNNEST(:languages)
                        ON CONFLICT (language_code) DO NOTHING
                    """),
                    {"languages": languages}
                )

            genres = df["genres"].dropna().unique().tolist()
            if genres:
                conn.execute(
                    text("""
                        INSERT INTO dim_genre (genre_name)
                        SELECT UNNEST(:genres)
                        ON CONFLICT (id) DO NOTHING
                    """),
                    {"genres": genres}
                )

            companies = df["production_companies"].dropna().unique().tolist()
            if companies:
                conn.execute(
                    text("""
                        INSERT INTO dim_production_company (company_name)
                        SELECT UNNEST(:companies)
                        ON CONFLICT (id) DO NOTHING
                    """),
                    {"companies": companies}
                )

            countries = df["production_countries"].dropna().unique().tolist()
            if countries:
                conn.execute(
                    text("""
                        INSERT INTO dim_production_country (country_name)
                        SELECT UNNEST(:countries)
                        ON CONFLICT (id) DO NOTHING
                    """),
                    {"countries": countries}
                )

            self._populate_bridge(df, "bridge_movie_genre", "dim_genre", "genres", "id", "genre_name")
            self._populate_bridge(df, "bridge_movie_company", "dim_production_company", "production_companies", "id", "company_name")
            self._populate_bridge(df, "bridge_movie_country", "dim_production_country", "production_countries", "id", "country_name")
    
    def _populate_bridge(self, df: pd.DataFrame, bridge_table: str, dim_table: str, 
                         source_col: str, dim_id_col: str, dim_name_col: str):
            """Popula tabelas bridge para relações N:N."""
            df_exploded = df[['id', source_col]].copy()
            df_exploded[source_col] = df_exploded[source_col].str.split(',')
            df_exploded = df_exploded.explode(source_col)

            dim_df = pd.read_sql(f"SELECT {dim_id_col}, {dim_name_col} FROM {dim_table}", self.engine)
            bridge_df = df_exploded.merge(dim_df, left_on=source_col, right_on=dim_name_col)

            bridge_df = bridge_df[['id', dim_id_col]].rename(columns={'id': 'movie_id'})
            bridge_df.to_sql(bridge_table, self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
            print(f"Tabela Bridge {bridge_table} carregada.")
            