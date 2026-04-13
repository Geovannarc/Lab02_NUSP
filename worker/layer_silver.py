from pathlib import Path
from datetime import datetime
import pandas as pd
import os
from log_utils import get_logger
from sqlalchemy import create_engine

class SilverLayerProcessor:
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

        DB_NAME = os.getenv("POSTGRES_DB", "postgres")
        DB_USER = os.getenv("POSTGRES_USER", "postgres")
        DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
        DB_HOST = os.getenv("DB_HOST", "db")
        DB_PORT = os.getenv("DB_PORT", "5432")
        DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

        self.engine = create_engine(DB_URL)

    def run(self):
        self.logger.info("Iniciando processamento da camada silver")

        base_path = Path(os.getenv("RAW_OUTPUT_PATH", "/app"))

        raw_path = base_path / "data" / "raw"

        latest_partition = self._get_latest_partition(raw_path)

        self.logger.info(f"Usando partição raw: {latest_partition}")

        df = self._read_raw(latest_partition)

        df = self._clean(df)
        df = self._transform(df)

        output_path = self.persist_silver(df)

        self.logger.info(f"Silver finalizado. Tabela: {output_path}")
        return str(output_path)

    def _read_raw(self, partition_path: Path) -> pd.DataFrame:
        files = list(partition_path.glob("*.csv"))
        self.logger.info(f"Arquivos CSV encontrados: {files}")

        if not files:
            raise FileNotFoundError(f"Nenhum CSV encontrado em {partition_path}")

        df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
        self.logger.info(f"DataFrame lido com shape: {df.shape}")
        return df

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Executando limpeza de dados")

        df = df.drop_duplicates(subset=["id"])

        df = df.replace({"": None})

        return df

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Executando transformações")

        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")

        numeric_cols = [
            "vote_average",
            "vote_count",
            "revenue",
            "runtime",
            "budget",
            "popularity",
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["adult"] = df["adult"].astype("boolean")

        return df

    def persist_silver(self, df: pd.DataFrame) -> str:
        self.logger.info(f"Iniciando persist_silver com DF shape: {df.shape}")
        try:
            table_name = "staging_movies"
            df.to_sql(table_name, self.engine, if_exists="replace", index=False, method="multi", chunksize=1000)
            self.logger.info(f"Persistido staging table: {table_name}")
            return table_name
        except Exception as e:
            self.logger.error(f"Erro ao inserir dados na tabela: {e}")
            raise

    def _get_latest_partition(self, raw_base: Path) -> Path:
        partitions = sorted(raw_base.glob("*/*/*"))

        if not partitions:
            raise FileNotFoundError("Nenhuma partição raw encontrada")

        return partitions[-1]