"""Bronze: dado original em Delta, com schema harmonizado e metadados de linhagem.

Mantém TODAS as colunas e TODAS as linhas (inclusive as "sujas"). A limpeza é
responsabilidade da silver — se uma regra de qualidade mudar, reprocessamos a
partir daqui sem voltar à landing.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import ref_month_str
from .schemas import SCHEMAS


def conform_to_schema(df: DataFrame, taxi_type: str) -> DataFrame:
    """Projeta o DataFrame lido para o schema canônico do tipo de táxi.

    - Resolve nomes sem diferenciar maiúsculas (ex.: `Airport_fee` -> `airport_fee`);
    - Faz cast explícito de cada coluna;
    - Coluna ausente no arquivo vira NULL tipado (em vez de quebrar o job).
    """
    target = SCHEMAS[taxi_type]
    by_lower = {c.lower(): c for c in df.columns}
    cols = []
    for field in target.fields:
        src = by_lower.get(field.name.lower())
        expr = F.col(f"`{src}`") if src else F.lit(None)
        cols.append(expr.cast(field.dataType).alias(field.name))
    return df.select(*cols)


def add_lineage(df: DataFrame, ref_month: date, source_file: str) -> DataFrame:
    return (
        df.withColumn("ref_month", F.lit(ref_month_str(ref_month)))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingestion_ts", F.current_timestamp())
    )


def build_bronze(spark: SparkSession, path: str, taxi_type: str, ref_month: date) -> DataFrame:
    raw = spark.read.parquet(path)
    return add_lineage(conform_to_schema(raw, taxi_type), ref_month, path)


def write_partition(df: DataFrame, table: str, ref_month: date) -> None:
    """Escrita idempotente: sobrescreve apenas a partição do mês processado.

    Rodar o job duas vezes para o mesmo mês não duplica dados.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ref_month = '{ref_month_str(ref_month)}'")
        .saveAsTable(table)
    )