"""Silver (camada de consumo): dado limpo, tipado e enxuto.

Regras de qualidade
-------------------
Uma linha é REJEITADA (vai para a quarentena com o motivo) quando:
  R1 pickup_outside_reference_month  embarque nulo ou fora do mês do arquivo
  R2 dropoff_before_pickup           desembarque nulo ou anterior ao embarque
  R3 null_total_amount               total_amount nulo

total_amount NEGATIVO não rejeita: são estornos/disputas (payment_type 2, 3 e 4) que
anulam uma cobrança. Ficam na silver com `is_negative_amount = true`, para que a soma
mensal reflita a receita líquida e cada análise decida se filtra.

passenger_count nulo/zero também NÃO rejeita a corrida — o valor cobrado continua
válido para análises financeiras. A coluna `is_valid_passenger_count`
sinaliza o problema e cada análise decide o filtro.

Datas
-----
Os timestamps da TLC são horário local de Nova York sem fuso. Na silver eles
viram TIMESTAMP_NTZ: `hour(tpep_pickup_datetime)` devolve a hora local correta
independentemente do fuso configurado na sessão de quem consulta.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .schemas import DATETIME_COLS

# Colunas obrigatórias pelo case
CONSUMPTION_COLUMNS = {
    "yellow": ["VendorID", "passenger_count", "total_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime"],
    "green": ["VendorID", "passenger_count", "total_amount", "lpep_pickup_datetime", "lpep_dropoff_datetime"],
}

# Colunas extras úteis para análise
EXTRA_COLUMNS = ["trip_distance", "PULocationID", "DOLocationID", "payment_type"]


def _month_bounds(ref_month: date) -> tuple[str, str]:
    nxt = date(ref_month.year + (ref_month.month // 12), ref_month.month % 12 + 1, 1)
    return ref_month.isoformat(), nxt.isoformat()


def add_quality_columns(df: DataFrame, taxi_type: str, ref_month: date) -> DataFrame:
    pickup, dropoff = DATETIME_COLS[taxi_type]
    start, end = _month_bounds(ref_month)
    p, d = F.col(pickup), F.col(dropoff)

    reason = (
        F.when(p.isNull() | (p < F.lit(start).cast("timestamp")) | (p >= F.lit(end).cast("timestamp")),
               F.lit("pickup_outside_reference_month"))
        .when(d.isNull() | (d < p), F.lit("dropoff_before_pickup"))
        .when(F.col("total_amount").isNull(), F.lit("null_total_amount"))
    )
    return df.withColumn("reject_reason", reason).withColumn(
        "is_valid_passenger_count",
        F.col("passenger_count").isNotNull() & (F.col("passenger_count") > 0),
    )


def to_consumption(df: DataFrame, taxi_type: str) -> DataFrame:
    pickup, dropoff = DATETIME_COLS[taxi_type]
    cols = CONSUMPTION_COLUMNS[taxi_type] + EXTRA_COLUMNS
    return (
        df.select(*cols, "is_valid_passenger_count", "ref_month")
        .withColumn(pickup, F.col(pickup).cast("timestamp_ntz"))
        .withColumn(dropoff, F.col(dropoff).cast("timestamp_ntz"))
        .withColumn("trip_duration_min",
                    F.round((F.unix_timestamp(F.col(dropoff).cast("timestamp"))
                             - F.unix_timestamp(F.col(pickup).cast("timestamp"))) / 60, 2))
        .withColumn("is_negative_amount", F.col("total_amount") < 0)
    )


def split_valid_rejected(bronze_df: DataFrame, taxi_type: str, ref_month: date) -> tuple[DataFrame, DataFrame]:
    flagged = add_quality_columns(bronze_df, taxi_type, ref_month)
    valid = to_consumption(flagged.filter(F.col("reject_reason").isNull()), taxi_type)
    rejected = flagged.filter(F.col("reject_reason").isNotNull()).withColumn("quarantine_ts", F.current_timestamp())
    return valid, rejected