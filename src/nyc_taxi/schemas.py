"""Schemas-alvo da camada bronze.

Os parquets da TLC de 2023 não têm schema estável entre meses:
  - VendorID / passenger_count / RatecodeID alternam entre int32, int64 e double;
  - a coluna de taxa de aeroporto aparece como `airport_fee` ou `Airport_fee`.
Ler todos os arquivos de uma vez (spark.read.parquet(pasta) com mergeSchema)
quebra com conflito de tipos. Por isso cada arquivo é lido isoladamente e
projetado (com cast) para o schema canônico abaixo.
"""
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_COMMON_TAIL = [
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", IntegerType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
]

YELLOW_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        *_COMMON_TAIL,
        StructField("airport_fee", DoubleType()),
    ]
)

GREEN_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType()),
        StructField("lpep_pickup_datetime", TimestampType()),
        StructField("lpep_dropoff_datetime", TimestampType()),
        *_COMMON_TAIL,
        StructField("ehail_fee", DoubleType()),
        StructField("trip_type", IntegerType()),
    ]
)

SCHEMAS = {"yellow": YELLOW_SCHEMA, "green": GREEN_SCHEMA}

# Nome das colunas de embarque/desembarque por tipo de táxi
DATETIME_COLS = {
    "yellow": ("tpep_pickup_datetime", "tpep_dropoff_datetime"),
    "green": ("lpep_pickup_datetime", "lpep_dropoff_datetime"),
}