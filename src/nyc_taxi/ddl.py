"""Modelagem das tabelas no Unity Catalog (a "tecnologia de metadados" escolhida).

Tabelas e colunas são criadas explicitamente, com COMMENT, para que o
catálogo sirva de documentação para quem consome os dados.
"""
from __future__ import annotations

from .config import PipelineConfig


def _common_bronze_cols() -> str:
    return """
    passenger_count INT, trip_distance DOUBLE, RatecodeID INT, store_and_fwd_flag STRING,
    PULocationID INT, DOLocationID INT, payment_type INT, fare_amount DOUBLE, extra DOUBLE,
    mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE,
    total_amount DOUBLE, congestion_surcharge DOUBLE"""


def setup_statements(cfg: PipelineConfig) -> list[str]:
    c = cfg.catalog
    stmts = [
        f"CREATE SCHEMA IF NOT EXISTS {c}.{cfg.landing_schema} COMMENT 'Landing zone: arquivos originais da TLC, sem transformação'",
        f"CREATE SCHEMA IF NOT EXISTS {c}.{cfg.bronze_schema} COMMENT 'Bronze: dado bruto em Delta, schema harmonizado, todas as linhas'",
        f"CREATE SCHEMA IF NOT EXISTS {c}.{cfg.silver_schema} COMMENT 'Silver/consumo: dado limpo e validado, pronto para SQL'",
        f"CREATE SCHEMA IF NOT EXISTS {c}.{cfg.gold_schema} COMMENT 'Gold: visões de negócio e agregados prontos para análise'",
        f"CREATE VOLUME IF NOT EXISTS {c}.{cfg.landing_schema}.{cfg.landing_volume} COMMENT 'Parquets originais baixados de nyc.gov/tlc'",
    ]

    lineage = "ref_month STRING, source_file STRING, ingestion_ts TIMESTAMP"
    stmts += [
        f"""CREATE TABLE IF NOT EXISTS {cfg.table('bronze', 'yellow_tripdata')} (
            VendorID INT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP,
            {_common_bronze_cols()}, airport_fee DOUBLE, {lineage})
        USING DELTA PARTITIONED BY (ref_month)
        COMMENT 'Corridas yellow taxi como vieram da TLC (inclui registros inválidos)'""",
        f"""CREATE TABLE IF NOT EXISTS {cfg.table('bronze', 'green_tripdata')} (
            VendorID INT, lpep_pickup_datetime TIMESTAMP, lpep_dropoff_datetime TIMESTAMP,
            {_common_bronze_cols()}, ehail_fee DOUBLE, trip_type INT, {lineage})
        USING DELTA PARTITIONED BY (ref_month)
        COMMENT 'Corridas green taxi como vieram da TLC (inclui registros inválidos)'""",
    ]

    for taxi, prefix in (("yellow", "tpep"), ("green", "lpep")):
        stmts.append(f"""CREATE TABLE IF NOT EXISTS {cfg.table('silver', f'{taxi}_tripdata')} (
            VendorID INT COMMENT 'Fornecedor do registro: 1=Creative Mobile Technologies, 2=Curb/VeriFone',
            passenger_count INT COMMENT 'Passageiros informados pelo motorista (pode ser nulo/0)',
            total_amount DOUBLE COMMENT 'Valor total cobrado do passageiro (USD); negativo = estorno/disputa',
            {prefix}_pickup_datetime TIMESTAMP_NTZ COMMENT 'Embarque — horário local de Nova York',
            {prefix}_dropoff_datetime TIMESTAMP_NTZ COMMENT 'Desembarque — horário local de Nova York',
            trip_distance DOUBLE COMMENT 'Distância em milhas',
            PULocationID INT COMMENT 'Zona TLC de embarque',
            DOLocationID INT COMMENT 'Zona TLC de desembarque',
            payment_type INT COMMENT '1=cartão, 2=dinheiro, 3=sem cobrança, 4=disputa, 5=desconhecido, 6=anulada',
            is_valid_passenger_count BOOLEAN COMMENT 'false quando passenger_count é nulo ou 0',
            ref_month STRING COMMENT 'Mês de referência do arquivo de origem (yyyy-MM)',
            trip_duration_min DOUBLE COMMENT 'Duração da corrida em minutos',
            is_negative_amount BOOLEAN COMMENT 'true para estornos/disputas/ajustes (total_amount < 0)')
        USING DELTA PARTITIONED BY (ref_month)
        COMMENT 'Camada de consumo: corridas {taxi} validadas (regras em src/nyc_taxi/silver.py)'
        TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')""")

    for taxi in ("yellow", "green"):
        stmts.append(f"""CREATE TABLE IF NOT EXISTS {cfg.table('silver', f'{taxi}_tripdata_quarantine')}
        USING DELTA PARTITIONED BY (ref_month)
        COMMENT 'Linhas {taxi} rejeitadas pelas regras de qualidade, com o motivo em reject_reason'
        AS SELECT *, CAST(NULL AS STRING) AS reject_reason, CAST(NULL AS BOOLEAN) AS is_valid_passenger_count,
                  CAST(NULL AS TIMESTAMP) AS quarantine_ts
        FROM {cfg.table('bronze', f'{taxi}_tripdata')} WHERE 1 = 0""")
    return stmts


def gold_statements(cfg: PipelineConfig) -> list[str]:
    y, g = cfg.table("silver", "yellow_tripdata"), cfg.table("silver", "green_tripdata")
    fleet = cfg.table("gold", "vw_fleet_trips")
    return [
        f"""CREATE OR REPLACE VIEW {fleet}
        COMMENT 'Toda a frota de táxis (yellow + green) com nomes de colunas padronizados'
        AS
        SELECT 'yellow' AS taxi_type, VendorID AS vendor_id, tpep_pickup_datetime AS pickup_datetime,
               tpep_dropoff_datetime AS dropoff_datetime, passenger_count, is_valid_passenger_count,
               total_amount, is_negative_amount, trip_distance, trip_duration_min,
               PULocationID, DOLocationID, payment_type, ref_month
        FROM {y}
        UNION ALL
        SELECT 'green', VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, passenger_count,
               is_valid_passenger_count, total_amount, is_negative_amount, trip_distance, trip_duration_min,
               PULocationID, DOLocationID, payment_type, ref_month
        FROM {g}""",
        f"""CREATE OR REPLACE VIEW {cfg.table('gold', 'vw_yellow_monthly_revenue')}
        COMMENT 'Resumo financeiro mensal dos yellow taxis (estornos incluídos; ver negative_rows)'
        AS
        SELECT ref_month,
               COUNT(*)                                                AS trips,
               SUM(CASE WHEN is_negative_amount THEN 1 ELSE 0 END)     AS negative_rows,
               ROUND(AVG(total_amount), 2)                             AS avg_total_amount_per_trip,
               ROUND(PERCENTILE_APPROX(total_amount, 0.5), 2)          AS median_total_amount_per_trip,
               ROUND(SUM(total_amount), 2)                             AS total_amount_sum
        FROM {y}
        GROUP BY ref_month""",
    ]