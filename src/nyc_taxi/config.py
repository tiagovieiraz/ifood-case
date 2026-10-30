"""Configuração central do pipeline.

Tudo que muda entre ambientes (catálogo, meses, tipos de táxi) fica aqui,
para que os notebooks/jobs sejam só orquestração.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

# Padrão de URL dos links da página oficial da TLC
TLC_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"


@dataclass(frozen=True)
class PipelineConfig:
    # Unity Catalog — no Databricks Free Edition o catálogo padrão se chama "workspace"
    catalog: str = "workspace"
    landing_schema: str = "nyc_taxi_landing"
    bronze_schema: str = "nyc_taxi_bronze"
    silver_schema: str = "nyc_taxi_silver"
    gold_schema: str = "nyc_taxi_gold"
    landing_volume: str = "raw_files"

    # Escopo pedido no case: jan–mai/2023
    start_month: date = date(2023, 1, 1)
    end_month: date = date(2023, 5, 1)

    # yellow: pergunta 1 e colunas obrigatórias; green: "todos os táxis" da pergunta 2.
    # FHV/HVFHV ficam de fora: não são táxis e não têm passenger_count/total_amount.
    taxi_types: tuple[str, ...] = ("yellow", "green")

    @property
    def landing_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.landing_schema}/{self.landing_volume}"

    def landing_path(self, taxi_type: str, ref_month: date) -> str:
        return (
            f"{self.landing_root}/{taxi_type}/{ref_month.year}/{ref_month.month:02d}/"
            f"{taxi_type}_tripdata_{ref_month.year}-{ref_month.month:02d}.parquet"
        )

    def table(self, layer: str, name: str) -> str:
        schema = {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
        }[layer]
        return f"{self.catalog}.{schema}.{name}"

    def months(self) -> list[date]:
        out, cur = [], self.start_month
        while cur <= self.end_month:
            out.append(cur)
            cur = date(cur.year + (cur.month // 12), cur.month % 12 + 1, 1)
        return out


def source_url(taxi_type: str, ref_month: date) -> str:
    return TLC_URL_TEMPLATE.format(taxi_type=taxi_type, year=ref_month.year, month=ref_month.month)


def ref_month_str(ref_month: date) -> str:
    return f"{ref_month.year}-{ref_month.month:02d}"