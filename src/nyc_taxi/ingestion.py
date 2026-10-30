"""Landing zone: cópia fiel dos arquivos originais da TLC para um Volume do Unity Catalog.

Nenhuma transformação acontece aqui — o arquivo é guardado byte a byte, o que
permite reprocessar bronze/silver a qualquer momento sem depender do site da TLC.
"""
from __future__ import annotations

import os
import shutil
import tempfile
import urllib.request
from dataclasses import dataclass
from datetime import date

from .config import PipelineConfig, source_url


@dataclass
class LandingResult:
    taxi_type: str
    ref_month: date
    path: str
    status: str  # downloaded | skipped_exists | failed
    size_bytes: int = 0
    error: str | None = None


def download_file(url: str, dest_path: str, timeout: int = 300) -> int:
    """Baixa para um arquivo temporário e só depois copia para o destino.

    Assim um download interrompido nunca deixa um parquet corrompido na landing.
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "ifood-case-ingestion/1.0"})
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            shutil.copyfileobj(resp, tmp, length=8 * 1024 * 1024)
        tmp_path = tmp.name
    shutil.copy(tmp_path, dest_path)  # Volumes aceitam copy, não rename entre filesystems
    os.remove(tmp_path)
    return os.path.getsize(dest_path)


def land_month(cfg: PipelineConfig, taxi_type: str, ref_month: date, overwrite: bool = False) -> LandingResult:
    dest = cfg.landing_path(taxi_type, ref_month)
    if os.path.exists(dest) and not overwrite:
        return LandingResult(taxi_type, ref_month, dest, "skipped_exists", os.path.getsize(dest))
    try:
        size = download_file(source_url(taxi_type, ref_month), dest)
        return LandingResult(taxi_type, ref_month, dest, "downloaded", size)
    except Exception as exc:  # noqa: BLE001 — registra qualquer falha e segue para o próximo arquivo
        return LandingResult(taxi_type, ref_month, dest, "failed", error=f"{type(exc).__name__}: {exc}")


def land_all(cfg: PipelineConfig, overwrite: bool = False) -> list[LandingResult]:
    return [
        land_month(cfg, taxi_type, m, overwrite)
        for taxi_type in cfg.taxi_types
        for m in cfg.months()
    ]