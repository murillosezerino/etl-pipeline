"""
main.py — Orquestrador do pipeline ETL com Cloudflare R2.
Fluxo: gera mock -> upload raw -> transforma -> upload processed (Parquet particionado)
"""

import logging
import pandas as pd

from config.settings import (
    R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
    R2_RAW_BUCKET, R2_PROCESSED_BUCKET, LOG_LEVEL,
)
from etl.extract import R2Extractor
from etl.transform import DeliveryTransformer
from etl.load import R2Loader
from etl.mock_data import generate_deliveries

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(module)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run() -> None:
    logger.info("=" * 55)
    logger.info("Iniciando pipeline ETL — Cloudflare R2")
    logger.info("=" * 55)

    # ── 0. GERAR MOCK ────────────────────────────────────
    logger.info("[0/3] Gerando dados mock...")
    df_mock = generate_deliveries(12000)

    # ── 1. UPLOAD RAW ────────────────────────────────────
    logger.info("[1/3] Fazendo upload dos dados raw para R2...")
    loader_raw = R2Loader(
        bucket=R2_RAW_BUCKET,
        endpoint=R2_ENDPOINT,
        access_key=R2_ACCESS_KEY_ID,
        secret_key=R2_SECRET_ACCESS_KEY,
    )
    loader_raw.upload_csv(df_mock, "raw/deliveries/latest.csv")
    logger.info(f"  Upload raw: {len(df_mock):,} registros -> {R2_RAW_BUCKET}/raw/deliveries/latest.csv")

    # ── 2. EXTRACT ───────────────────────────────────────
    logger.info("[2/3] Extraindo dados do R2...")
    extractor = R2Extractor(
        bucket=R2_RAW_BUCKET,
        endpoint=R2_ENDPOINT,
        access_key=R2_ACCESS_KEY_ID,
        secret_key=R2_SECRET_ACCESS_KEY,
    )
    df_raw = extractor.read_csv("raw/deliveries/latest.csv")
    logger.info(f"  Extraido: {len(df_raw):,} registros")

    # ── 3. TRANSFORM ─────────────────────────────────────
    logger.info("[3/3] Transformando dados...")
    df_clean = DeliveryTransformer.run_all(df_raw)

    # ── 4. LOAD PROCESSED ────────────────────────────────
    logger.info("[4/3] Carregando dados processados no R2...")
    loader_processed = R2Loader(
        bucket=R2_PROCESSED_BUCKET,
        endpoint=R2_ENDPOINT,
        access_key=R2_ACCESS_KEY_ID,
        secret_key=R2_SECRET_ACCESS_KEY,
    )

    partitions = df_clean.groupby(["year", "month", "day"])
    for (year, month, day), group in partitions:
        key = R2Loader.partitioned_key(
            "deliveries", int(year), int(month), int(day), "data.parquet"
        )
        loader_processed.upload_parquet(
            group.drop(columns=["year", "month", "day"]), key
        )

    logger.info("=" * 55)
    logger.info(f"Pipeline concluido: {len(df_clean):,} registros processados")
    logger.info(f"Particoes geradas:  {partitions.ngroups}")
    logger.info("=" * 55)


if __name__ == "__main__":
    run()